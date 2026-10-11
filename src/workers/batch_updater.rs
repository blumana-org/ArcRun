// Diesel QueryableByName emits separate impls with explicit field initializers;
// an allow on the struct does not cover those generated impls.
#![allow(clippy::redundant_field_names)]

use crate::{Conn, DbPool, db_operation, db_operation::DbError, metrics};
use actix_web::rt;
use dashmap::DashMap;
use diesel_async::RunQueryDsl;
use std::sync::{Arc, atomic::AtomicI64, atomic::Ordering};
use tokio::sync::{mpsc, watch};

#[derive(Debug)]
pub struct UpdateEvent {
    pub success: i32,
    pub failures: i32,
    pub task_id: uuid::Uuid,
}

#[derive(Debug, Default)]
struct Entry {
    success: AtomicI64,
    failures: AtomicI64,
}

type CountMap = DashMap<uuid::Uuid, Entry>;

/// A single pending counter delta for one task.
type Update = (uuid::Uuid, i64, i64);

/// Accumulate one incoming event into the shared map. A single `entry()` call
/// holds the shard lock for both counter updates.
fn apply_event(data: &CountMap, evt: &UpdateEvent) {
    let entry = data.entry(evt.task_id).or_default();
    entry
        .success
        .fetch_add(i64::from(evt.success), Ordering::Relaxed);
    entry
        .failures
        .fetch_add(i64::from(evt.failures), Ordering::Relaxed);
}

/// Re-add a set of counts to the map for a later retry (transient-failure path).
fn requeue(data: &CountMap, updates: impl IntoIterator<Item = Update>) {
    for (task_id, success_count, failure_count) in updates {
        let entry = data.entry(task_id).or_default();
        entry.success.fetch_add(success_count, Ordering::Relaxed);
        entry.failures.fetch_add(failure_count, Ordering::Relaxed);
    }
}

/// Atomically swap out the accumulated counters, returning the non-zero deltas.
fn drain_updates(data: &CountMap) -> Vec<Update> {
    data.iter()
        .map(|entry| {
            let task_id = *entry.key();
            let s = entry.value().success.swap(0, Ordering::Relaxed);
            let f = entry.value().failures.swap(0, Ordering::Relaxed);
            (task_id, s, f)
        })
        .filter(|(_, s, f)| *s != 0 || *f != 0)
        .collect()
}

/// Receiver-side drain loop. Continuously moves channel events into the shared
/// map so the updater loop never has to touch the channel. On shutdown it drains
/// every already-buffered event (`try_recv`) before exiting, so no in-flight
/// event is lost between the channel and the final flush snapshot.
async fn receiver_drain_loop(
    data: Arc<CountMap>,
    mut receiver: mpsc::Receiver<UpdateEvent>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            maybe = receiver.recv() => match maybe {
                Some(evt) => apply_event(&data, &evt),
                // All senders dropped: nothing more will ever arrive.
                None => break,
            },
            _ = shutdown.changed() => {
                // Shutdown requested (or the watch sender was dropped). Drain
                // everything currently buffered in the channel, then exit so the
                // updater's final flush sees a complete snapshot.
                while let Ok(evt) = receiver.try_recv() {
                    apply_event(&data, &evt);
                }
                break;
            }
        }
    }
}

/// Receives success/failure update events and batches them to the database.
/// Uses DashMap for lock-free concurrent access between receiver and updater.
pub async fn batch_updater(
    pool: DbPool,
    receiver: mpsc::Receiver<UpdateEvent>,
    flush_interval: std::time::Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    let data: Arc<CountMap> = Arc::new(DashMap::new());

    // Receiver task - continuously drains channel without blocking updater.
    // It shares the shutdown watch so it can flush the channel before exit.
    let recv_handle = tokio::spawn(receiver_drain_loop(
        Arc::clone(&data),
        receiver,
        shutdown.clone(),
    ));

    // Updater loop - persists batched counts to database
    loop {
        let loop_start = std::time::Instant::now();
        if let Ok(mut conn) = pool.get().await {
            // Collect updates - DashMap iter() doesn't block other operations on different shards
            let updates = drain_updates(&data);

            // Process updates in a single batched SQL query
            if !updates.is_empty() {
                let row_count = updates.len();
                log::debug!("Batch update: {} tasks to persist", row_count);

                let flush_start = std::time::Instant::now();
                flush_updates(updates, &mut conn, &data).await;
                metrics::record_batch_updater_flush(row_count, flush_start.elapsed().as_secs_f64());
            }

            // Cleanup zero entries periodically
            data.retain(|_, entry| {
                AtomicI64::load(&entry.success, Ordering::Relaxed) != 0
                    || AtomicI64::load(&entry.failures, Ordering::Relaxed) != 0
            });
            metrics::set_batch_updater_pending_tasks(data.len());
        }
        metrics::record_worker_loop_iteration("batch_updater", loop_start.elapsed().as_secs_f64());
        tokio::select! {
            _ = shutdown.changed() => {
                log::info!("Batch updater: shutdown signal received, draining channel then flushing");
                // Let the receiver drain any still-buffered events into the map and
                // exit BEFORE we snapshot for the final flush. This closes the race
                // where in-flight events were lost at shutdown (audit A7, C6).
                let _ = recv_handle.await;
                final_flush_batch_data(&data, &pool).await;
                log::info!("Batch updater: final flush complete, exiting");
                return;
            }
            _ = rt::time::sleep(flush_interval) => {}
        }
    }
}

/// Persist one drained batch, with anti-poison recovery.
///
/// The happy path is a single UNNEST statement. Rows whose task is terminal
/// (or gone) simply don't match and their counts are **dropped on the floor** —
/// a terminal task's counters are frozen and were
/// already delivered with the end notification, so re-queuing them would diverge
/// forever (audit A7). A drop is NOT an error: on `Ok` we never re-queue.
///
/// On a DB error we fall back to per-row so a single deterministically-faulty
/// ("poison") row cannot wedge the whole pipeline.
async fn flush_updates(updates: Vec<Update>, conn: &mut Conn<'_>, data: &CountMap) {
    if let Err(e) = handle_batch_with_counts(&updates, conn).await {
        log::warn!(
            "Batched update failed for {} tasks ({:?}), recovering per-row",
            updates.len(),
            e
        );
        recover_per_row(updates, conn, data).await;
    }
}

/// Per-row recovery after a failed batch flush (anti-poison).
///
/// Each row is applied on its own. A row that no longer matches because the task is
/// terminal/missing is intentionally consumed. Every database error is re-queued,
/// independently of sibling outcomes: one successful statement does not prove that
/// another error is deterministic, and dropping it would lose acknowledged progress.
async fn recover_per_row(updates: Vec<Update>, conn: &mut Conn<'_>, data: &CountMap) {
    let mut failed: Vec<(Update, DbError)> = Vec::new();

    for update in updates {
        match handle_one_with_counts(update.0, conn, update.1, update.2).await {
            Ok(_) => {}
            Err(e) => failed.push((update, e)),
        }
    }

    if failed.is_empty() {
        return;
    }

    for ((task_id, success, failures), error) in &failed {
        log::error!(
            "Per-row recovery failed for task={} (+{} success, +{} failures): {:?}; re-queuing",
            task_id,
            success,
            failures,
            error
        );
        metrics::record_batch_update_failure();
    }
    requeue(data, failed.into_iter().map(|(update, _)| update));
}

/// Flush all remaining batch data to the database before shutdown.
async fn final_flush_batch_data(data: &CountMap, pool: &DbPool) {
    let updates = drain_updates(data);

    if updates.is_empty() {
        return;
    }

    log::info!(
        "Batch updater final flush: {} entries to persist",
        updates.len()
    );

    if let Ok(mut conn) = pool.get().await {
        // Same anti-poison / terminal-drop semantics as the steady-state flush,
        // but any transient re-queue here is best-effort (we are exiting).
        flush_updates(updates, &mut conn, data).await;
    } else {
        log::error!("Final flush: could not acquire DB connection, data lost");
    }
}

/// Apply counter updates for multiple tasks in a single SQL statement using UNNEST.
/// This reduces N round-trips to 1 for the common case.
///
/// **Deadlock avoidance (Audit 2, A9)**: the UNNEST `UPDATE` locks rows in the query
/// planner's order (a hash join makes it effectively arbitrary), so it could cross a
/// concurrent `propagate_to_children` (which touches the same task rows) and deadlock
/// (`40P01`). To give this flush a canonical acquisition order, the ids are **sorted**
/// and the whole flush runs in ONE transaction that first pre-locks the target rows
/// with `SELECT id … WHERE id = ANY(...) ORDER BY id FOR UPDATE` (matching the order
/// terminal transitions use for their complete downward set), then runs the guarded
/// UPDATE. The pre-lock does NOT filter on status — locking a terminal row is
/// harmless and keeps the order stable; the A7 terminal guard stays in the UPDATE.
/// A database error surfaces
/// as an `Err` here and the caller (`flush_updates`) falls back to the per-row path.
/// That path still acquires capacity slots, but always AFTER its task row,
/// matching release.
///
/// Two audit-A7 guards live in the SQL:
/// * `AND task.status NOT IN ('success','failure','canceled')` — never mutate a
///   **terminal** task (its counters are frozen and were already delivered with
///   its end notification). Terminal rows silently don't match; their counts are
///   dropped by the caller (not re-queued). The audit phrased this as
///   `IN ('running','claimed')`; we gate on the terminal set instead so a genuine
///   progress update on a still-active but non-`running` task (e.g. `paused` after
///   a worker's last flush, or a `pending`/`waiting` task in the test harness) is
///   not silently discarded — same "terminal immutability" intent, no false drop.
/// * `LEAST(task.<c>::bigint + batch.<d>, 2147483647)` — the sum is computed in
///   `bigint` so `int + int` can never overflow, then clamped back to `int4`'s
///   max. Without the cast a large accumulated delta would raise `integer out of
///   range` and poison the flush forever.
async fn handle_batch_with_counts(
    updates: &[Update],
    conn: &mut Conn<'_>,
) -> Result<usize, DbError> {
    // A9: sort by task_id so the pre-lock (and the UPDATE it guards) acquire row
    // locks in the same canonical order terminal transitions use.
    let mut ordered: Vec<Update> = updates.to_vec();
    ordered.sort_by_key(|(id, _, _)| *id);

    let ids: Vec<uuid::Uuid> = ordered.iter().map(|(id, _, _)| *id).collect();
    let successes: Vec<i64> = ordered.iter().map(|(_, s, _)| *s).collect();
    let fail_counts: Vec<i64> = ordered.iter().map(|(_, _, f)| *f).collect();

    // A9: pre-lock the target rows in id order, then apply the UNNEST update, in one
    // transaction so the locks are held together. A `40P01` on either statement rolls
    // the whole thing back and propagates as `Err`, letting `flush_updates` recover
    // per-row without the multi-task acquisition cycle.
    //
    // Capacity slot maintenance (Audit 2, D1 / 7.3b): as a Running task reports progress
    // its remaining work (`capacity_charge`) shrinks, freeing capacity for new claims.
    // We push that shrink onto the `cap:` `rule_slot` counters in the SAME transaction.
    // Task rows MUST be locked before their slots, matching admission and terminal
    // transitions. Reading stored keys after that lock also prevents a concurrent
    // release/reclaim from changing the capacity keys behind this flush.
    // The overwhelmingly common flush touches no capacity task,
    // so this is a single cheap SELECT that returns nothing and the later capacity
    // statement is skipped entirely.
    db_operation::run_in_transaction(conn, move |conn| {
        Box::pin(async move {
            // 1. Ordered task pre-lock; the counter UPDATE retains its terminal guard.
            diesel::sql_query(
                "SELECT id FROM task WHERE id = ANY($1::uuid[]) ORDER BY id FOR UPDATE",
            )
            .bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(&ids)
            .execute(&mut *conn)
            .await?;

            // 2. Pre-lock the cap: slots of any task in this flush that still
            //    carries a positive charge, in sorted key order. Empty ⇒ no capacity work.
            #[derive(diesel::QueryableByName)]
            struct LockKeyRow {
                #[diesel(sql_type = diesel::sql_types::Text)]
                #[allow(dead_code)]
                lock_key: String,
            }
            let cap_slots: Vec<LockKeyRow> = diesel::sql_query(
                "SELECT lock_key FROM rule_slot \
                 WHERE lock_key IN ( \
                     SELECT DISTINCT k FROM task, unnest(claimed_slot_keys) AS k \
                     WHERE id = ANY($1) AND capacity_charge > 0 AND k LIKE 'cap:%' \
                 ) \
                 ORDER BY lock_key COLLATE \"C\" FOR UPDATE",
            )
            .bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(&ids)
            .get_results(&mut *conn)
            .await?;

            // 3. Counter UPDATE.
            let updated = diesel::sql_query(
                "UPDATE task SET \
                    success = LEAST(task.success::bigint + batch.s, 2147483647), \
                    failures = LEAST(task.failures::bigint + batch.f, 2147483647), \
                    last_updated = NOW() \
                FROM UNNEST($1::uuid[], $2::bigint[], $3::bigint[]) AS batch(id, s, f) \
                WHERE task.id = batch.id \
                  AND task.status NOT IN ('success', 'failure', 'canceled')",
            )
            .bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(&ids)
            .bind::<diesel::sql_types::Array<diesel::sql_types::BigInt>, _>(&successes)
            .bind::<diesel::sql_types::Array<diesel::sql_types::BigInt>, _>(&fail_counts)
            .execute(&mut *conn)
            .await?;

            // 4. Charge adjust + slot decrement — only when step 2 found capacity slots.
            //    The charge is MONOTONICALLY NON-INCREASING: `LEAST(old_charge, remaining)`
            //    where remaining = expected - success - failures recomputed from the
            //    now-updated counters (step 3). A PATCH that raised expected_count mid-run
            //    does NOT raise the charge (same never-recompute-upward discipline as
            //    release). `delta = old_charge - new_charge >= 0` is the amount to release
            //    from each of the task's cap: slots. bigint arithmetic in the remaining
            //    computation avoids an int4 underflow when success + failures is large.
            //
            //    Terminal-race safety: a task terminalized by a concurrent tx had its
            //    charge released + NULLed there, so `capacity_charge > 0` excludes it; the
            //    step-1 task lock serializes the two txs before either takes cap: slots.
            if !cap_slots.is_empty() {
                diesel::sql_query(
                    "WITH adj AS ( \
                        UPDATE task t \
                        SET capacity_charge = GREATEST( \
                                LEAST(t.capacity_charge::bigint, \
                                      COALESCE(t.expected_count, 0)::bigint - t.success::bigint - t.failures::bigint), \
                                0)::int \
                        FROM (SELECT id, capacity_charge AS old_charge \
                              FROM task WHERE id = ANY($1) AND capacity_charge > 0) s \
                        WHERE t.id = s.id \
                        RETURNING t.id, t.claimed_slot_keys, (s.old_charge - t.capacity_charge) AS delta \
                     ), \
                     key_deltas AS ( \
                        SELECT k AS lock_key, SUM(delta)::int AS dec \
                        FROM adj, unnest(claimed_slot_keys) AS k \
                        WHERE k LIKE 'cap:%' AND delta > 0 \
                        GROUP BY k \
                     ) \
                     UPDATE rule_slot rs SET used = GREATEST(rs.used - kd.dec, 0) \
                     FROM key_deltas kd WHERE rs.lock_key = kd.lock_key",
                )
                .bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(&ids)
                .execute(&mut *conn)
                .await?;
            }

            Ok(updated)
        })
    })
    .await
}

/// Test/inspection entry: apply ONE batch of counter deltas through the real flush path
/// (counter UPDATE + the D1/7.3b capacity-slot maintenance) in a single call, exactly as
/// the steady-state updater loop does per drained batch. Exposed so integration tests can
/// drive the capacity-slot delta deterministically without spinning the full loop or
/// racing its flush interval. `updates` is `(task_id, +success, +failures)`.
pub async fn run_counter_flush_once(
    conn: &mut Conn<'_>,
    updates: &[(uuid::Uuid, i32, i32)],
) -> Result<(), DbError> {
    let updates: Vec<Update> = updates
        .iter()
        .map(|(id, success, failures)| (*id, i64::from(*success), i64::from(*failures)))
        .collect();
    handle_batch_with_counts(&updates, conn).await.map(|_| ())
}

/// Apply counter update for a single task. Used by the per-row anti-poison
/// recovery and the final shutdown flush.
async fn handle_one_with_counts(
    task_id: uuid::Uuid,
    conn: &mut Conn<'_>,
    success_count: i64,
    failure_count: i64,
) -> Result<usize, DbError> {
    handle_batch_with_counts(&[(task_id, success_count, failure_count)], conn).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_event_accumulates_into_single_entry() {
        let data = DashMap::new();
        let id = uuid::Uuid::new_v4();
        apply_event(
            &data,
            &UpdateEvent {
                success: 3,
                failures: 1,
                task_id: id,
            },
        );
        apply_event(
            &data,
            &UpdateEvent {
                success: 2,
                failures: 4,
                task_id: id,
            },
        );
        let entry = data.get(&id).unwrap();
        assert_eq!(AtomicI64::load(&entry.success, Ordering::Relaxed), 5);
        assert_eq!(AtomicI64::load(&entry.failures, Ordering::Relaxed), 5);
    }

    #[test]
    fn apply_event_accumulator_does_not_wrap_at_i32_max() {
        let data = DashMap::new();
        let id = uuid::Uuid::new_v4();
        for _ in 0..2 {
            apply_event(
                &data,
                &UpdateEvent {
                    success: i32::MAX,
                    failures: 0,
                    task_id: id,
                },
            );
        }
        assert_eq!(
            AtomicI64::load(&data.get(&id).unwrap().success, Ordering::Relaxed),
            i64::from(i32::MAX) * 2
        );
    }

    #[test]
    fn drain_updates_swaps_and_filters_zero() {
        let data = DashMap::new();
        let a = uuid::Uuid::new_v4();
        let b = uuid::Uuid::new_v4();
        apply_event(
            &data,
            &UpdateEvent {
                success: 7,
                failures: 0,
                task_id: a,
            },
        );
        // b accumulates a net-zero (should be filtered out of the drain)
        apply_event(
            &data,
            &UpdateEvent {
                success: 0,
                failures: 0,
                task_id: b,
            },
        );

        let mut updates = drain_updates(&data);
        updates.sort();
        assert_eq!(updates, vec![(a, 7, 0)]);

        // After draining, the counters were swapped to zero.
        assert_eq!(
            AtomicI64::load(&data.get(&a).unwrap().success, Ordering::Relaxed),
            0
        );
    }

    #[test]
    fn requeue_readds_counts_only_for_given_rows() {
        let data = DashMap::new();
        let a = uuid::Uuid::new_v4();
        let b = uuid::Uuid::new_v4();
        // a already has some residual counts; requeue must ADD, not replace.
        apply_event(
            &data,
            &UpdateEvent {
                success: 1,
                failures: 1,
                task_id: a,
            },
        );

        requeue(&data, vec![(a, 4, 0), (b, 0, 9)]);

        assert_eq!(
            AtomicI64::load(&data.get(&a).unwrap().success, Ordering::Relaxed),
            5
        );
        assert_eq!(
            AtomicI64::load(&data.get(&a).unwrap().failures, Ordering::Relaxed),
            1
        );
        assert_eq!(
            AtomicI64::load(&data.get(&b).unwrap().failures, Ordering::Relaxed),
            9
        );
    }

    /// Drain-before-flush at shutdown: events still buffered in the channel when
    /// the shutdown signal fires must land in the map before the receiver exits,
    /// so the final flush snapshot is complete (audit A7, C6). Without the
    /// `try_recv` drain the receiver would race the shutdown and drop them.
    #[tokio::test]
    async fn receiver_drains_buffered_events_before_exit() {
        let data = Arc::new(DashMap::new());
        let (tx, rx) = mpsc::channel(1024);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        // Enqueue a burst of events WITHOUT giving the receiver a chance to run,
        // so they sit buffered in the channel.
        let ids: Vec<uuid::Uuid> = (0..50).map(|_| uuid::Uuid::new_v4()).collect();
        for id in &ids {
            tx.send(UpdateEvent {
                success: 1,
                failures: 0,
                task_id: *id,
            })
            .await
            .unwrap();
        }

        let handle = tokio::spawn(receiver_drain_loop(Arc::clone(&data), rx, shutdown_rx));

        // Trigger shutdown; the receiver must drain the buffered burst before exit.
        shutdown_tx.send(true).unwrap();
        handle.await.unwrap();

        assert_eq!(
            data.len(),
            ids.len(),
            "every buffered event must be drained into the map before shutdown"
        );
        for id in &ids {
            assert_eq!(
                AtomicI64::load(&data.get(id).unwrap().success, Ordering::Relaxed),
                1
            );
        }
    }
}
