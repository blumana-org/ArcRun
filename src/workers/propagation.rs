use crate::{
    Conn,
    db_operation::{self, DbError},
    metrics,
    models::{StatusKind, Task},
};
use diesel::prelude::*;
use diesel_async::RunQueryDsl;

use super::webhooks::{
    enqueue_cancel_outbox, enqueue_end_outbox, enqueue_outbox_for_canceled_ancestors,
};

/// Propagates task completion to dependent children using batched queries.
///
/// When a parent task completes:
/// 1. If parent failed/canceled: batch-mark all requires_success children as Failure
/// 2. Batch-decrement wait_finished for all remaining children in Waiting/Paused status
/// 3. Batch-decrement wait_success for children where parent succeeded AND requires_success
/// 4. Batch-transition children to Pending where both counters reach 0
///
/// Returns the list of all task IDs that were cascade-failed, so callers can fire
/// on_failure webhooks for them after the transaction commits.
///
/// This uses O(1) queries per propagation level instead of O(N) per child. The
/// direct level is handled here; the failure cascade below it is then walked
/// **level by level** (a frontier BFS in `cascade_failure_frontier`, B5) rather
/// than recursing once per failed child, so a root failure over N descendants
/// costs O(depth) statements in this transaction instead of O(N).
///
/// **Paused children (Audit 2, A3)**: a `Paused` task is NOT protected from its
/// dependencies. The cascade-fail (step 1) and the counter decrements (steps 2-3)
/// apply to `Paused` children exactly like `Waiting` ones — so a pause never
/// silently strands `wait_*` counters or shields a task from a required parent's
/// failure. The **transition to Pending (step 4) stays `Waiting`-only**: a `Paused`
/// task whose counters reach 0 remains `Paused` (only an explicit resume moves it
/// out of `Paused`).
///
/// **Deadlock avoidance (Audit 2, A9)**: the batched `UPDATE … WHERE id = ANY(...)`
/// statements below lock rows in the query planner's chosen order, not the array
/// order. Two propagations of parents that share children (a "diamond" DAG) — or a
/// propagation crossing the batch-updater flush — could therefore acquire the same
/// child locks in opposite orders and deadlock (Postgres `40P01`), surfacing as a
/// 500 on the PATCH. To give every propagation a single, canonical acquisition
/// order, this function **pre-locks the whole level's child set once** with a
/// `SELECT id … WHERE id = ANY(...) ORDER BY id FOR UPDATE` before any UPDATE. Since
/// `propagate_to_children` always runs inside a transaction (caller invariant), the
/// locks are held until COMMIT, so two concurrent diamond propagations now queue on
/// the same globally-ordered locks instead of cycling. The pre-lock deliberately
/// does NOT filter on status (locking an already-terminal child row is harmless —
/// the status-guarded UPDATEs below simply won't match it), so the ordering is
/// stable regardless of how far each child has progressed.
///
/// **Assumed residual**: the failure cascade (each level of
/// `cascade_failure_frontier` locks its own child set in a nested acquisition,
/// after the previous level's locks) and a flush-vs-propagation crossing *through
/// the parent row* remain theoretically possible, as there is no single
/// acquisition order spanning levels. Those are left
/// to Postgres's deadlock detector + the batch-updater's per-row fallback (see
/// `handle_batch_with_counts`); no generic `40P01` retry is added here (A9 decision).
#[tracing::instrument(name = "propagate_to_children", level = "debug", skip(conn), fields(parent_id = %parent_id, status = ?result_status))]
pub(crate) async fn propagate_to_children<'a>(
    parent_id: &uuid::Uuid,
    result_status: &StatusKind,
    conn: &mut Conn<'a>,
) -> Result<Vec<uuid::Uuid>, DbError> {
    use crate::schema::link::dsl as link_dsl;
    use crate::schema::task::dsl as task_dsl;

    let parent_succeeded = result_status == &StatusKind::Success;
    let parent_failed =
        result_status == &StatusKind::Failure || result_status == &StatusKind::Canceled;

    // Record dependency propagation metric
    let outcome = if parent_succeeded {
        "success"
    } else {
        "failure"
    };
    metrics::record_dependency_propagation(outcome);

    // Get all children of this parent task
    let children_links: Vec<(uuid::Uuid, bool)> = link_dsl::link
        .filter(link_dsl::parent_id.eq(parent_id))
        .select((link_dsl::child_id, link_dsl::requires_success))
        .load::<(uuid::Uuid, bool)>(conn)
        .await?;

    if children_links.is_empty() {
        return Ok(vec![]);
    }

    // A9: pre-lock the entire level's child set in one globally-ordered statement,
    // BEFORE any batched UPDATE below. `SELECT … ORDER BY id FOR UPDATE` forces a
    // canonical lock-acquisition order, so two concurrent propagations that share
    // children (diamond DAG) can no longer acquire those locks in opposite orders
    // and deadlock. No status filter here on purpose: locking a terminal child row
    // is harmless (the guarded UPDATEs won't match it), and skipping the filter
    // keeps the order stable no matter each child's progress. Held until COMMIT
    // because this always runs inside a transaction (caller invariant).
    let mut lock_ids: Vec<uuid::Uuid> = children_links.iter().map(|(cid, _)| *cid).collect();
    lock_ids.sort();
    lock_ids.dedup();
    let _locked: Vec<uuid::Uuid> = task_dsl::task
        .filter(task_dsl::id.eq_any(&lock_ids))
        .select(task_dsl::id)
        .order(task_dsl::id.asc())
        .for_update()
        .load::<uuid::Uuid>(conn)
        .await?;

    // Split children into groups for batched operations
    let mut fail_child_ids: Vec<uuid::Uuid> = Vec::new();
    let mut decrement_child_ids: Vec<uuid::Uuid> = Vec::new();
    let mut decrement_success_child_ids: Vec<uuid::Uuid> = Vec::new();

    for (child_id, requires_success) in &children_links {
        if parent_failed && *requires_success {
            fail_child_ids.push(*child_id);
        } else {
            decrement_child_ids.push(*child_id);
            if parent_succeeded && *requires_success {
                decrement_success_child_ids.push(*child_id);
            }
        }
    }

    // Collect all cascade-failed task IDs (direct + recursive)
    let mut all_cascade_failed: Vec<uuid::Uuid> = Vec::new();

    // 1. Batch-mark failed children (parent failed + requires_success)
    if !fail_child_ids.is_empty() {
        let failure_reason = format!("Required parent task {} failed", parent_id);
        let failed_ids: Vec<uuid::Uuid> = diesel::update(
            task_dsl::task.filter(
                task_dsl::id
                    .eq_any(&fail_child_ids)
                    // A3: cascade-fail reaches Paused children too — a pause does not
                    // protect a task from a required parent's failure.
                    .and(task_dsl::status.eq_any([StatusKind::Waiting, StatusKind::Paused])),
            ),
        )
        .set((
            task_dsl::status.eq(StatusKind::Failure),
            task_dsl::failure_reason.eq(&failure_reason),
            task_dsl::ended_at.eq(diesel::dsl::now),
        ))
        .returning(task_dsl::id)
        .get_results::<uuid::Uuid>(conn)
        .await?;

        for fid in &failed_ids {
            metrics::record_task_failed_by_dependency();
            log::info!(
                "Child task {} marked as failed due to required parent {} failure",
                fid,
                parent_id
            );
        }

        // Track direct failures
        all_cascade_failed.extend_from_slice(&failed_ids);

        // B5: cascade the failure LEVEL BY LEVEL (frontier BFS) instead of
        // recursing once per failed child. `failed_ids` is the first failure
        // frontier; each iteration resolves one whole DAG level with a constant
        // number of statements, so a root failure over N descendants costs
        // O(depth) round-trips in this transaction, not O(N).
        let deeper_failures = cascade_failure_frontier(failed_ids, conn).await?;
        all_cascade_failed.extend(deeper_failures);
    }

    // 2. Batch-decrement counters for remaining children
    if !decrement_child_ids.is_empty() {
        // Decrement wait_finished for all remaining children.
        // A3: Paused children receive the decrements too (a pause must not strand
        // the counters), so they resume with an accurate view of their deps.
        diesel::update(
            task_dsl::task.filter(
                task_dsl::id
                    .eq_any(&decrement_child_ids)
                    .and(task_dsl::status.eq_any([StatusKind::Waiting, StatusKind::Paused])),
            ),
        )
        .set(task_dsl::wait_finished.eq(task_dsl::wait_finished - 1))
        .execute(conn)
        .await?;

        // Decrement wait_success only for children that require it
        if !decrement_success_child_ids.is_empty() {
            diesel::update(
                task_dsl::task.filter(
                    task_dsl::id
                        .eq_any(&decrement_success_child_ids)
                        .and(task_dsl::status.eq_any([StatusKind::Waiting, StatusKind::Paused])),
                ),
            )
            .set(task_dsl::wait_success.eq(task_dsl::wait_success - 1))
            .execute(conn)
            .await?;
        }

        // 3. Batch-transition to Pending where both counters reach 0.
        // A3: this stays Waiting-only on purpose — a Paused task whose counters
        // reach 0 stays Paused; only an explicit resume moves it out of Paused.
        let unblocked_ids: Vec<uuid::Uuid> = diesel::update(
            task_dsl::task.filter(
                task_dsl::id
                    .eq_any(&decrement_child_ids)
                    .and(task_dsl::status.eq(StatusKind::Waiting))
                    .and(task_dsl::wait_finished.eq(0))
                    .and(task_dsl::wait_success.eq(0)),
            ),
        )
        .set(task_dsl::status.eq(StatusKind::Pending))
        .returning(task_dsl::id)
        .get_results::<uuid::Uuid>(conn)
        .await?;

        for uid in &unblocked_ids {
            metrics::record_task_unblocked();
            metrics::record_status_transition("Waiting", "Pending");
            log::info!("Child task {} transitioned from Waiting to Pending", uid);
        }
    }

    Ok(all_cascade_failed)
}

/// Cascade a failure LEVEL BY LEVEL, starting from `frontier` — a set of tasks
/// that have just been marked `Failure` because a required parent failed.
///
/// **B5 (perf):** this replaces the old per-failed-child recursion (a `Box::pin`
/// self-call on `propagate_to_children` for every failed node). That recursion
/// ran O(N) sequential SELECT/UPDATE round-trips inside the PATCH transaction for
/// a root failure over N descendants — locks held, latency in seconds. The
/// frontier walk resolves one whole DAG level per iteration with a constant
/// number of statements, so the cost is O(depth) instead of O(nodes).
///
/// Every node in `frontier` (and every node it later fails) is in `Failure`, so
/// the whole cascade is a uniform failure propagation: `parent_failed = true`,
/// `parent_succeeded = false`. Per level the children split into a fail set
/// (`requires_success = true`) and a plain `wait_finished` decrement set
/// (`requires_success = false`); `wait_success` is **never** touched (no parent
/// in the cascade succeeded). The A9 pre-lock and the A3 status filters
/// (cascade-fail/decrement reach `Paused`; the Pending unblock stays
/// `Waiting`-only) are preserved on every level exactly as in
/// `propagate_to_children`.
///
/// Returns the ids failed at levels **below** the input frontier (the input
/// frontier is already tracked by the caller).
///
/// **Metric note:** `record_dependency_propagation("failure")` is now recorded
/// once per node in each processed frontier — the input frontier's own nodes
/// included. This matches the old semantics exactly: the recursion fired one
/// `propagate_to_children` call (hence one metric) per failed node, whether or
/// not it had children.
async fn cascade_failure_frontier<'a>(
    mut frontier: Vec<uuid::Uuid>,
    conn: &mut Conn<'a>,
) -> Result<Vec<uuid::Uuid>, DbError> {
    use crate::schema::link::dsl as link_dsl;
    use crate::schema::task::dsl as task_dsl;

    let mut deeper_failed: Vec<uuid::Uuid> = Vec::new();

    while !frontier.is_empty() {
        // One dependency-propagation metric per node in this frontier (see the
        // "Metric note" above — preserves the old per-node recursive semantics).
        for _ in &frontier {
            metrics::record_dependency_propagation("failure");
        }

        // ONE SELECT for the whole frontier's outgoing links.
        let children_links: Vec<(uuid::Uuid, bool)> = link_dsl::link
            .filter(link_dsl::parent_id.eq_any(&frontier))
            .select((link_dsl::child_id, link_dsl::requires_success))
            .load::<(uuid::Uuid, bool)>(conn)
            .await?;

        if children_links.is_empty() {
            break;
        }

        // A9: pre-lock the level's whole child set in one globally-ordered
        // statement BEFORE any UPDATE (identical rationale to
        // `propagate_to_children`). A diamond inside the cascade means a child
        // appears once per parent in the frontier — dedup the union for the lock
        // set. No status filter (locking a terminal row is harmless).
        let mut lock_ids: Vec<uuid::Uuid> = children_links.iter().map(|(cid, _)| *cid).collect();
        lock_ids.sort();
        lock_ids.dedup();
        let _locked: Vec<uuid::Uuid> = task_dsl::task
            .filter(task_dsl::id.eq_any(&lock_ids))
            .select(task_dsl::id)
            .order(task_dsl::id.asc())
            .for_update()
            .load::<uuid::Uuid>(conn)
            .await?;

        // Split (all frontier parents failed): requires_success -> fail set,
        // otherwise -> wait_finished decrement MULTIPLICITY map. The fail set is
        // deduped (the Failure transition is idempotent), but the decrements are
        // NOT: a child with N non-required parents failing in this same frontier
        // must be decremented N times (`wait_finished` counts every dependency —
        // the old per-failed-child recursion applied one decrement per parent).
        // A child that lands in BOTH the fail set (required by parent A) and the
        // decrement map (not-required by parent B) is failed first; the
        // decrement's status filter below then no longer matches it, so it ends
        // `Failure` (the deterministic fail-before-decrement order — one of the
        // pre-fix orderings, now guaranteed).
        let mut fail_set: Vec<uuid::Uuid> = Vec::new();
        let mut decrement_counts: std::collections::HashMap<uuid::Uuid, i32> =
            std::collections::HashMap::new();
        for (child_id, requires_success) in &children_links {
            if *requires_success {
                fail_set.push(*child_id);
            } else {
                *decrement_counts.entry(*child_id).or_insert(0) += 1;
            }
        }
        fail_set.sort();
        fail_set.dedup();

        // Children actually transitioned to Failure this level = the next frontier.
        let mut next_frontier: Vec<uuid::Uuid> = Vec::new();

        // 1. Cascade-fail (A3: reaches Paused too). RETURNING drives the next level.
        if !fail_set.is_empty() {
            let failed_ids: Vec<uuid::Uuid> = diesel::update(
                task_dsl::task.filter(
                    task_dsl::id
                        .eq_any(&fail_set)
                        .and(task_dsl::status.eq_any([StatusKind::Waiting, StatusKind::Paused])),
                ),
            )
            .set((
                task_dsl::status.eq(StatusKind::Failure),
                task_dsl::failure_reason.eq("Required parent task failed"),
                task_dsl::ended_at.eq(diesel::dsl::now),
            ))
            .returning(task_dsl::id)
            .get_results::<uuid::Uuid>(conn)
            .await?;

            for fid in &failed_ids {
                metrics::record_task_failed_by_dependency();
                log::info!(
                    "Child task {} marked as failed due to a required parent failure",
                    fid
                );
            }
            deeper_failed.extend_from_slice(&failed_ids);
            next_frontier = failed_ids;
        }

        // 2. Decrement wait_finished for the non-required children (A3: Paused
        //    too), by each child's MULTIPLICITY in this level's links (N failed
        //    parents in the frontier ⇒ -N). Children are grouped by delta so the
        //    level still costs one UPDATE per DISTINCT delta (1 in the common
        //    case). wait_success is NOT touched — no parent in the cascade
        //    succeeded. Runs AFTER the fail UPDATE so a child in both sets stays
        //    Failure (its status is no longer Waiting/Paused, so it won't match).
        if !decrement_counts.is_empty() {
            let mut by_delta: std::collections::HashMap<i32, Vec<uuid::Uuid>> =
                std::collections::HashMap::new();
            for (child_id, delta) in &decrement_counts {
                by_delta.entry(*delta).or_default().push(*child_id);
            }
            let decrement_set: Vec<uuid::Uuid> = decrement_counts.keys().copied().collect();
            for (delta, ids) in by_delta {
                diesel::update(
                    task_dsl::task.filter(
                        task_dsl::id.eq_any(&ids).and(
                            task_dsl::status.eq_any([StatusKind::Waiting, StatusKind::Paused]),
                        ),
                    ),
                )
                .set(task_dsl::wait_finished.eq(task_dsl::wait_finished - delta))
                .execute(conn)
                .await?;
            }

            // 3. Unblock (A3: Waiting-only) where both counters reached 0.
            let unblocked_ids: Vec<uuid::Uuid> = diesel::update(
                task_dsl::task.filter(
                    task_dsl::id
                        .eq_any(&decrement_set)
                        .and(task_dsl::status.eq(StatusKind::Waiting))
                        .and(task_dsl::wait_finished.eq(0))
                        .and(task_dsl::wait_success.eq(0)),
                ),
            )
            .set(task_dsl::status.eq(StatusKind::Pending))
            .returning(task_dsl::id)
            .get_results::<uuid::Uuid>(conn)
            .await?;

            for uid in &unblocked_ids {
                metrics::record_task_unblocked();
                metrics::record_status_transition("Waiting", "Pending");
                log::info!("Child task {} transitioned from Waiting to Pending", uid);
            }
        }

        frontier = next_frontier;
    }

    Ok(deeper_failed)
}

// =============================================================================
// Dead-end ancestor cancellation
// =============================================================================

/// An ancestor task that was canceled by dead-end detection.
#[derive(Debug)]
pub(crate) struct CanceledAncestor {
    pub id: uuid::Uuid,
    /// True if the task's on_start webhook may have run before cancellation, i.e. it
    /// was `Running` OR `Claimed` (needs a cancel webhook). A4 fix: `Claimed` covers
    /// the whole on_start-in-flight window, so a Claimed dead-end ancestor whose
    /// consumer started work must also get a cancel notification.
    pub was_active: bool,
}

/// Row returned by the writable CTE in `cancel_dead_end_ancestors`.
#[derive(diesel::QueryableByName, Debug)]
struct CanceledDeadEndRow {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    id: uuid::Uuid,
    #[diesel(sql_type = diesel::sql_types::Text)]
    prev_status: String,
    #[diesel(sql_type = diesel::sql_types::Bool)]
    is_barrier: bool,
}

/// Detect and cancel ancestor tasks whose ALL children are already terminal
/// (dead-end detection). Iterates upward through the DAG until no more
/// dead-end ancestors are found.
///
/// Must be called inside a transaction (same conn as the status change
/// that made children terminal).
///
/// Returns the list of canceled ancestors so callers can fire webhooks
/// after the transaction commits.
pub(crate) async fn cancel_dead_end_ancestors<'a>(
    newly_terminal_ids: &[uuid::Uuid],
    conn: &mut Conn<'a>,
) -> Result<Vec<CanceledAncestor>, DbError> {
    if newly_terminal_ids.is_empty() {
        return Ok(vec![]);
    }

    let mut all_canceled: Vec<CanceledAncestor> = Vec::new();
    let mut check_ids: Vec<uuid::Uuid> = newly_terminal_ids.to_vec();

    loop {
        if check_ids.is_empty() {
            break;
        }

        let canceled: Vec<CanceledDeadEndRow> = diesel::sql_query(
            "WITH candidates AS (
                SELECT DISTINCT l.parent_id
                FROM link l
                WHERE l.child_id = ANY($1)
            ),
            to_cancel AS (
                SELECT t.id, t.status::text AS prev_status, t.dead_end_barrier AS is_barrier
                FROM task t
                JOIN candidates c ON c.parent_id = t.id
                WHERE t.status NOT IN ('success', 'failure', 'canceled')
                  AND NOT EXISTS (
                      SELECT 1 FROM link l2
                      JOIN task c2 ON c2.id = l2.child_id
                      WHERE l2.parent_id = t.id
                        AND c2.status NOT IN ('success', 'failure', 'canceled')
                  )
                FOR UPDATE OF t SKIP LOCKED
            )
            UPDATE task
            SET status = 'canceled',
                failure_reason = 'All child tasks already terminated',
                ended_at = now(),
                last_updated = now()
            FROM to_cancel
            WHERE task.id = to_cancel.id
            RETURNING task.id, to_cancel.prev_status, to_cancel.is_barrier",
        )
        .bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(&check_ids)
        .load::<CanceledDeadEndRow>(conn)
        .await?;

        if canceled.is_empty() {
            break;
        }

        // Next iteration: only propagate upward through non-barrier tasks
        check_ids = canceled
            .iter()
            .filter(|r| !r.is_barrier)
            .map(|r| r.id)
            .collect();

        for row in canceled {
            metrics::record_task_canceled_dead_end();
            metrics::record_status_transition(&row.prev_status, "Canceled");
            log::info!(
                "Dead-end detection: canceled ancestor task {} (was {}{})",
                row.id,
                row.prev_status,
                if row.is_barrier { ", barrier" } else { "" }
            );
            all_canceled.push(CanceledAncestor {
                id: row.id,
                was_active: row.prev_status == "running" || row.prev_status == "claimed",
            });
        }
    }

    Ok(all_canceled)
}

pub async fn cancel_task<'a>(
    task_id: &uuid::Uuid,
    dead_end_enabled: bool,
    conn: &mut Conn<'a>,
) -> Result<(), DbError> {
    use crate::schema::task::dsl::*;
    let task_id = *task_id;

    // Single transaction: cancel + propagation + dead-end detection + outbox enqueue
    // are all atomic. Webhook notifications are enqueued into the transactional outbox
    // here (no reqwest in the call path); the delivery loop sends them async.
    db_operation::run_in_transaction(conn, |conn| {
        Box::pin(async move {
            // A10: distinguish "task absent" (→ 404) from "wrong state" (→ 400).
            // A bare `.first()?` would surface a missing task as a diesel NotFound
            // that maps to a 500 Database error, so use `.optional()` and raise a
            // typed `NotFound` instead.
            let t = match task
                .filter(id.eq(task_id))
                .for_update()
                .first::<Task>(conn)
                .await
                .optional()?
            {
                Some(t) => t,
                None => {
                    return Err(crate::error::ArcRunError::NotFound {
                        message: format!("Task {} not found", task_id),
                    });
                }
            };

            match t.status {
                // A10: `Waiting` is now cancelable too — it lets an operator prune a
                // not-yet-eligible DAG branch. A Waiting task never received on_start,
                // so (like Pending) no cancel webhook is enqueued below; its children
                // still cascade via `propagate_to_children` (Canceled == Failed).
                StatusKind::Pending
                | StatusKind::Waiting
                | StatusKind::Paused
                | StatusKind::Claimed
                | StatusKind::Running => {}
                other => {
                    return Err(crate::error::ArcRunError::InvalidState {
                        message: format!(
                            "Cannot cancel task in {:?} state (only Pending, Waiting, Paused, \
                             Claimed, or Running tasks can be canceled)",
                            other
                        ),
                    });
                }
            }

            diesel::update(task.filter(id.eq(task_id)))
                .set((
                    status.eq(StatusKind::Canceled),
                    ended_at.eq(diesel::dsl::now),
                    last_updated.eq(diesel::dsl::now),
                ))
                .execute(conn)
                .await?;

            // Propagate cancellation to dependent children (inside tx)
            let cascade_failed =
                propagate_to_children(&task_id, &StatusKind::Canceled, conn).await?;

            // Dead-end ancestor cancellation (inside tx)
            let canceled_ancestors = if dead_end_enabled {
                let mut terminal_ids = vec![task_id];
                terminal_ids.extend_from_slice(&cascade_failed);
                cancel_dead_end_ancestors(&terminal_ids, conn).await?
            } else {
                vec![]
            };

            // Enqueue outbox rows (inside tx):
            // - cancel notification for the canceled task if on_start may have run,
            // - on_failure for each cascade-failed child,
            // - cancel/on_failure for dead-end ancestors.
            //
            // A4 fix: enqueue the cancel for `Claimed` as well as `Running`. `Claimed`
            // is NOT "on_start never called" — it covers the whole on_start
            // webhook-in-flight window (permit queue + up to the webhook timeout). A
            // consumer that already received on_start and started work must get a cancel.
            // Safe: the delivery loop's start-before-end gate holds this cancel row while
            // the task's start row is pending+fresh (the start row is completed in the
            // same tx as the Claimed->Running transition AND the cancel-action save — see
            // start_loop A4), so the cancel is delivered only after the actions exist; a
            // Claimed task that never returned a cancel action prefetches zero actions ⇒
            // fast-path success (no HTTP).
            if matches!(t.status, StatusKind::Running | StatusKind::Claimed) {
                enqueue_cancel_outbox(&task_id, conn).await?;
            }
            for child_id in &cascade_failed {
                enqueue_end_outbox(child_id, crate::notification::TerminalStatus::FAILURE, conn)
                    .await?;
            }
            enqueue_outbox_for_canceled_ancestors(&canceled_ancestors, conn).await?;

            // Batch-complete detection (D2): decrement `batch.remaining` for the
            // canceled task + its cascade-failed children + dead-end-canceled ancestors.
            let mut terminal_ids = vec![task_id];
            terminal_ids.extend_from_slice(&cascade_failed);
            terminal_ids.extend(canceled_ancestors.iter().map(|a| a.id));
            db_operation::decrement_batch_remaining_for_tasks(
                &mut *conn,
                &terminal_ids,
                "cancel_task",
            )
            .await?;

            // Slot release (Audit 2, D1): if the canceled task was Claimed/Running it
            // held its slots; dead-end ancestors that were active held theirs. A cancel
            // of a Pending/Waiting/Paused task released nothing (NULL keys) — the
            // `claimed_slot_keys IS NOT NULL` gate encodes exactly the "only if it was
            // Claimed/Running" rule, so passing the whole terminal set is correct.
            db_operation::release_slots_for_tasks(&mut *conn, &terminal_ids).await?;

            Ok(())
        })
    })
    .await?;

    metrics::record_task_cancelled();
    Ok(())
}
