use crate::{
    Conn, dtos, metrics,
    models::{self, StatusKind},
    workers,
};
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use uuid::Uuid;

use super::task_crud::release_slots_for_tasks;
use super::webhook_execution::{
    decrement_batch_remaining_for_tasks, zero_batch_remaining_and_complete,
};
use super::{DbError, run_in_transaction, task_crud::insert_actions};

/// Result of attempting to update a running task.
///
/// A10: the outcome is now precise enough for an idempotent, retry-safe PATCH. When
/// the guarded UPDATE matches 0 rows, a follow-up SELECT classifies the task so the
/// handler can answer 404 / 200 (idempotent) / 409 instead of one blanket 404.
#[derive(Debug, PartialEq)]
pub enum UpdateTaskResult {
    /// Task was successfully updated (transitioned from Running/Claimed).
    Updated,
    /// Task does not exist.
    NotFound,
    /// Task already sits in the requested terminal status — an idempotent no-op.
    /// No propagation is re-run and no outbox row is (re-)enqueued. Lets a client
    /// safely retry a PATCH whose response was lost.
    AlreadyInRequestedState,
    /// Task exists but is in a state that cannot be updated to the requested status
    /// (neither Running/Claimed nor already the requested status). Carries the
    /// current status so the caller can surface it in a 409.
    Conflict(StatusKind),
}

#[tracing::instrument(name = "update_running_task", level = "debug", skip(conn, dto), fields(task_id = %task_id))]
pub async fn update_running_task<'a>(
    conn: &mut Conn<'a>,
    task_id: Uuid,
    dto: dtos::UpdateTaskDto,
    dead_end_enabled: bool,
) -> Result<UpdateTaskResult, DbError> {
    use crate::schema::task::dsl::*;
    use tracing::Instrument;
    let s = dto.status;
    let final_status_clone = dto.status;

    let has_status_change = dto.status.is_some();

    let res = if has_status_change {
        // Status change: transaction needed for atomic UPDATE + propagation +
        // outbox enqueue. The webhook notifications (end + cascade + dead-end
        // ancestors) are enqueued into the transactional outbox INSIDE this
        // transaction, so "API response = durable state" holds and a crash after
        // commit cannot lose the notifications. Actual HTTP delivery is async via
        // the delivery loop.
        let (rows, _cascade_failed, _ancestors) = run_in_transaction(conn, |conn| {
            Box::pin(async move {
                // Accept both Running and Claimed states to avoid a race condition
                // where the on_start webhook target PATCHes the task before the
                // worker loop transitions it from Claimed to Running.
                let res = diesel::update(
                    task.filter(
                        id.eq(task_id).and(
                            status
                                .eq(models::StatusKind::Running)
                                .or(status.eq(models::StatusKind::Claimed)),
                        ),
                    ),
                )
                .set((
                    last_updated.eq(diesel::dsl::now),
                    dto.new_success.map(|e| success.eq(success + e)),
                    dto.new_failures.map(|e| failures.eq(failures + e)),
                    s.filter(|e| {
                        e == &models::StatusKind::Success || e == &models::StatusKind::Failure
                    })
                    .map(|_| ended_at.eq(diesel::dsl::now)),
                    dto.metadata.map(|m| metadata.eq(m)),
                    dto.status.as_ref().map(|m| status.eq(m)),
                    dto.failure_reason.map(|m| failure_reason.eq(m)),
                    dto.expected_count.map(|c| expected_count.eq(c)),
                    dto.priority.map(|p| priority.eq(p)),
                ))
                .execute(conn)
                .await?;

                let mut cascade = Vec::new();
                let mut ancestors = Vec::new();
                if res == 1 {
                    if let Some(ref final_status) = dto.status {
                        cascade =
                            workers::propagate_to_children(&task_id, final_status, conn).await?;

                        // Dead-end ancestor cancellation
                        if dead_end_enabled {
                            let mut terminal_ids = vec![task_id];
                            terminal_ids.extend_from_slice(&cascade);
                            ancestors =
                                workers::cancel_dead_end_ancestors(&terminal_ids, conn).await?;
                        }

                        // Enqueue outbox rows for end + cascade + dead-end ancestors
                        // (inside the tx, no reqwest in the call path).
                        workers::enqueue_end_outbox_with_cascade(
                            &task_id,
                            (*final_status).try_into()?,
                            &cascade,
                            conn,
                        )
                        .await?;
                        workers::enqueue_outbox_for_canceled_ancestors(&ancestors, conn).await?;

                        // Batch-complete detection (D2): every task terminalized in this
                        // tx — the PATCHed task (Success/Failure, guaranteed terminal by
                        // `validate_update_task`), its cascade-failed children, and any
                        // dead-end-canceled ancestors — decrements `batch.remaining`. A
                        // batch that reaches 0 (with a registered webhook) enqueues the
                        // batch_complete row in this same tx.
                        let mut terminal_ids = vec![task_id];
                        terminal_ids.extend_from_slice(&cascade);
                        terminal_ids.extend(ancestors.iter().map(|a| a.id));
                        decrement_batch_remaining_for_tasks(
                            conn,
                            &terminal_ids,
                            "update_running_task",
                        )
                        .await?;

                        // Slot release (Audit 2, D1): the PATCHed task was Running/Claimed
                        // (held its slots); dead-end-canceled ancestors that were
                        // Running/Claimed held theirs too. Cascade-failed children were
                        // Waiting/Paused (never claimed ⇒ NULL keys ⇒ no-op). The
                        // `claimed_slot_keys IS NOT NULL` gate releases only real holders,
                        // so passing the whole terminal set is correct.
                        release_slots_for_tasks(conn, &terminal_ids).await?;
                    }
                }

                Ok((res, cascade, ancestors))
            })
        })
        .instrument(tracing::info_span!("tx_update_and_propagate"))
        .await?;
        rows
    } else {
        // Counter-only update: no transaction needed, autocommit for minimal row lock
        diesel::update(
            task.filter(
                id.eq(task_id).and(
                    status
                        .eq(models::StatusKind::Running)
                        .or(status.eq(models::StatusKind::Claimed)),
                ),
            ),
        )
        .set((
            last_updated.eq(diesel::dsl::now),
            dto.new_success.map(|e| success.eq(success + e)),
            dto.new_failures.map(|e| failures.eq(failures + e)),
            dto.metadata.map(|m| metadata.eq(m)),
            dto.expected_count.map(|c| expected_count.eq(c)),
            dto.priority.map(|p| priority.eq(p)),
        ))
        .execute(conn)
        .await?
    };

    // After commit: record metrics only. Webhook notifications were enqueued into
    // the outbox inside the transaction above and are delivered async by the
    // delivery loop (Lot 2) — no reqwest in this call path.
    if let Some(ref final_status) = final_status_clone {
        if res == 1 {
            let outcome = match final_status {
                models::StatusKind::Success => "success",
                models::StatusKind::Failure => "failure",
                _ => "other",
            };
            metrics::record_status_transition("Running", outcome);
        } else {
            log::warn!(
                "update_running_task: task {} was not in Running/Claimed state, skipping outbox/propagation",
                task_id
            );
        }
    }

    if res == 1 {
        return Ok(UpdateTaskResult::Updated);
    }

    // 0 rows matched the guarded UPDATE. For a status-changing PATCH, run a
    // follow-up SELECT to classify the miss (A10): absent → NotFound; already the
    // requested status → idempotent no-op; any other state → Conflict. The tx above
    // committed nothing, so this autocommit read only shapes the diagnostic — it does
    // NOT re-run propagation or re-enqueue the outbox (the guarded UPDATE never fired).
    if let Some(requested) = final_status_clone {
        let current: Option<StatusKind> = task
            .filter(id.eq(task_id))
            .select(status)
            .first::<StatusKind>(conn)
            .await
            .optional()?;
        return Ok(match current {
            None => UpdateTaskResult::NotFound,
            Some(s) if s == requested => UpdateTaskResult::AlreadyInRequestedState,
            Some(s) => UpdateTaskResult::Conflict(s),
        });
    }

    // Counter-only update that matched nothing (task absent or not Running/Claimed).
    Ok(UpdateTaskResult::NotFound)
}

/// Persist the cancel actions a task's on_start webhook returned, validating each
/// webhook URL first (SSRF protection via cancel action responses).
///
/// **Best-effort validation (A4)**: an individual cancel action that fails SSRF/param
/// validation is logged and *skipped*, NOT surfaced as an error. This is what lets the
/// call run INSIDE the Claimed->Running / start-row-completion transaction (see
/// `start_loop::execute_webhook_for_task`): committing the cancel actions atomically
/// with the start-row completion closes the race where a concurrently-enqueued cancel
/// outbox row (from a DELETE/stop_batch fired while on_start was in flight) could be
/// prefetched by the delivery loop *before* the actions existed — the consumer would
/// then never receive a cancel and keep running zombie work. A validation failure must
/// not roll back the transition (4.2 decision), hence the swallow. Only a real DB
/// error (from `insert_actions`) propagates and rolls back the tx.
pub(crate) async fn save_cancel_actions<'a>(
    conn: &mut Conn<'a>,
    task_id: uuid::Uuid,
    cancel_tasks: &[dtos::NewActionDto],
) -> Result<(), DbError> {
    if cancel_tasks.is_empty() {
        return Ok(());
    }

    // Validate each cancel action; skip (log) the invalid ones instead of failing.
    let mut valid: Vec<dtos::NewActionDto> = Vec::with_capacity(cancel_tasks.len());
    for action_dto in cancel_tasks {
        match crate::validation::validate_action_params(&action_dto.kind, &action_dto.params) {
            Ok(()) => valid.push(action_dto.clone()),
            Err(e) => log::warn!(
                "Skipping cancel action for task {} due to validation failure: {}",
                task_id,
                e
            ),
        }
    }

    if valid.is_empty() {
        return Ok(());
    }

    insert_actions(
        task_id,
        &valid,
        &models::TriggerKind::Cancel,
        &models::TriggerCondition::Success, // Condition doesn't matter for cancel
        conn,
    )
    .await?;
    Ok(())
}

/// Mark a task as failed with a reason. Returns true if the task was updated.
/// Used when on_start webhook fails after claim.
pub(crate) async fn mark_task_failed<'a>(
    conn: &mut Conn<'a>,
    task_id: &uuid::Uuid,
    reason: &str,
) -> Result<bool, DbError> {
    use crate::schema::task::dsl::*;
    use diesel::dsl::now;

    let updated = diesel::update(
        task.filter(
            id.eq(task_id).and(
                status
                    .eq(StatusKind::Running)
                    .or(status.eq(StatusKind::Claimed)),
            ),
        ),
    )
    .set((
        status.eq(StatusKind::Failure),
        failure_reason.eq(reason),
        ended_at.eq(now),
        last_updated.eq(now),
    ))
    .execute(conn)
    .await?;
    Ok(updated == 1)
}

/// Mark a task as failed and propagate failure to children, enqueueing the
/// on_failure outbox rows in the same transaction (delivery is async, Lot 2).
/// Used when on_start webhook fails after claim.
pub(crate) async fn fail_task_and_propagate<'a>(
    conn: &mut Conn<'a>,
    task_id: &uuid::Uuid,
    reason: &str,
    dead_end_enabled: bool,
) -> Result<(), DbError> {
    // Wrap status update + propagation in a transaction
    let tid = *task_id;
    let reason_owned = reason.to_string();
    let updated = run_in_transaction(conn, |conn| {
        Box::pin(async move {
            let updated = mark_task_failed(conn, &tid, &reason_owned).await?;
            if updated {
                let cascade =
                    workers::propagate_to_children(&tid, &StatusKind::Failure, conn).await?;

                let ancestors = if dead_end_enabled {
                    let mut terminal_ids = vec![tid];
                    terminal_ids.extend_from_slice(&cascade);
                    workers::cancel_dead_end_ancestors(&terminal_ids, conn).await?
                } else {
                    Vec::new()
                };

                // Enqueue on_failure outbox rows (task + cascade + ancestors) in-tx.
                workers::enqueue_end_outbox_with_cascade(
                    &tid,
                    crate::notification::TerminalStatus::FAILURE,
                    &cascade,
                    conn,
                )
                .await?;
                workers::enqueue_outbox_for_canceled_ancestors(&ancestors, conn).await?;

                // Batch-complete detection (D2): decrement `batch.remaining` for the
                // failed task + its cascade-failed children + dead-end-canceled ancestors.
                let mut terminal_ids = vec![tid];
                terminal_ids.extend_from_slice(&cascade);
                terminal_ids.extend(ancestors.iter().map(|a| a.id));
                decrement_batch_remaining_for_tasks(conn, &terminal_ids, "fail_task_and_propagate")
                    .await?;

                // Slot release (Audit 2, D1): the failed task was Running/Claimed (held
                // its slots); dead-end ancestors that were active held theirs. Cascade
                // children were Waiting/Paused ⇒ NULL keys ⇒ no-op.
                release_slots_for_tasks(conn, &terminal_ids).await?;
            }
            Ok(updated)
        })
    })
    .await?;

    if !updated {
        log::warn!(
            "fail_task_and_propagate: task {} not in Running/Claimed state; skipping failure propagation",
            task_id
        );
    }

    Ok(())
}

/// Result of stopping a batch. Contains counts per status category
/// and the list of Running task IDs that need cancel webhooks fired.
pub struct StopBatchResult {
    pub canceled_waiting: i64,
    pub canceled_pending: i64,
    pub canceled_claimed: i64,
    /// IDs of formerly-Running tasks (need cancel webhooks fired).
    pub canceled_running_ids: Vec<uuid::Uuid>,
    pub canceled_paused: i64,
    pub already_terminal: i64,
}

/// Stop all non-terminal tasks in a batch by setting them to Canceled.
/// Runs in a transaction for atomicity. Returns per-status counts and the
/// IDs of formerly-Running tasks (for cancel webhook firing).
#[tracing::instrument(name = "stop_batch", skip(conn), fields(batch_id = %batch_id))]
pub(crate) async fn stop_batch<'a>(
    conn: &mut Conn<'a>,
    batch_id: Uuid,
) -> Result<StopBatchResult, DbError> {
    use crate::schema::task::dsl;

    // Check that the batch exists before starting the transaction
    let total: i64 = dsl::task
        .filter(dsl::batch_id.eq(batch_id))
        .count()
        .get_result(conn)
        .await?;

    if total == 0 {
        return Err(crate::error::ArcRunError::NotFound {
            message: format!("No tasks found for batch {}", batch_id),
        });
    }

    // Macro to build the cancel changeset inline (Diesel types are not Copy)
    macro_rules! cancel_set {
        () => {
            (
                dsl::status.eq(StatusKind::Canceled),
                dsl::failure_reason.eq("Batch stopped"),
                dsl::ended_at.eq(diesel::dsl::now),
                dsl::last_updated.eq(diesel::dsl::now),
            )
        };
    }

    run_in_transaction(conn, |conn| {
        Box::pin(async move {
            // Count already-terminal tasks
            let already_terminal: i64 = dsl::task
                .filter(
                    dsl::batch_id.eq(batch_id).and(
                        dsl::status
                            .eq(StatusKind::Success)
                            .or(dsl::status.eq(StatusKind::Failure))
                            .or(dsl::status.eq(StatusKind::Canceled)),
                    ),
                )
                .count()
                .get_result(conn)
                .await?;

            // Cancel Waiting tasks
            let canceled_waiting = diesel::update(
                dsl::task.filter(
                    dsl::batch_id
                        .eq(batch_id)
                        .and(dsl::status.eq(StatusKind::Waiting)),
                ),
            )
            .set(cancel_set!())
            .execute(conn)
            .await? as i64;

            // Cancel Pending tasks
            let canceled_pending = diesel::update(
                dsl::task.filter(
                    dsl::batch_id
                        .eq(batch_id)
                        .and(dsl::status.eq(StatusKind::Pending)),
                ),
            )
            .set(cancel_set!())
            .execute(conn)
            .await? as i64;

            // Cancel Paused tasks
            let canceled_paused = diesel::update(
                dsl::task.filter(
                    dsl::batch_id
                        .eq(batch_id)
                        .and(dsl::status.eq(StatusKind::Paused)),
                ),
            )
            .set(cancel_set!())
            .execute(conn)
            .await? as i64;

            // Cancel Claimed tasks (return their IDs for cancel webhook firing).
            // A4 fix: `Claimed` is NOT "on_start not yet called" — it spans the entire
            // on_start webhook-in-flight window (permit queue + up to the webhook
            // timeout per action). A consumer that already received on_start and started
            // work must still get a cancel, so we enqueue a cancel outbox row for
            // formerly-Claimed tasks exactly like formerly-Running ones. Safe: the
            // delivery loop's start-before-end gate holds the cancel row while the
            // start row is pending+fresh, and a Claimed task that never returned a
            // cancel action prefetches zero actions ⇒ fast-path success (no HTTP).
            let canceled_claimed_ids: Vec<uuid::Uuid> = diesel::update(
                dsl::task.filter(
                    dsl::batch_id
                        .eq(batch_id)
                        .and(dsl::status.eq(StatusKind::Claimed)),
                ),
            )
            .set(cancel_set!())
            .returning(dsl::id)
            .get_results(conn)
            .await?;
            let canceled_claimed = canceled_claimed_ids.len() as i64;

            // Cancel Running tasks (return their IDs for cancel webhook firing)
            let canceled_running_ids: Vec<uuid::Uuid> = diesel::update(
                dsl::task.filter(
                    dsl::batch_id
                        .eq(batch_id)
                        .and(dsl::status.eq(StatusKind::Running)),
                ),
            )
            .set(cancel_set!())
            .returning(dsl::id)
            .get_results(conn)
            .await?;

            // Enqueue cancel outbox rows for formerly-Running AND formerly-Claimed tasks
            // INSIDE this transaction (at-least-once delivery via the delivery loop). No
            // reqwest in the call path. Safe per invariant #7: intra-batch links only,
            // both sides of every link are canceled here, so no per-task propagation.
            for rid in canceled_running_ids
                .iter()
                .chain(canceled_claimed_ids.iter())
            {
                crate::workers::enqueue_cancel_outbox(rid, conn).await?;
            }

            // Batch-complete detection (D2): stopping a batch cancels every remaining
            // task in one sweep, so `batch.remaining` goes straight to 0 (rather than
            // being decremented task-by-task). This fires the batch_complete webhook
            // if one was registered.
            zero_batch_remaining_and_complete(conn, batch_id, "stop_batch").await?;

            // Slot release (Audit 2, D1): only the formerly-Running and formerly-Claimed
            // tasks could have consumed concurrency slots (Waiting/Pending/Paused never
            // claimed). The `claimed_slot_keys IS NOT NULL` gate makes this exact.
            let mut released_ids = canceled_running_ids.clone();
            released_ids.extend_from_slice(&canceled_claimed_ids);
            release_slots_for_tasks(conn, &released_ids).await?;

            Ok(StopBatchResult {
                canceled_waiting,
                canceled_pending,
                canceled_claimed,
                canceled_running_ids,
                canceled_paused,
                already_terminal,
            })
        })
    })
    .await
}

/// Pause a task (A3). Only tasks that have NOT started executing may be paused:
/// `Pending` or `Waiting`. Pausing is a single atomic guarded UPDATE
/// (`WHERE status IN ('pending','waiting')`), so there is no check-then-act race.
///
/// A `Running`/`Claimed` task is already executing (its `on_start` webhook may have
/// fired) — pausing it would strand it (it escapes the timeout loop and the worker's
/// PATCH would 404), so we refuse and point the caller at cancel. A terminal task is
/// immutable. All refusals surface as a 400 with an explanatory message; a missing
/// task surfaces as a 404.
pub(crate) async fn pause_task<'a>(
    task_id: &uuid::Uuid,
    conn: &mut Conn<'a>,
) -> Result<(), DbError> {
    use crate::schema::task::dsl::{id, last_updated, status, task};
    let updated = diesel::update(
        task.filter(
            id.eq(task_id)
                .and(status.eq_any([StatusKind::Pending, StatusKind::Waiting])),
        ),
    )
    .set((
        status.eq(StatusKind::Paused),
        last_updated.eq(diesel::dsl::now),
    ))
    .execute(conn)
    .await?;
    if updated == 0 {
        return Err(refuse_transition(conn, task_id, "pause", "Pending or Waiting").await);
    }
    Ok(())
}

/// Resume a paused task (A3). Only a `Paused` task may be resumed. The target state is
/// derived atomically from the task's outstanding dependency counters in a single
/// guarded UPDATE (no SELECT-then-UPDATE race): if any `wait_*` counter is still
/// outstanding the task returns to `Waiting`, otherwise straight to `Pending`. This is
/// the ONLY path that moves a task out of `Paused` back into the schedulable flow
/// (propagation never auto-transitions a Paused task to Pending — see
/// `propagate_to_children`).
pub(crate) async fn resume_task<'a>(
    task_id: &uuid::Uuid,
    conn: &mut Conn<'a>,
) -> Result<(), DbError> {
    let updated = diesel::sql_query(
        "UPDATE task \
         SET status = CASE WHEN wait_finished > 0 OR wait_success > 0 \
                           THEN 'waiting'::status_kind \
                           ELSE 'pending'::status_kind END, \
             last_updated = now() \
         WHERE id = $1 AND status = 'paused'",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .execute(conn)
    .await?;
    if updated == 0 {
        return Err(refuse_transition(conn, task_id, "resume", "Paused").await);
    }
    Ok(())
}

/// Build the error for a refused pause/resume: 404 if the task is gone, otherwise a
/// 400 whose message names the current state and (for a running task being paused)
/// suggests cancel. The guarded UPDATE already committed nothing; this best-effort
/// SELECT only shapes the diagnostic and does not affect atomicity.
async fn refuse_transition<'a>(
    conn: &mut Conn<'a>,
    task_id: &uuid::Uuid,
    op: &str,
    allowed: &str,
) -> DbError {
    use crate::schema::task::dsl::{id, status, task};
    let current: Option<StatusKind> = match task
        .filter(id.eq(task_id))
        .select(status)
        .first::<StatusKind>(conn)
        .await
    {
        Ok(s) => Some(s),
        Err(diesel::result::Error::NotFound) => None,
        Err(e) => return DbError::from(e),
    };
    match current {
        None => crate::error::ArcRunError::NotFound {
            message: format!("Task {} not found", task_id),
        },
        Some(StatusKind::Running) if op == "pause" => crate::error::ArcRunError::InvalidState {
            message: "Cannot pause a Running task (its on_start webhook may have fired); \
                      cancel it instead (DELETE /task/{id})"
                .into(),
        },
        Some(s) => crate::error::ArcRunError::InvalidState {
            message: format!(
                "Cannot {} task in {:?} state (only {} tasks can be {}d)",
                op, s, allowed, op
            ),
        },
    }
}
