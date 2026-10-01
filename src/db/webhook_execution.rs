use crate::Conn;
use crate::models::{TriggerCondition, TriggerKind, WebhookExecutionStatus};
use diesel::ExpressionMethods;
use diesel::OptionalExtension;
use diesel::QueryDsl;
use diesel_async::RunQueryDsl;

use super::DbError;

/// Attempt to claim a webhook execution slot for idempotency.
///
/// Uses INSERT ... ON CONFLICT to atomically claim or skip:
/// - If no row exists: inserts a new `pending` row → returns `Ok(true)` (proceed)
/// - If a row exists with `status = 'success'`: no update → returns `Ok(false)` (skip)
/// - If a row exists with `status = 'failure'`: resets to pending → returns `Ok(true)` (retry)
/// - If a row exists with `status = 'pending'`: retries only when `stale_after` elapsed
/// Note: for Start/Cancel triggers, `condition` is stored as `Success` sentinel.
pub async fn try_claim_webhook_execution<'a>(
    conn: &mut Conn<'a>,
    task_id: uuid::Uuid,
    trigger_kind: TriggerKind,
    trigger_condition: TriggerCondition,
    key: &str,
    stale_after: Option<std::time::Duration>,
) -> Result<bool, DbError> {
    use crate::schema::sql_types as st;

    let stale_after_micros = stale_after.map(|d| i64::try_from(d.as_micros()).unwrap_or(i64::MAX));

    // INSERT ... ON CONFLICT with a WHERE filter on the DO UPDATE.
    // - If status = 'success' → no update (0 rows affected)
    // - If status = 'failure' → update, reset to pending (retry allowed)
    // - If status = 'pending' → update only if stale_after is provided and elapsed
    let result: usize = diesel::sql_query(
        "INSERT INTO webhook_execution (task_id, trigger, condition, idempotency_key, status, attempts)
         VALUES ($1, $2, $3, $4, 'pending', 1)
         ON CONFLICT (idempotency_key) DO UPDATE
         SET attempts = webhook_execution.attempts + 1,
             status = 'pending',
             updated_at = now()
         WHERE webhook_execution.status = 'failure'
            OR (
                webhook_execution.status = 'pending'
                AND $5 IS NOT NULL
                AND webhook_execution.updated_at < (now() - ($5::bigint * interval '1 microsecond'))
            )"
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .bind::<st::TriggerKind, _>(trigger_kind)
    .bind::<st::TriggerCondition, _>(trigger_condition)
    .bind::<diesel::sql_types::Text, _>(key)
    .bind::<diesel::sql_types::Nullable<diesel::sql_types::BigInt>, _>(stale_after_micros)
    .execute(conn)
    .await?;

    Ok(result > 0)
}

/// Mark a webhook execution as success or failure after execution completes.
pub async fn complete_webhook_execution<'a>(
    conn: &mut Conn<'a>,
    key: &str,
    succeeded: bool,
) -> Result<(), DbError> {
    use crate::schema::webhook_execution::dsl;

    let new_status = if succeeded {
        WebhookExecutionStatus::Success
    } else {
        WebhookExecutionStatus::Failure
    };

    diesel::update(dsl::webhook_execution.filter(dsl::idempotency_key.eq(key)))
        .set((
            dsl::status.eq(new_status),
            dsl::updated_at.eq(diesel::dsl::now),
        ))
        .execute(conn)
        .await?;

    Ok(())
}

// =============================================================================
// Batch-complete detection (Audit 2, D2) — enqueue lives in `super::webhook_outbox`
// =============================================================================

/// Decrement `batch.remaining` (Audit 2, D2) by the number of just-terminalized
/// tasks in `terminal_task_ids`, grouped by their batch, and — for any batch whose
/// counter reaches 0 with a non-empty `on_complete` (#601 gate) — enqueue exactly one
/// `batch_complete` outbox row.
///
/// This REPLACES the old `FOR UPDATE` + `NOT EXISTS (task active)` probe. The
/// decrement is a single `UPDATE batch SET remaining = GREATEST(remaining - N, 0) …
/// RETURNING remaining` per batch: atomic, O(1), and naturally serialized on the
/// `batch` row (two transactions finishing the batch's last two tasks each apply
/// their own `-1`, so exactly one of them observes `remaining = 0`). `remaining = 0`
/// IS the completion signal.
///
/// **Exactly-once contract.** Pass ONLY ids of tasks that ACTUALLY transitioned to a
/// terminal state in the current transaction (the guarded UPDATE / cascade RETURNING
/// matched them). A re-PATCH/cancel/timeout of an already-terminal task does not
/// transition it, so its id never reaches here and the counter never double-decrements.
/// Duplicate ids in the slice are harmless — the COUNT is over distinct matching
/// `task` rows, so each task is counted at most once.
///
/// The decrement itself is UNCONDITIONAL for every batched task (it also drives free
/// progress reporting for scope/metadata-only batches, whose `on_complete = '[]'`);
/// only the `batch_complete` ENQUEUE is gated on a non-empty `on_complete`. A task
/// with no `batch` row (its `batch_id` has no row, or is NULL) matches no batch, so
/// the UPDATE is a near-free no-op and nothing is enqueued.
///
/// Belt-and-braces: the unique idempotency key + `ON CONFLICT DO NOTHING` in
/// [`enqueue_batch_complete_outbox`] make a re-signal of an already-signalled batch
/// inoffensive. Must run inside the same transaction as the terminal transition(s)
/// (outbox contract, Lot 2). `caller` is a short label for logs.
pub async fn decrement_batch_remaining_for_tasks<'a>(
    conn: &mut Conn<'a>,
    terminal_task_ids: &[uuid::Uuid],
    caller: &str,
) -> Result<(), DbError> {
    if terminal_task_ids.is_empty() {
        return Ok(());
    }

    #[derive(diesel::QueryableByName)]
    struct CompletedBatch {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: uuid::Uuid,
        #[diesel(sql_type = diesel::sql_types::Integer)]
        remaining: i32,
        #[diesel(sql_type = diesel::sql_types::Bool)]
        has_webhook: bool,
    }

    // One statement decrements every affected batch by its count of newly-terminal
    // tasks and RETURNs the post-update `remaining` + whether the batch registered a
    // webhook. `COUNT(*)::int` keeps arithmetic in int4 (matches the column type).
    let updated: Vec<CompletedBatch> = diesel::sql_query(
        "UPDATE batch b
         SET remaining = GREATEST(b.remaining - sub.cnt, 0)
         FROM (
             SELECT batch_id, COUNT(*)::int AS cnt
             FROM task
             WHERE id = ANY($1) AND batch_id IS NOT NULL
             GROUP BY batch_id
         ) sub
         WHERE b.id = sub.batch_id
         RETURNING b.id, b.remaining, (b.on_complete <> '[]'::jsonb) AS has_webhook",
    )
    .bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(terminal_task_ids)
    .load::<CompletedBatch>(conn)
    .await?;

    for cb in updated {
        if cb.remaining == 0 && cb.has_webhook {
            super::webhook_outbox::enqueue_batch_complete_outbox(conn, cb.id).await?;
            log::debug!(
                "[{}] batch {} reached remaining=0 — enqueued batch_complete outbox row",
                caller,
                cb.id
            );
        }
    }

    Ok(())
}

/// Convenience wrapper for the single-task terminal sites: decrement `batch.remaining`
/// for exactly one just-terminalized task. See [`decrement_batch_remaining_for_tasks`].
pub async fn decrement_batch_remaining_for_task<'a>(
    conn: &mut Conn<'a>,
    task_id: uuid::Uuid,
    caller: &str,
) -> Result<(), DbError> {
    decrement_batch_remaining_for_tasks(conn, std::slice::from_ref(&task_id), caller).await
}

/// Force `batch.remaining` to 0 for a batch (used by `stop_batch`, which cancels every
/// remaining task in one sweep) and, if the batch registered a webhook (#601 gate),
/// enqueue the `batch_complete` signal. No-op if the batch has no `batch` row.
pub async fn zero_batch_remaining_and_complete<'a>(
    conn: &mut Conn<'a>,
    batch_id: uuid::Uuid,
    caller: &str,
) -> Result<(), DbError> {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Bool)]
        has_webhook: bool,
    }

    let row: Option<Row> = diesel::sql_query(
        "UPDATE batch SET remaining = 0 WHERE id = $1
         RETURNING (on_complete <> '[]'::jsonb) AS has_webhook",
    )
    .bind::<diesel::sql_types::Uuid, _>(batch_id)
    .get_result::<Row>(conn)
    .await
    .optional()?;

    if let Some(r) = row
        && r.has_webhook
    {
        super::webhook_outbox::enqueue_batch_complete_outbox(conn, batch_id).await?;
        log::debug!(
            "[{}] batch {} stopped (remaining set to 0) — enqueued batch_complete outbox row",
            caller,
            batch_id
        );
    }

    Ok(())
}

/// Initialize a freshly-created batch's `remaining` to `count` — the number of tasks
/// ACTUALLY inserted (dedupe-skips excluded, known at insert time). When `count == 0`
/// (empty / all-dedupe-skipped batch) AND the batch registered a webhook (non-empty
/// `on_complete`, #601 gate), the batch is vacuously complete, so the `batch_complete`
/// signal is enqueued immediately — preserving the pre-D2 vacuous-empty behaviour.
/// No-op if the batch has no `batch` row (the UPDATE matches nothing).
pub async fn init_batch_remaining<'a>(
    conn: &mut Conn<'a>,
    batch_id: uuid::Uuid,
    count: i32,
    caller: &str,
) -> Result<(), DbError> {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Bool)]
        fire: bool,
    }

    let row: Option<Row> = diesel::sql_query(
        "UPDATE batch SET remaining = $2 WHERE id = $1
         RETURNING (remaining = 0 AND on_complete <> '[]'::jsonb) AS fire",
    )
    .bind::<diesel::sql_types::Uuid, _>(batch_id)
    .bind::<diesel::sql_types::Integer, _>(count)
    .get_result::<Row>(conn)
    .await
    .optional()?;

    if let Some(r) = row
        && r.fire
    {
        super::webhook_outbox::enqueue_batch_complete_outbox(conn, batch_id).await?;
        log::debug!(
            "[{}] batch {} vacuously complete (0 tasks inserted) — enqueued batch_complete outbox row",
            caller,
            batch_id
        );
    }

    Ok(())
}

/// Insert the `batch` row, inside the caller's transaction (the same tx as
/// `add_task`). `on_complete` is the JSON array of `NewActionDto` (empty `[]` for a
/// scope/metadata-only batch); `scope`/`metadata` are the batch's business-level
/// identity used for filtering and search.
pub async fn insert_batch<'a>(
    conn: &mut Conn<'a>,
    batch_id: uuid::Uuid,
    on_complete: serde_json::Value,
    scope: Option<String>,
    metadata: serde_json::Value,
) -> Result<(), DbError> {
    use crate::schema::batch::dsl;
    diesel::insert_into(dsl::batch)
        .values(crate::models::NewBatch {
            id: batch_id,
            on_complete,
            scope,
            metadata,
        })
        .execute(conn)
        .await?;
    Ok(())
}

/// Load a batch's `on_complete` payload, if the batch row exists.
pub async fn load_batch_on_complete<'a>(
    conn: &mut Conn<'a>,
    batch_id: uuid::Uuid,
) -> Result<Option<serde_json::Value>, DbError> {
    use crate::schema::batch::dsl;
    let row: Option<serde_json::Value> = dsl::batch
        .filter(dsl::id.eq(batch_id))
        .select(dsl::on_complete)
        .first::<serde_json::Value>(conn)
        .await
        .optional()?;
    Ok(row)
}

/// Per-status terminal counts for a batch, plus the latest `ended_at`, used to build
/// the `arcrun` enrichment of a batch_complete webhook payload at delivery time.
#[derive(diesel::QueryableByName, Debug)]
pub struct BatchCompletionStats {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    pub success: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    pub failure: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    pub canceled: i64,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>)]
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Compute per-status terminal counts + max(ended_at) for a batch (at delivery time).
pub async fn batch_completion_stats<'a>(
    conn: &mut Conn<'a>,
    batch_id: uuid::Uuid,
) -> Result<BatchCompletionStats, DbError> {
    let stats: BatchCompletionStats = diesel::sql_query(
        "SELECT
            COUNT(*) FILTER (WHERE status = 'success')  AS success,
            COUNT(*) FILTER (WHERE status = 'failure')  AS failure,
            COUNT(*) FILTER (WHERE status = 'canceled') AS canceled,
            MAX(ended_at) AS completed_at
         FROM task WHERE batch_id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(batch_id)
    .get_result(conn)
    .await?;
    Ok(stats)
}
