//! The dedicated webhook delivery QUEUE (`webhook_outbox`, Audit 2 D3).
//!
//! This is the at-least-once queue of end/cancel/batch_complete notifications: rows are
//! enqueued in the status-change transaction (outbox contract, Lot 2), claimed by lease,
//! and — the moment delivery terminates — DELETED here and historised into
//! `webhook_execution` as `success`/`exhausted`. Because a present row is by definition
//! awaiting delivery, this table has NO `status` column.
//!
//! `webhook_execution` (module [`super::webhook_execution`]) keeps two roles the queue
//! does not: the idempotency LEDGER of `start` deliveries (start_loop gate) and the
//! delivery LOG/history. The two tables are kept disjoint per idempotency_key by the
//! backstop `NOT EXISTS (ledger)` on enqueue and the DELETE-then-INSERT on terminal.

use crate::models::{WebhookExecution, WebhookExecutionStatus, WebhookOutbox};
use crate::{Conn, notification::TaskNotification};
use diesel_async::RunQueryDsl;

use super::DbError;

/// Enqueue a queue row for an end/cancel webhook event, inside the caller's transaction
/// (the same tx that changed the task status).
///
/// Two guards keep the exactly-once/at-least-once contract intact:
/// - `ON CONFLICT (idempotency_key) DO NOTHING` — a re-run of the same transition, or a
///   concurrent detection, never enqueues a duplicate while a queue row is live.
/// - **backstop `WHERE NOT EXISTS (webhook_execution ... key)`** — once the event has
///   been historised into the ledger in ANY state (success/exhausted), the queue row was
///   already delivered/given-up, so we must never re-enqueue it. With the pre-D3
///   single-table outbox this was implicit (the ON CONFLICT hit the retained success
///   row); now the queue is emptied on success, so the ledger NOT EXISTS is what blocks a
///   re-signal (e.g. a double `stop_batch` after the batch_complete already delivered).
///
/// The row matures immediately (`next_attempt_at` defaults to `now()`).
pub async fn enqueue_outbox<'a>(
    conn: &mut Conn<'a>,
    task_id: uuid::Uuid,
    event: TaskNotification,
) -> Result<(), DbError> {
    use crate::schema::sql_types as st;
    let key = event.idempotency_key(task_id);

    diesel::sql_query(
        "INSERT INTO webhook_outbox
            (task_id, trigger, condition, idempotency_key, attempts, next_attempt_at)
         SELECT $1, $2, $3, $4, 0, now()
         WHERE NOT EXISTS (
             SELECT 1 FROM webhook_execution WHERE idempotency_key = $4
         )
         ON CONFLICT (idempotency_key) DO NOTHING",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .bind::<st::TriggerKind, _>(event.trigger())
    .bind::<st::TriggerCondition, _>(event.condition())
    .bind::<diesel::sql_types::Text, _>(key)
    .execute(conn)
    .await?;

    Ok(())
}

/// Enqueue a `batch_complete` queue row for a batch whose last task just became terminal,
/// inside the caller's transaction. `task_id` is left NULL; `batch_id` is set; `condition`
/// is the `Success` sentinel (NOT NULL column, meaningless for batch-complete).
///
/// Same two guards as [`enqueue_outbox`]: `ON CONFLICT DO NOTHING` (concurrent
/// detection) + backstop `NOT EXISTS (ledger)` (already-delivered batch_complete). The
/// metric counts only a real insert (`rows > 0`), so a re-signal of an already-signalled
/// batch — blocked by either guard — is not double-counted.
pub async fn enqueue_batch_complete_outbox<'a>(
    conn: &mut Conn<'a>,
    batch_id: uuid::Uuid,
) -> Result<(), DbError> {
    let key = crate::action::batch_complete_idempotency_key(batch_id);
    let rows_inserted = diesel::sql_query(
        "INSERT INTO webhook_outbox
            (batch_id, trigger, condition, idempotency_key, attempts, next_attempt_at)
         SELECT $1, 'batch_complete', 'success', $2, 0, now()
         WHERE NOT EXISTS (
             SELECT 1 FROM webhook_execution WHERE idempotency_key = $2
         )
         ON CONFLICT (idempotency_key) DO NOTHING",
    )
    .bind::<diesel::sql_types::Uuid, _>(batch_id)
    .bind::<diesel::sql_types::Text, _>(key)
    .execute(conn)
    .await?;

    if rows_inserted > 0 {
        crate::metrics::record_batch_completed();
    }

    Ok(())
}

#[derive(diesel::QueryableByName, Debug)]
pub struct OutboxBacklogStats {
    /// Queue rows whose `next_attempt_at <= now()` (mature, awaiting delivery).
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    pub ready: i64,
    /// Queue rows whose `next_attempt_at > now()` (leased in-flight or backing off).
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    pub leased: i64,
    /// Age in seconds of the oldest mature queue row (0.0 if none).
    #[diesel(sql_type = diesel::sql_types::Double)]
    pub oldest_ready_age_secs: f64,
}

/// Snapshot the outbox backlog for observability: how many queue rows are mature
/// (`ready`) vs not-yet-due (`leased`), and the age of the oldest mature row. A single
/// indexed scan over `webhook_outbox` (served by `idx_webhook_outbox_next_attempt_at`),
/// run once per delivery-loop iteration — never in an HTTP path.
pub async fn outbox_backlog_stats<'a>(conn: &mut Conn<'a>) -> Result<OutboxBacklogStats, DbError> {
    let stats: OutboxBacklogStats = diesel::sql_query(
        "SELECT
            COUNT(*) FILTER (WHERE next_attempt_at <= now()) AS ready,
            COUNT(*) FILTER (WHERE next_attempt_at >  now()) AS leased,
            COALESCE(
                EXTRACT(EPOCH FROM (now() - MIN(created_at) FILTER (WHERE next_attempt_at <= now()))),
                0
            )::double precision AS oldest_ready_age_secs
         FROM webhook_outbox",
    )
    .get_result(conn)
    .await?;
    Ok(stats)
}

/// Select up to `limit` mature queue rows for delivery, locking them with
/// `FOR UPDATE SKIP LOCKED` so concurrent delivery workers don't double-deliver.
///
/// Every row in `webhook_outbox` is awaiting delivery, so the only maturity predicate is
/// `next_attempt_at <= now()` (no status / trigger filter — those roles moved out to the
/// ledger). Per-task ordering (start-before-end) is enforced against the LEDGER: an
/// end/cancel row is held back while the task's `start` row is still `pending` in
/// `webhook_execution` AND fresh (`updated_at > now() - start_stale_secs`, Audit 2 A2).
///
/// NOTE: this non-leased variant is unused by the delivery loop (which uses
/// [`claim_due_outbox_leased`]); it is kept as a mirror so the two gates never diverge.
///
/// Rows are returned oldest-first (`created_at ASC`) for fair, FIFO-ish delivery.
pub async fn claim_due_outbox<'a>(
    conn: &mut Conn<'a>,
    limit: i64,
    start_stale_secs: i64,
) -> Result<Vec<WebhookOutbox>, DbError> {
    let rows = diesel::sql_query(
        "SELECT we.* FROM webhook_outbox we
         WHERE we.next_attempt_at <= now()
           AND NOT EXISTS (
                 SELECT 1 FROM webhook_execution s
                 WHERE s.task_id = we.task_id
                   AND s.trigger = 'start'
                   AND s.status = 'pending'
                   AND s.updated_at > now() - ($2::bigint * interval '1 second')
           )
         ORDER BY we.next_attempt_at ASC, we.created_at ASC, we.id ASC
         LIMIT $1
         FOR UPDATE OF we SKIP LOCKED",
    )
    .bind::<diesel::sql_types::BigInt, _>(limit)
    .bind::<diesel::sql_types::BigInt, _>(start_stale_secs)
    .load::<WebhookOutbox>(conn)
    .await?;

    Ok(rows)
}

/// Claim up to `limit` mature queue rows for delivery AND push their `next_attempt_at`
/// `lease_secs` into the future, in one statement.
///
/// Lease-based variant of [`claim_due_outbox`]: instead of holding the `FOR UPDATE`
/// locks across the whole (HTTP-bearing) delivery, it commits a short claim that sets a
/// lease on each row. While the lease holds the row is not mature, so a concurrent worker
/// / next iteration won't re-claim it; on crash mid-delivery the lease expires and the row
/// matures again (at-least-once). The lease does NOT bump `attempts`.
///
/// Same selection predicates as [`claim_due_outbox`]: `next_attempt_at <= now()`, the
/// per-task start-before-end gate (bounded by `start_stale_secs`, Audit 2 A2),
/// `ORDER BY created_at ASC`, `LIMIT`, `FOR UPDATE SKIP LOCKED`.
///
/// `lease_secs` must be >= 1 and should exceed the worst-case single-row delivery time.
pub async fn claim_due_outbox_leased<'a>(
    conn: &mut Conn<'a>,
    limit: i64,
    lease_secs: i64,
    start_stale_secs: i64,
) -> Result<Vec<WebhookOutbox>, DbError> {
    let rows = diesel::sql_query(
        "UPDATE webhook_outbox we
         SET next_attempt_at = now() + ($2::bigint * interval '1 second'),
             lease_token = gen_random_uuid(),
             updated_at = now()
         FROM (
             SELECT c.id FROM webhook_outbox c
             WHERE c.next_attempt_at <= now()
               AND NOT EXISTS (
                     SELECT 1 FROM webhook_execution s
                     WHERE s.task_id = c.task_id
                       AND s.trigger = 'start'
                       AND s.status = 'pending'
                       AND s.updated_at > now() - ($3::bigint * interval '1 second')
               )
             ORDER BY c.next_attempt_at ASC, c.created_at ASC, c.id ASC
             LIMIT $1
             FOR UPDATE SKIP LOCKED
         ) AS claimed
         WHERE we.id = claimed.id
         RETURNING we.*",
    )
    .bind::<diesel::sql_types::BigInt, _>(limit)
    .bind::<diesel::sql_types::BigInt, _>(lease_secs)
    .bind::<diesel::sql_types::BigInt, _>(start_stale_secs)
    .load::<WebhookOutbox>(conn)
    .await?;

    Ok(rows)
}

/// Mark a queue row delivered: DELETE it from `webhook_outbox` and, atomically in the
/// same statement, INSERT a `success` history row into `webhook_execution` (carrying the
/// original id/timestamps/attempts, `updated_at = now()`, no error). A single CTE, so the
/// row can never be lost between the two tables. `ON CONFLICT DO NOTHING` guards the rare
/// case where a history row for the key already exists.
pub async fn mark_outbox_success<'a>(
    conn: &mut Conn<'a>,
    key: &str,
    lease_token: uuid::Uuid,
) -> Result<bool, DbError> {
    #[derive(diesel::QueryableByName)]
    struct Marked {
        #[diesel(sql_type = diesel::sql_types::Bool)]
        marked: bool,
    }
    let result: Marked = diesel::sql_query(
        "WITH del AS (
            DELETE FROM webhook_outbox
            WHERE idempotency_key = $1 AND lease_token = $2
            RETURNING *
        ), ins AS (
          INSERT INTO webhook_execution
            (id, task_id, batch_id, trigger, condition, idempotency_key,
             status, attempts, created_at, updated_at, next_attempt_at, last_error)
          SELECT id, task_id, batch_id, trigger, condition, idempotency_key,
                 'success', attempts, created_at, now(), next_attempt_at, NULL
          FROM del
          ON CONFLICT (idempotency_key) DO NOTHING
        )
        SELECT EXISTS(SELECT 1 FROM del) AS marked",
    )
    .bind::<diesel::sql_types::Text, _>(key)
    .bind::<diesel::sql_types::Uuid, _>(lease_token)
    .get_result(conn)
    .await?;
    Ok(result.marked)
}

/// Record a failed delivery attempt: increment `attempts`, store `last_error`, and
/// schedule the next attempt `backoff_secs` in the future. The row STAYS in the queue.
pub async fn mark_outbox_retry<'a>(
    conn: &mut Conn<'a>,
    key: &str,
    lease_token: uuid::Uuid,
    error: &str,
    backoff_secs: i64,
) -> Result<bool, DbError> {
    // Truncate overly-long error bodies to keep the row bounded.
    let trimmed: String = error.chars().take(1000).collect();
    let rows = diesel::sql_query(
        "UPDATE webhook_outbox
         SET attempts = attempts + 1,
             last_error = $2,
             next_attempt_at = now() + ($3::bigint * interval '1 second'),
             updated_at = now(),
             lease_token = NULL
         WHERE idempotency_key = $1 AND lease_token = $4",
    )
    .bind::<diesel::sql_types::Text, _>(key)
    .bind::<diesel::sql_types::Text, _>(trimmed)
    .bind::<diesel::sql_types::BigInt, _>(backoff_secs)
    .bind::<diesel::sql_types::Uuid, _>(lease_token)
    .execute(conn)
    .await?;
    Ok(rows > 0)
}

/// Mark a queue row permanently failed after exhausting retries: DELETE it from
/// `webhook_outbox` and INSERT an `exhausted` history row into `webhook_execution`
/// (`attempts + 1`, `last_error` truncated to 1000 chars) — one atomic CTE, mirror of
/// [`mark_outbox_success`].
pub async fn mark_outbox_exhausted<'a>(
    conn: &mut Conn<'a>,
    key: &str,
    lease_token: uuid::Uuid,
    error: &str,
) -> Result<bool, DbError> {
    let trimmed: String = error.chars().take(1000).collect();
    #[derive(diesel::QueryableByName)]
    struct Marked {
        #[diesel(sql_type = diesel::sql_types::Bool)]
        marked: bool,
    }
    let result: Marked = diesel::sql_query(
        "WITH del AS (
            DELETE FROM webhook_outbox
            WHERE idempotency_key = $1 AND lease_token = $3
            RETURNING *
        ), ins AS (
          INSERT INTO webhook_execution
            (id, task_id, batch_id, trigger, condition, idempotency_key,
             status, attempts, created_at, updated_at, next_attempt_at, last_error)
          SELECT id, task_id, batch_id, trigger, condition, idempotency_key,
                 'exhausted', attempts + 1, created_at, now(), next_attempt_at, $2
          FROM del
          ON CONFLICT (idempotency_key) DO NOTHING
        )
        SELECT EXISTS(SELECT 1 FROM del) AS marked",
    )
    .bind::<diesel::sql_types::Text, _>(key)
    .bind::<diesel::sql_types::Text, _>(trimmed)
    .bind::<diesel::sql_types::Uuid, _>(lease_token)
    .get_result(conn)
    .await?;
    Ok(result.marked)
}

/// List webhook delivery records for `GET /webhook-deliveries`, optionally filtered by
/// status, most-recently-updated first. Response contract is unchanged: `pending` rows
/// come from the QUEUE (`webhook_outbox`, projected with a synthetic `pending` status)
/// PLUS any pending `start` rows still in the ledger; `success`/`failure`/`exhausted`
/// come from the ledger/history (`webhook_execution`).
///
/// Implementation: for the pending / unfiltered views, a `UNION ALL` of both tables
/// projected onto the `webhook_execution` shape; for a non-pending status filter, only
/// the ledger is queried (the queue has no such rows).
pub async fn list_webhook_deliveries<'a>(
    conn: &mut Conn<'a>,
    status: Option<WebhookExecutionStatus>,
    limit: i64,
    offset: i64,
) -> Result<Vec<WebhookExecution>, DbError> {
    // Ledger projection (all webhook_execution columns, in the struct's column order).
    const LEDGER_COLS: &str = "id, task_id, trigger, condition, idempotency_key, \
         status, attempts, created_at, updated_at, next_attempt_at, last_error, batch_id";
    // Queue projection onto the same shape, with a synthetic `pending` status.
    const QUEUE_PROJ: &str = "SELECT id, task_id, trigger, condition, idempotency_key, \
         'pending'::webhook_execution_status AS status, attempts, created_at, updated_at, \
         next_attempt_at, last_error, batch_id FROM webhook_outbox";

    // A non-pending status filter can never match a queue row, so query the ledger only.
    // The literal comes from a Rust enum (never user text), so inlining it is safe.
    let sql = match status {
        Some(WebhookExecutionStatus::Success)
        | Some(WebhookExecutionStatus::Failure)
        | Some(WebhookExecutionStatus::Exhausted) => {
            let lit = match status.unwrap() {
                WebhookExecutionStatus::Success => "success",
                WebhookExecutionStatus::Failure => "failure",
                WebhookExecutionStatus::Exhausted => "exhausted",
                WebhookExecutionStatus::Pending => unreachable!(),
            };
            format!(
                "SELECT {cols} FROM webhook_execution WHERE status = '{lit}' \
                 ORDER BY updated_at DESC, id DESC LIMIT $1 OFFSET $2",
                cols = LEDGER_COLS,
            )
        }
        // Pending: queue rows + the ledger's pending `start` rows.
        Some(WebhookExecutionStatus::Pending) => format!(
            "SELECT * FROM (
                 SELECT {cols} FROM webhook_execution WHERE status = 'pending'
                 UNION ALL
                 {queue}
             ) u ORDER BY updated_at DESC, id DESC LIMIT $1 OFFSET $2",
            cols = LEDGER_COLS,
            queue = QUEUE_PROJ,
        ),
        // Unfiltered: every ledger row + the whole queue.
        None => format!(
            "SELECT * FROM (
                 SELECT {cols} FROM webhook_execution
                 UNION ALL
                 {queue}
             ) u ORDER BY updated_at DESC, id DESC LIMIT $1 OFFSET $2",
            cols = LEDGER_COLS,
            queue = QUEUE_PROJ,
        ),
    };

    let rows = diesel::sql_query(sql)
        .bind::<diesel::sql_types::BigInt, _>(limit)
        .bind::<diesel::sql_types::BigInt, _>(offset)
        .load::<WebhookExecution>(conn)
        .await?;
    Ok(rows)
}
