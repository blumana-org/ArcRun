use crate::{
    Conn, dtos,
    models::{self, Link, Task},
};
use diesel::prelude::*;
use diesel::sql_types;
use diesel_async::RunQueryDsl;
use uuid::Uuid;

use super::DbError;

/// One `(label, count)` row of an aggregation query (status or kind).
#[derive(diesel::QueryableByName)]
struct LabelCount {
    #[diesel(sql_type = sql_types::Text)]
    label: String,
    #[diesel(sql_type = sql_types::BigInt)]
    count: i64,
}

/// Count tasks grouped by status, for the `tasks_by_status` gauge. One indexed
/// `GROUP BY` run by the metrics sampler — never in an HTTP path.
pub(crate) async fn count_tasks_by_status<'a>(
    conn: &mut Conn<'a>,
) -> Result<Vec<(String, i64)>, DbError> {
    let rows: Vec<LabelCount> = diesel::sql_query(
        "SELECT status::text AS label, COUNT(*) AS count FROM task GROUP BY status",
    )
    .get_results(conn)
    .await?;
    Ok(rows.into_iter().map(|r| (r.label, r.count)).collect())
}

/// Count Running tasks grouped by kind, for the `running_tasks_by_kind` gauge.
pub(crate) async fn count_running_tasks_by_kind<'a>(
    conn: &mut Conn<'a>,
) -> Result<Vec<(String, i64)>, DbError> {
    let rows: Vec<LabelCount> = diesel::sql_query(
        "SELECT kind AS label, COUNT(*) AS count FROM task WHERE status = 'running' GROUP BY kind",
    )
    .get_results(conn)
    .await?;
    Ok(rows.into_iter().map(|r| (r.label, r.count)).collect())
}

/// Find up to `limit` Running tasks that have exceeded their timeout (based on
/// `last_updated`). Returns matching task IDs without modifying them — callers
/// should use `timeout_task_and_propagate` to atomically mark each as failed and
/// propagate.
///
/// Bounded by `limit` (Audit 2, B7): a mass-timeout of thousands of tasks would
/// otherwise pin the timeout loop for a long time in one pass, delaying the stale
/// Claimed requeue that shares the loop. The caller drains in bounded passes.
/// Ordered oldest-first (`last_updated ASC`) so the most overdue tasks are handled
/// first and each drain pass makes deterministic progress.
pub(crate) async fn find_timed_out_tasks<'a>(
    conn: &mut Conn<'a>,
    limit: i64,
) -> Result<Vec<uuid::Uuid>, DbError> {
    use {
        crate::schema::task::dsl::*,
        diesel::{dsl::now, pg::data_types::PgInterval},
    };
    let ids = task
        .filter(
            status
                .eq(models::StatusKind::Running)
                .and(started_at.is_not_null())
                .and(last_updated.lt(now.into_sql::<sql_types::Timestamptz>()
                    - (PgInterval::from_microseconds(1_000_000).into_sql::<sql_types::Interval>()
                        * timeout))),
        )
        .order(last_updated.asc())
        .limit(limit)
        .select(id)
        .get_results::<uuid::Uuid>(conn)
        .await?;
    Ok(ids)
}

/// Atomically mark a single task as timed-out (Failed) and propagate to children,
/// all within one transaction. Returns the failed task, cascade-failed child IDs,
/// and any ancestors canceled by dead-end detection.
/// Uses FOR UPDATE SKIP LOCKED to avoid conflicts with concurrent workers.
pub(crate) async fn timeout_task_and_propagate<'a>(
    conn: &mut Conn<'a>,
    task_id: uuid::Uuid,
    dead_end_enabled: bool,
) -> Result<
    Option<(
        Task,
        Vec<uuid::Uuid>,
        Vec<crate::workers::propagation::CanceledAncestor>,
    )>,
    DbError,
> {
    use super::run_in_transaction;
    use {crate::schema::task::dsl::*, diesel::dsl::now};
    const TIMEOUT_REASON: &str = "Timeout";

    run_in_transaction(conn, |conn| {
        Box::pin(async move {
            // Lock the task; SKIP LOCKED so concurrent timeout workers don't block
            let t: Option<Task> = task
                .filter(id.eq(task_id).and(status.eq(models::StatusKind::Running)))
                .for_update()
                .skip_locked()
                .first::<Task>(conn)
                .await
                .optional()?;

            let Some(t) = t else {
                // Already transitioned (e.g. completed or cancelled concurrently)
                return Ok(None);
            };

            // Mark as failed (last_updated intentionally NOT updated —
            // it preserves when the task last showed activity, useful for diagnostics)
            diesel::update(task.filter(id.eq(task_id)))
                .set((
                    status.eq(models::StatusKind::Failure),
                    ended_at.eq(now),
                    failure_reason.eq(TIMEOUT_REASON),
                ))
                .execute(conn)
                .await?;

            // Propagate failure to children (inside tx)
            let cascade_failed =
                crate::workers::propagate_to_children(&task_id, &models::StatusKind::Failure, conn)
                    .await?;

            // Dead-end ancestor cancellation (inside tx)
            let canceled_ancestors = if dead_end_enabled {
                let mut terminal_ids = vec![task_id];
                terminal_ids.extend_from_slice(&cascade_failed);
                crate::workers::cancel_dead_end_ancestors(&terminal_ids, conn).await?
            } else {
                vec![]
            };

            // Enqueue on_failure outbox rows (task + cascade + ancestors) in-tx.
            crate::workers::enqueue_end_outbox_with_cascade(
                &task_id,
                crate::notification::TerminalStatus::FAILURE,
                &cascade_failed,
                conn,
            )
            .await?;
            crate::workers::enqueue_outbox_for_canceled_ancestors(&canceled_ancestors, conn)
                .await?;

            // Batch-complete detection (D2): decrement `batch.remaining` for the
            // timed-out task + its cascade-failed children + dead-end-canceled ancestors.
            let mut terminal_ids = vec![task_id];
            terminal_ids.extend_from_slice(&cascade_failed);
            terminal_ids.extend(canceled_ancestors.iter().map(|a| a.id));
            crate::db_operation::decrement_batch_remaining_for_tasks(
                conn,
                &terminal_ids,
                "timeout_task_and_propagate",
            )
            .await?;

            // Slot release (Audit 2, D1): the timed-out task was Running (held its
            // slots); dead-end ancestors that were active held theirs. Cascade children
            // were Waiting/Paused ⇒ NULL keys ⇒ no-op.
            crate::db_operation::release_slots_for_tasks(conn, &terminal_ids).await?;

            Ok(Some((t, cascade_failed, canceled_ancestors)))
        })
    })
    .await
}

/// Requeue Claimed tasks that never started within the claim timeout.
/// Returns the ids of the tasks moved back to Pending.
///
/// Slot release (Audit 2, D1): a stale Claimed task holds its concurrency slots (it
/// consumed them at claim time). Requeuing it to Pending MUST release those slots and
/// NULL its `claimed_slot_keys` — otherwise the slot leaks (the task re-claims later
/// and re-increments). This is the 7th release site.
///
/// All three steps run in ONE transaction so a crash cannot leave a task Pending with
/// its slot still held: (1) lock + capture the stale Claimed rows (`FOR UPDATE SKIP
/// LOCKED`), (2) flip them to Pending, (3) `release_slots_for_tasks` — which reads their
/// still-set `claimed_slot_keys`, decrements `rule_slot`, and NULLs the keys. Capturing
/// the ids BEFORE the release is why we can't do it all in one `UPDATE ... RETURNING`
/// (that would RETURN the just-NULLed keys).
pub(crate) async fn requeue_stale_claimed_tasks<'a>(
    conn: &mut Conn<'a>,
    claim_timeout: std::time::Duration,
) -> Result<Vec<uuid::Uuid>, DbError> {
    use super::run_in_transaction;
    let micros = i64::try_from(claim_timeout.as_micros()).unwrap_or(i64::MAX);

    #[derive(diesel::QueryableByName)]
    struct StaleId {
        #[diesel(sql_type = sql_types::Uuid)]
        id: uuid::Uuid,
    }

    run_in_transaction(conn, |conn| {
        Box::pin(async move {
            use crate::schema::task::dsl::*;
            use diesel::dsl::now;

            // 1. Lock + capture stale Claimed ids (SKIP LOCKED so we never block on a
            //    row another worker is mid-transitioning). Interval built from micros.
            let stale: Vec<StaleId> = diesel::sql_query(
                "SELECT id FROM task
                 WHERE status = 'claimed'
                   AND last_updated < now() - make_interval(secs => $1::double precision / 1000000.0)
                 FOR UPDATE SKIP LOCKED",
            )
            .bind::<sql_types::BigInt, _>(micros)
            .get_results::<StaleId>(&mut *conn)
            .await?;

            if stale.is_empty() {
                return Ok(Vec::new());
            }
            let ids: Vec<uuid::Uuid> = stale.into_iter().map(|s| s.id).collect();

            // 2. Flip to Pending (leaves claimed_slot_keys set for the release read).
            diesel::update(task.filter(id.eq_any(&ids)))
                .set((status.eq(models::StatusKind::Pending), last_updated.eq(now)))
                .execute(&mut *conn)
                .await?;

            // 3. Release their slots (reads the still-set keys, decrements, NULLs them).
            crate::db_operation::release_slots_for_tasks(&mut *conn, &ids).await?;

            Ok(ids)
        })
    })
    .await
}

/// Keyset cursor for paginating Pending tasks in the start_loop claim phase.
/// Captures the ordering tuple `(priority, created_at, id)` of the last row of
/// the previous page. The ordering is `priority DESC, created_at ASC, id ASC`.
#[derive(Debug, Clone)]
pub(crate) struct PendingCursor {
    pub priority: i32,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub id: uuid::Uuid,
}

impl From<&Task> for PendingCursor {
    fn from(t: &Task) -> Self {
        PendingCursor {
            priority: t.priority,
            created_at: t.created_at,
            id: t.id,
        }
    }
}

/// Fetch one page of Pending tasks, ordered by `priority DESC, created_at ASC, id ASC`,
/// optionally starting strictly after `cursor` (keyset pagination).
///
/// The keyset predicate is the *expanded* form of the mixed-order tuple comparison
/// (no direct tuple `<`/`>` comparison, which would be wrong for mixed ASC/DESC orders):
///
/// ```text
/// (priority < p)
///   OR (priority = p AND created_at > c)
///   OR (priority = p AND created_at = c AND id > i)
/// ```
///
/// This serves the `idx_task_priority(status, priority DESC, created_at ASC)` index.
/// The `id` tiebreaker keeps the cursor stable even if rows are claimed between pages.
pub(crate) async fn list_pending_page<'a>(
    conn: &mut Conn<'a>,
    cursor: Option<&PendingCursor>,
    page_size: i64,
) -> Result<Vec<Task>, DbError> {
    use crate::schema::task::dsl::*;

    let mut query = task
        .filter(status.eq(models::StatusKind::Pending))
        .order((priority.desc(), created_at.asc(), id.asc()))
        .limit(page_size)
        .into_boxed();

    if let Some(c) = cursor {
        // Expanded keyset predicate for mixed-order tuple (priority DESC, created_at ASC, id ASC).
        query = query.filter(
            priority.lt(c.priority).or(priority
                .eq(c.priority)
                .and(created_at.gt(c.created_at))
                .or(priority
                    .eq(c.priority)
                    .and(created_at.eq(c.created_at))
                    .and(id.gt(c.id)))),
        );
    }

    let tasks = query.get_results(conn).await?;
    Ok(tasks)
}

pub(crate) async fn list_task_filtered_paged<'a>(
    conn: &mut Conn<'a>,
    pagination: dtos::Pagination,
    filter: dtos::Filter,
) -> Result<Vec<dtos::BasicTaskDto>, DbError> {
    use crate::schema::task::dsl::*;
    use diesel::PgJsonbExpressionMethods;

    let mut query = task
        .into_boxed()
        .offset(pagination.offset)
        .limit(pagination.limit)
        .order((created_at.desc(), id.desc()));

    // Only apply the LIKE filters when a pattern is provided. `name`/`kind` are
    // NOT NULL, so skipping the filter on an empty string is equivalent to
    // `LIKE '%%'` while avoiding a useless predicate on every row.
    if !filter.name.is_empty() {
        query = query.filter(name.like(format!("%{}%", filter.name)));
    }

    if !filter.kind.is_empty() {
        query = query.filter(kind.like(format!("%{}%", filter.kind)));
    }

    if let Some(val) = filter.metadata {
        query = query.filter(metadata.contains(val));
    }

    if let Some(s) = filter.status {
        query = query.filter(status.eq(s));
    }

    if let Some(bid) = filter.batch_id {
        query = query.filter(batch_id.eq(bid));
    }

    if let Some(t) = filter.timeout {
        query = query.filter(timeout.eq(t));
    }

    // Targeted projection (Audit 2, B7): BasicTaskDto carries none of the heavy
    // JSONB columns, so select only the light columns instead of the full row.
    let result = query
        .select(dtos::BasicTaskRow::as_select())
        .load::<dtos::BasicTaskRow>(conn)
        .await?;

    let tasks: Vec<dtos::BasicTaskDto> = result.into_iter().map(dtos::BasicTaskDto::from).collect();

    Ok(tasks)
}

/// Get DAG data for a batch: all tasks and their links
pub(crate) async fn get_dag_for_batch<'a>(
    conn: &mut Conn<'a>,
    bid: Uuid,
) -> Result<dtos::DagDto, DbError> {
    use crate::schema::link::dsl::link;
    use crate::schema::task::dsl::*;

    // Get all tasks in the batch. Targeted projection (Audit 2, B7): the DAG
    // response is BasicTaskDto, which carries none of the heavy JSONB columns.
    let tasks_result = task
        .filter(batch_id.eq(bid))
        .order((created_at.asc(), id.asc()))
        .select(dtos::BasicTaskRow::as_select())
        .load::<dtos::BasicTaskRow>(conn)
        .await?;

    let task_ids: Vec<Uuid> = tasks_result.iter().map(|t| t.id).collect();

    // Get all links where both parent and child are in this batch
    let links_result = link
        .filter(
            crate::schema::link::dsl::parent_id
                .eq_any(&task_ids)
                .and(crate::schema::link::dsl::child_id.eq_any(&task_ids)),
        )
        .load::<Link>(conn)
        .await?;

    let tasks_dto: Vec<dtos::BasicTaskDto> = tasks_result
        .into_iter()
        .map(dtos::BasicTaskDto::from)
        .collect();

    let links_dto: Vec<dtos::LinkDto> = links_result
        .into_iter()
        .map(|l| dtos::LinkDto {
            parent_id: l.parent_id,
            child_id: l.child_id,
            requires_success: l.requires_success,
        })
        .collect();

    Ok(dtos::DagDto {
        tasks: tasks_dto,
        links: links_dto,
    })
}
