// Diesel QueryableByName emits separate impls with explicit field initializers;
// an allow on the struct does not cover those generated impls.
#![allow(clippy::redundant_field_names)]

use crate::{
    Conn,
    dtos::{self, escape_like_pattern},
    models::StatusKind,
    rule::Rules,
};
use chrono::Utc;
use diesel::prelude::*;
use diesel::sql_types;
use diesel_async::RunQueryDsl;

use super::DbError;

/// One row per (batch_id, status) pair — pivoted into `BatchSummaryDto` in Rust.
#[derive(Debug, diesel::QueryableByName)]
struct BatchStatusRow {
    #[diesel(sql_type = sql_types::Uuid)]
    batch_id: uuid::Uuid,
    #[diesel(sql_type = sql_types::Text)]
    status_text: String,
    #[diesel(sql_type = sql_types::BigInt)]
    cnt: i64,
    #[diesel(sql_type = sql_types::Timestamptz)]
    first_created_at: chrono::DateTime<Utc>,
    #[diesel(sql_type = sql_types::Timestamptz)]
    latest_updated_at: chrono::DateTime<Utc>,
    #[diesel(sql_type = sql_types::Array<sql_types::Text>)]
    kinds: Vec<String>,
    /// Batch scope (NULL when the batch has no `batch` row).
    #[diesel(sql_type = sql_types::Nullable<sql_types::Text>)]
    scope: Option<String>,
    /// Batch metadata (NULL when the batch has no `batch` row).
    #[diesel(sql_type = sql_types::Nullable<sql_types::Jsonb>)]
    metadata: Option<serde_json::Value>,
    /// Denormalized non-terminal task counter (NULL when the batch has no `batch` row).
    #[diesel(sql_type = sql_types::Nullable<sql_types::Integer>)]
    remaining: Option<i32>,
}

/// List batches with aggregated statistics, supporting optional filters and pagination.
///
/// Uses `GROUP BY (batch_id, status)` instead of repeated `COUNT(*) FILTER`
/// clauses — the pivot into per-status counts happens in Rust. A `LEFT JOIN batch`
/// surfaces the batch scope/metadata (NULL for batches that have no `batch` row).
/// All filter parameters are always bound as `Nullable` ($1–$8), so no dynamic
/// SQL assembly or bind-count branching is needed.
pub(crate) async fn list_batches<'a>(
    conn: &mut Conn<'a>,
    pagination: dtos::Pagination,
    filter: dtos::BatchFilterDto,
) -> Result<Vec<dtos::BatchSummaryDto>, DbError> {
    let kind_filter = filter.kind.map(|k| escape_like_pattern(&k));
    let name_filter = filter.name.map(|n| escape_like_pattern(&n));
    let search_filter = filter.search.map(|s| escape_like_pattern(&s));
    // A malformed filter must never silently broaden the result set. Surface the same
    // validation error semantics as GET /task instead of turning invalid JSON into NULL.
    let metadata_filter: Option<serde_json::Value> = match filter.metadata {
        None => None,
        Some(raw) => Some(serde_json::from_str(&raw).map_err(|e| {
            crate::error::ArcRunError::Validation(format!(
                "invalid `metadata` filter: expected valid JSON ({e})"
            ))
        })?),
    };

    let rows = diesel::sql_query(
        r#"
        WITH paginated_batches AS (
            SELECT
                t.batch_id,
                ARRAY_AGG(DISTINCT t.kind) AS kinds,
                MIN(t.created_at) AS first_created,
                b.scope AS scope,
                b.metadata AS metadata,
                b.remaining AS remaining
            FROM task t
            LEFT JOIN batch b ON b.id = t.batch_id
            WHERE t.batch_id IS NOT NULL
              AND ($1::text        IS NULL OR t.kind       LIKE '%' || $1 || '%')
              AND ($2::status_kind IS NULL OR t.status     = $2)
              AND ($3::text        IS NULL OR t.name       LIKE '%' || $3 || '%')
              AND ($4::timestamptz IS NULL OR t.created_at >= $4)
              AND ($5::timestamptz IS NULL OR t.created_at <= $5)
              AND ($6::text        IS NULL OR b.scope = $6)
              AND ($7::jsonb       IS NULL OR b.metadata @> $7)
              AND ($8::text        IS NULL
                   OR b.scope ILIKE '%' || $8 || '%'
                   OR b.metadata::text ILIKE '%' || $8 || '%')
            GROUP BY t.batch_id, b.scope, b.metadata, b.remaining
            ORDER BY first_created DESC, t.batch_id DESC
            LIMIT $9 OFFSET $10
        )
        SELECT
            t.batch_id,
            t.status::text AS status_text,
            COUNT(*)::bigint AS cnt,
            MIN(t.created_at) AS first_created_at,
            MAX(t.last_updated) AS latest_updated_at,
            pb.kinds,
            pb.scope,
            pb.metadata,
            pb.remaining
        FROM task t
        INNER JOIN paginated_batches pb ON t.batch_id = pb.batch_id
        GROUP BY t.batch_id, t.status, pb.kinds, pb.first_created, pb.scope, pb.metadata, pb.remaining
        ORDER BY pb.first_created DESC, t.batch_id
        "#,
    )
    .bind::<sql_types::Nullable<sql_types::Text>, _>(kind_filter)
    .bind::<sql_types::Nullable<crate::schema::sql_types::StatusKind>, _>(filter.status)
    .bind::<sql_types::Nullable<sql_types::Text>, _>(name_filter)
    .bind::<sql_types::Nullable<sql_types::Timestamptz>, _>(filter.created_after)
    .bind::<sql_types::Nullable<sql_types::Timestamptz>, _>(filter.created_before)
    .bind::<sql_types::Nullable<sql_types::Text>, _>(filter.scope)
    .bind::<sql_types::Nullable<sql_types::Jsonb>, _>(metadata_filter)
    .bind::<sql_types::Nullable<sql_types::Text>, _>(search_filter)
    .bind::<sql_types::BigInt, _>(pagination.limit)
    .bind::<sql_types::BigInt, _>(pagination.offset)
    .get_results::<BatchStatusRow>(conn)
    .await?;

    // Pivot: rows are ordered by batch, so group contiguous rows by batch_id.
    let mut batches: Vec<dtos::BatchSummaryDto> = Vec::new();
    for row in rows {
        let is_new_batch = batches.last().is_none_or(|b| b.batch_id != row.batch_id);
        if is_new_batch {
            batches.push(dtos::BatchSummaryDto {
                batch_id: row.batch_id,
                total_tasks: 0,
                first_created_at: row.first_created_at,
                latest_updated_at: row.latest_updated_at,
                status_counts: dtos::BatchStatusCounts::default(),
                kinds: row.kinds,
                scope: row.scope,
                metadata: row
                    .metadata
                    .unwrap_or_else(|| serde_json::Value::Object(Default::default())),
                remaining: row.remaining,
            });
        }
        let entry = batches.last_mut().unwrap();
        if row.first_created_at < entry.first_created_at {
            entry.first_created_at = row.first_created_at;
        }
        if row.latest_updated_at > entry.latest_updated_at {
            entry.latest_updated_at = row.latest_updated_at;
        }
        entry.total_tasks += row.cnt;
        match row.status_text.as_str() {
            "waiting" => entry.status_counts.waiting = row.cnt,
            "pending" => entry.status_counts.pending = row.cnt,
            "claimed" => entry.status_counts.claimed = row.cnt,
            "running" => entry.status_counts.running = row.cnt,
            "success" => entry.status_counts.success = row.cnt,
            "failure" => entry.status_counts.failure = row.cnt,
            "paused" => entry.status_counts.paused = row.cnt,
            "canceled" => entry.status_counts.canceled = row.cnt,
            _ => {}
        }
    }

    Ok(batches)
}

/// Get aggregated stats (success, failures, expected_count) for a single batch.
/// Uses a single GROUP BY query — returns None if no tasks match the batch_id.
pub(crate) async fn get_batch_stats<'a>(
    conn: &mut Conn<'a>,
    bid: uuid::Uuid,
) -> Result<Option<dtos::BatchStatsDto>, DbError> {
    #[derive(Debug, diesel::QueryableByName)]
    struct BatchStatsRow {
        #[diesel(sql_type = sql_types::BigInt)]
        total_tasks: i64,
        #[diesel(sql_type = sql_types::BigInt)]
        total_success: i64,
        #[diesel(sql_type = sql_types::BigInt)]
        total_failures: i64,
        #[diesel(sql_type = sql_types::Nullable<sql_types::BigInt>)]
        total_expected: Option<i64>,
        #[diesel(sql_type = sql_types::BigInt)]
        waiting: i64,
        #[diesel(sql_type = sql_types::BigInt)]
        pending: i64,
        #[diesel(sql_type = sql_types::BigInt)]
        claimed: i64,
        #[diesel(sql_type = sql_types::BigInt)]
        running: i64,
        #[diesel(sql_type = sql_types::BigInt)]
        cnt_success: i64,
        #[diesel(sql_type = sql_types::BigInt)]
        cnt_failure: i64,
        #[diesel(sql_type = sql_types::BigInt)]
        paused: i64,
        #[diesel(sql_type = sql_types::BigInt)]
        canceled: i64,
        #[diesel(sql_type = sql_types::Nullable<sql_types::Text>)]
        scope: Option<String>,
        #[diesel(sql_type = sql_types::Nullable<sql_types::Jsonb>)]
        metadata: Option<serde_json::Value>,
    }

    let row = diesel::sql_query(
        r#"
        SELECT
            COUNT(*)::bigint AS total_tasks,
            COALESCE(SUM(t.success), 0)::bigint AS total_success,
            COALESCE(SUM(t.failures), 0)::bigint AS total_failures,
            CASE WHEN COUNT(*) FILTER (WHERE t.expected_count IS NULL) = 0
                 THEN SUM(t.expected_count)::bigint
            END AS total_expected,
            COUNT(*) FILTER (WHERE t.status = 'waiting')::bigint AS waiting,
            COUNT(*) FILTER (WHERE t.status = 'pending')::bigint AS pending,
            COUNT(*) FILTER (WHERE t.status = 'claimed')::bigint AS claimed,
            COUNT(*) FILTER (WHERE t.status = 'running')::bigint AS running,
            COUNT(*) FILTER (WHERE t.status = 'success')::bigint AS cnt_success,
            COUNT(*) FILTER (WHERE t.status = 'failure')::bigint AS cnt_failure,
            COUNT(*) FILTER (WHERE t.status = 'paused')::bigint AS paused,
            COUNT(*) FILTER (WHERE t.status = 'canceled')::bigint AS canceled,
            b.scope AS scope,
            b.metadata AS metadata
        FROM task t
        LEFT JOIN batch b ON b.id = t.batch_id
        WHERE t.batch_id = $1
        GROUP BY t.batch_id, b.scope, b.metadata
        "#,
    )
    .bind::<sql_types::Uuid, _>(bid)
    .get_result::<BatchStatsRow>(conn)
    .await
    .optional()?;

    Ok(row.map(|r| dtos::BatchStatsDto {
        batch_id: bid,
        total_tasks: r.total_tasks,
        total_success: r.total_success,
        total_failures: r.total_failures,
        total_expected: r.total_expected,
        status_counts: dtos::BatchStatusCounts {
            waiting: r.waiting,
            pending: r.pending,
            claimed: r.claimed,
            running: r.running,
            success: r.cnt_success,
            failure: r.cnt_failure,
            paused: r.paused,
            canceled: r.canceled,
        },
        scope: r.scope,
        metadata: r
            .metadata
            .unwrap_or_else(|| serde_json::Value::Object(Default::default())),
    }))
}

/// Update concurrency/capacity rules for pre-claim tasks (Waiting/Pending/Paused) of a
/// given kind in a batch. Claimed/Running and terminal tasks are immutable here.
/// Returns the number of tasks updated, or NotFound if no tasks exist for the batch.
#[tracing::instrument(name = "update_batch_rules", skip(conn, new_rules), fields(batch_id = %bid, kind = %task_kind))]
pub(crate) async fn update_batch_rules<'a>(
    conn: &mut Conn<'a>,
    bid: uuid::Uuid,
    task_kind: &str,
    new_rules: Rules,
) -> Result<i64, DbError> {
    use crate::schema::task::dsl;

    // Check that the batch exists
    let total: i64 = dsl::task
        .filter(dsl::batch_id.eq(bid))
        .count()
        .get_result(conn)
        .await?;

    if total == 0 {
        return Err(crate::error::ArcRunError::NotFound {
            message: format!("No tasks found for batch {}", bid),
        });
    }

    let task_kind = task_kind.to_owned();
    super::run_in_transaction(conn, |conn| {
        Box::pin(async move {
            // A multi-row UPDATE alone takes locks in planner order. Pre-lock the
            // exact eligible set in task-id order, as the progress flush does.
            let locked_ids: Vec<uuid::Uuid> = dsl::task
                .filter(
                    dsl::batch_id.eq(bid).and(dsl::kind.eq(task_kind)).and(
                        dsl::status
                            .eq(StatusKind::Waiting)
                            .or(dsl::status.eq(StatusKind::Pending))
                            .or(dsl::status.eq(StatusKind::Paused)),
                    ),
                )
                .select(dsl::id)
                .order(dsl::id.asc())
                .for_update()
                .load(conn)
                .await?;

            // Claimed/Running tasks retain the rule set whose stored slot keys
            // they own. Only update the locked set: a previously Claimed task
            // requeued between the statements must not introduce an unordered lock.
            let updated = diesel::update(
                dsl::task.filter(
                    dsl::id.eq_any(&locked_ids).and(
                        dsl::status
                            .eq(StatusKind::Waiting)
                            .or(dsl::status.eq(StatusKind::Pending))
                            .or(dsl::status.eq(StatusKind::Paused)),
                    ),
                ),
            )
            .set((
                dsl::start_condition.eq(new_rules),
                dsl::last_updated.eq(diesel::dsl::now),
            ))
            .execute(conn)
            .await? as i64;

            Ok(updated)
        })
    })
    .await
}
