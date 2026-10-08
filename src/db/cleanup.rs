// Diesel QueryableByName emits separate impls with explicit field initializers;
// an allow on the struct does not cover those generated impls.
#![allow(clippy::redundant_field_names)]

use crate::Conn;
use chrono::Utc;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;

use super::{DbError, run_in_transaction};

/// The explicit `task` / `task_archive` column list shared by the archive MOVE below.
/// Kept explicit (never `SELECT *`) so the move survives any future column-order
/// divergence between the two tables. `archived_at` is appended on the target side only.
const ARCHIVE_COLUMNS: &str = "id, name, kind, status, timeout, created_at, started_at, \
    last_updated, metadata, ended_at, start_condition, wait_success, wait_finished, \
    success, failures, failure_reason, batch_id, expected_count, dead_end_barrier, \
    priority, claimed_slot_keys, capacity_charge";

/// Archive terminal tasks (Success/Failure/Canceled) with `ended_at` older than the
/// retention period (Audit 2, D6). Instead of DELETEing the task rows, this MOVES them
/// into `task_archive` so `GET /task/{id}` can still serve their history. The actions,
/// links and webhook rows of those tasks are still DELETED (the archive preserves the
/// task record, not its tooling), in FK order (actions → webhooks → links → move) within
/// one transaction. The orphan-`batch` sweep is unchanged: a batch whose tasks are all
/// archived no longer has any `task` rows, so it is swept as an orphan (its `batch_id`
/// lives on in `task_archive` without an FK). Returns the number of tasks archived.
pub async fn cleanup_old_terminal_tasks<'a>(
    conn: &mut Conn<'a>,
    retention_days: u32,
    batch_size: i64,
) -> Result<usize, DbError> {
    let cutoff = Utc::now() - chrono::Duration::days(retention_days as i64);

    #[derive(diesel::QueryableByName)]
    struct TaskIdRow {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: uuid::Uuid,
    }

    // A terminal task is not eligible while its own notification is queued. Batch
    // members are also retained from the first terminal member until batch_complete
    // reaches a terminal delivery state: while `remaining > 0` no signal exists yet,
    // then the outbox-row guard covers the queued signal. Batch enrichment is computed
    // from hot `task` rows at delivery time, so archiving one early would under-count it.
    let task_ids: Vec<uuid::Uuid> = diesel::sql_query(
        "SELECT t.id
         FROM task t
         WHERE t.status IN ('success', 'failure', 'canceled')
           AND t.ended_at <= $1
           AND NOT EXISTS (
               SELECT 1 FROM webhook_outbox wo WHERE wo.task_id = t.id
           )
           AND NOT EXISTS (
               SELECT 1 FROM webhook_outbox wo WHERE wo.batch_id = t.batch_id
           )
           AND NOT EXISTS (
               SELECT 1
               FROM batch b
               WHERE b.id = t.batch_id
                 AND jsonb_typeof(b.on_complete) = 'array'
                 AND jsonb_array_length(b.on_complete) > 0
                 AND b.remaining > 0
           )
         ORDER BY t.ended_at, t.id
         LIMIT $2",
    )
    .bind::<diesel::sql_types::Timestamptz, _>(cutoff)
    .bind::<diesel::sql_types::BigInt, _>(batch_size)
    .load::<TaskIdRow>(conn)
    .await?
    .into_iter()
    .map(|row| row.id)
    .collect();

    if task_ids.is_empty() {
        return Ok(0);
    }

    let count = task_ids.len();

    run_in_transaction(conn, |conn| {
        Box::pin(async move {
            use crate::schema::action::dsl as action_dsl;
            use crate::schema::link::dsl as link_dsl;
            use crate::schema::task::dsl as task_dsl;
            use crate::schema::webhook_execution::dsl as we_dsl;
            use crate::schema::webhook_outbox::dsl as wo_dsl;

            // 1. Delete actions belonging to these tasks
            diesel::delete(action_dsl::action.filter(action_dsl::task_id.eq_any(&task_ids)))
                .execute(&mut *conn)
                .await?;

            // 2. Delete webhook rows (FK on task_id): the delivery QUEUE
            // (webhook_outbox — normally empty for terminal tasks this old, but
            // belt-and-braces) and the ledger/history (webhook_execution).
            diesel::delete(wo_dsl::webhook_outbox.filter(wo_dsl::task_id.eq_any(&task_ids)))
                .execute(&mut *conn)
                .await?;
            diesel::delete(we_dsl::webhook_execution.filter(we_dsl::task_id.eq_any(&task_ids)))
                .execute(&mut *conn)
                .await?;

            // 3. Delete links referencing these tasks (as parent or child)
            diesel::delete(
                link_dsl::link.filter(
                    link_dsl::parent_id
                        .eq_any(&task_ids)
                        .or(link_dsl::child_id.eq_any(&task_ids)),
                ),
            )
            .execute(&mut *conn)
            .await?;

            // 4. MOVE the tasks into the cold archive (D6): a single atomic statement
            //    that DELETEs from `task` and INSERTs the same rows into `task_archive`
            //    with `archived_at = now()`. Explicit column lists on both sides (see
            //    ARCHIVE_COLUMNS) — never `SELECT *` — so the move is immune to any
            //    column-order divergence between the two tables. Runs in this same
            //    transaction as the actions/links/webhook deletions above.
            let move_sql = format!(
                "WITH moved AS (
                     DELETE FROM task WHERE id = ANY($1) RETURNING {cols}
                 )
                 INSERT INTO task_archive ({cols}, archived_at)
                 SELECT {cols}, now() FROM moved",
                cols = ARCHIVE_COLUMNS
            );
            diesel::sql_query(move_sql)
                .bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(&task_ids)
                .execute(&mut *conn)
                .await?;

            // 5. Clean up orphaned `batch` rows (Lot 3b): a batch row whose tasks have
            // all been deleted is unreachable. Delete its batch-level webhook rows first
            // (FK on batch_id — both the queue and the history), then the batch rows
            // themselves. We scope to batches touched by the tasks just deleted to keep
            // this bounded.
            use crate::schema::batch::dsl as batch_dsl;
            let orphan_batch_ids: Vec<uuid::Uuid> = batch_dsl::batch
                .filter(diesel::dsl::not(diesel::dsl::exists(
                    task_dsl::task.filter(task_dsl::batch_id.eq(batch_dsl::id.nullable())),
                )))
                // Never sweep a batch whose batch_complete signal is still awaiting
                // delivery: an empty batch (all tasks dedupe-skipped) has no tasks at
                // all, so it is "orphaned" from birth — deleting it here would destroy
                // the queued outbox row and lose the at-least-once signal. A present
                // webhook_outbox row (D3) with this batch_id IS the pending signal.
                .filter(diesel::dsl::not(diesel::dsl::exists(
                    wo_dsl::webhook_outbox.filter(wo_dsl::batch_id.eq(batch_dsl::id.nullable())),
                )))
                .select(batch_dsl::id)
                .limit(batch_size)
                .load::<uuid::Uuid>(&mut *conn)
                .await?;

            if !orphan_batch_ids.is_empty() {
                diesel::delete(
                    wo_dsl::webhook_outbox.filter(wo_dsl::batch_id.eq_any(&orphan_batch_ids)),
                )
                .execute(&mut *conn)
                .await?;
                diesel::delete(
                    we_dsl::webhook_execution.filter(we_dsl::batch_id.eq_any(&orphan_batch_ids)),
                )
                .execute(&mut *conn)
                .await?;

                diesel::delete(batch_dsl::batch.filter(batch_dsl::id.eq_any(&orphan_batch_ids)))
                    .execute(&mut *conn)
                    .await?;
            }

            Ok(count)
        })
    })
    .await
}

/// Garbage-collect empty concurrency slot rows (Audit 2, D1). Slot keys are derived
/// from task metadata and are therefore unbounded, so without periodic GC the
/// `rule_slot` table grows forever. Deleting a `used = 0` row is safe under concurrency:
/// the DELETE takes a row lock, and Postgres re-checks `used = 0` after acquiring it —
/// so a row a concurrent claim just incremented (to >= 1) is NOT deleted. If a key is
/// needed again after deletion, the claim's `INSERT ... ON CONFLICT` recreates it.
/// Returns the number of rows deleted.
pub async fn gc_empty_rule_slots<'a>(conn: &mut Conn<'a>) -> Result<usize, DbError> {
    let deleted = diesel::sql_query("DELETE FROM rule_slot WHERE used = 0")
        .execute(conn)
        .await?;
    Ok(deleted)
}

/// Purge archived tasks (Audit 2, D6) whose `archived_at` is older than
/// `archive_retention_days`, in bounded batches of at most `batch_size`. `task_archive`
/// is a cold, FK-free store, so a plain bounded DELETE suffices (no dependent rows to
/// clean first). Returns the number of archive rows purged.
///
/// This is the ONLY thing that bounds archive growth: with `RETENTION_ARCHIVE_DAYS=0`
/// (the default) the caller never invokes this, and the archive keeps every terminal
/// task forever — growth just moves to a cold table. Set the parameter to bound it.
pub async fn purge_old_archived_tasks<'a>(
    conn: &mut Conn<'a>,
    archive_retention_days: u32,
    batch_size: i64,
) -> Result<usize, DbError> {
    let cutoff = Utc::now() - chrono::Duration::days(archive_retention_days as i64);

    // Bounded DELETE via an id subquery (Postgres has no DELETE ... LIMIT): pick at most
    // `batch_size` old ids, then delete exactly those. The `archived_at` index serves the
    // inner scan.
    let deleted = diesel::sql_query(
        "DELETE FROM task_archive
         WHERE id IN (
             SELECT id FROM task_archive WHERE archived_at <= $1 LIMIT $2
         )",
    )
    .bind::<diesel::sql_types::Timestamptz, _>(cutoff)
    .bind::<diesel::sql_types::BigInt, _>(batch_size)
    .execute(conn)
    .await?;
    Ok(deleted)
}
