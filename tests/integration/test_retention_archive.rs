// Diesel QueryableByName emits separate impls with explicit field initializers;
// an allow on the struct does not cover those generated impls.
#![allow(clippy::redundant_field_names)]

//! Integration tests for the D6 (7.5b) task archive.
//!
//! The retention loop no longer DELETEs old terminal tasks — it MOVES them into the cold
//! `task_archive` table so `GET /task/{id}` keeps serving their history, while their
//! actions/links/webhook rows are still reclaimed. A separate purge (gated by
//! `RETENTION_ARCHIVE_DAYS > 0`) bounds the archive itself. These tests exercise the move,
//! the GET fallback, the exclusion from listings, the 404 on writes, the purge, and the
//! orphan-batch sweep after archiving.

use crate::common::*;
use diesel::sql_types;
use diesel_async::RunQueryDsl;
use serde_json::json;

#[derive(diesel::QueryableByName)]
struct Count {
    #[diesel(sql_type = sql_types::BigInt)]
    count: i64,
}

/// COUNT(*) of a single-parameter `... WHERE id = $1` query.
async fn count_by_id(pool: &arcrun::DbPool, query: &str, id: uuid::Uuid) -> i64 {
    let mut conn = pool.get().await.unwrap();
    let c: Count = diesel::sql_query(query)
        .bind::<sql_types::Uuid, _>(id)
        .get_result(&mut *conn)
        .await
        .unwrap();
    c.count
}

async fn task_count(pool: &arcrun::DbPool, id: uuid::Uuid) -> i64 {
    count_by_id(pool, "SELECT COUNT(*) AS count FROM task WHERE id = $1", id).await
}

async fn archive_count(pool: &arcrun::DbPool, id: uuid::Uuid) -> i64 {
    count_by_id(
        pool,
        "SELECT COUNT(*) AS count FROM task_archive WHERE id = $1",
        id,
    )
    .await
}

/// Backdate a terminal task's `ended_at` so the retention loop treats it as eligible.
async fn backdate_ended_at(pool: &arcrun::DbPool, id: uuid::Uuid, days: i64) {
    let mut conn = pool.get().await.unwrap();
    diesel::sql_query(format!(
        "UPDATE task SET ended_at = now() - interval '{days} days' WHERE id = $1"
    ))
    .bind::<sql_types::Uuid, _>(id)
    .execute(&mut *conn)
    .await
    .unwrap();
}

/// Backdate an archived task's `archived_at` so the purge treats it as eligible.
async fn backdate_archived_at(pool: &arcrun::DbPool, id: uuid::Uuid, days: i64) {
    let mut conn = pool.get().await.unwrap();
    diesel::sql_query(format!(
        "UPDATE task_archive SET archived_at = now() - interval '{days} days' WHERE id = $1"
    ))
    .bind::<sql_types::Uuid, _>(id)
    .execute(&mut *conn)
    .await
    .unwrap();
}

async fn run_cleanup(state: &arcrun::handlers::AppState) -> usize {
    // Normal production cleanup runs behind the delivery worker. Empty-action end
    // signals are nevertheless enqueued transactionally, so drain their fast-path
    // before exercising archive behavior unrelated to pending delivery.
    drain_outbox(state).await;
    run_cleanup_without_draining(state).await
}

async fn run_cleanup_without_draining(state: &arcrun::handlers::AppState) -> usize {
    let mut conn = state.pool.get().await.unwrap();
    arcrun::db_operation::cleanup_old_terminal_tasks(&mut conn, 30, 1000)
        .await
        .unwrap()
}

/// Retention must never destroy an at-least-once notification that has not reached a
/// terminal delivery state yet.
#[tokio::test]
async fn test_pending_task_outbox_blocks_archival() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let created = create_tasks_ok(
        &app,
        &[task_with_metadata(
            "pending-outbox",
            "Pending outbox",
            "archive-kind",
            json!({}),
        )],
    )
    .await;
    let id = created[0].id;
    succeed_task(&state, id).await;
    backdate_ended_at(&state.pool, id, 40).await;

    assert_eq!(
        count_by_id(
            &state.pool,
            "SELECT COUNT(*) AS count FROM webhook_outbox WHERE task_id = $1",
            id,
        )
        .await,
        1
    );
    assert_eq!(run_cleanup_without_draining(&state).await, 0);
    assert_eq!(
        task_count(&state.pool, id).await,
        1,
        "hot task is preserved"
    );
    assert_eq!(archive_count(&state.pool, id).await, 0);

    drain_outbox(&state).await;
    assert_eq!(run_cleanup_without_draining(&state).await, 1);
    assert_eq!(archive_count(&state.pool, id).await, 1);
}

/// A terminal task older than the retention window is MOVED out of `task` and INTO
/// `task_archive`, with its key fields (status, ended_at, metadata, batch_id) intact.
#[tokio::test]
async fn test_old_terminal_task_moves_to_archive() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let meta = json!({"archive_marker": "keep-me", "n": 7});
    let created = create_tasks_ok(
        &app,
        &[task_with_metadata(
            "t",
            "Archive Me",
            "arch-kind",
            meta.clone(),
        )],
    )
    .await;
    let id = created[0].id;
    let batch_id = created[0].batch_id;

    succeed_task(&state, id).await;
    backdate_ended_at(&state.pool, id, 40).await;

    let moved = run_cleanup(&state).await;
    assert_eq!(moved, 1, "exactly one task should be archived");

    assert_eq!(task_count(&state.pool, id).await, 0, "gone from hot table");
    assert_eq!(
        archive_count(&state.pool, id).await,
        1,
        "present in archive"
    );

    // Key fields survive the move verbatim.
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = sql_types::Text)]
        status: String,
        #[diesel(sql_type = sql_types::Jsonb)]
        metadata: serde_json::Value,
        #[diesel(sql_type = sql_types::Nullable<sql_types::Uuid>)]
        batch_id: Option<uuid::Uuid>,
        #[diesel(sql_type = sql_types::Nullable<sql_types::Timestamptz>)]
        ended_at: Option<chrono::DateTime<chrono::Utc>>,
    }
    let mut conn = state.pool.get().await.unwrap();
    let row: Row = diesel::sql_query(
        "SELECT status::text AS status, metadata, batch_id, ended_at \
         FROM task_archive WHERE id = $1",
    )
    .bind::<sql_types::Uuid, _>(id)
    .get_result(&mut *conn)
    .await
    .unwrap();
    assert_eq!(row.status, "success");
    assert_eq!(row.metadata, meta);
    assert_eq!(row.batch_id, batch_id);
    assert!(row.ended_at.is_some(), "ended_at preserved");
}

/// `GET /task/{id}` serves an archived task with the SAME response shape it had while
/// live. Compares the JSON before and after archiving, excluding only the expected
/// difference: `actions` (deleted on archive, so the archived DTO carries an empty array).
#[tokio::test]
async fn test_get_serves_archived_task_same_shape() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let created = create_tasks_ok(
        &app,
        &[task_with_metadata(
            "t",
            "Shape",
            "shape-kind",
            json!({"k": "v"}),
        )],
    )
    .await;
    let id = created[0].id;
    succeed_task(&state, id).await;
    // Backdate BEFORE snapshotting so the live snapshot's ended_at matches the value the
    // move copies into the archive (otherwise the backdate itself would be a spurious diff).
    backdate_ended_at(&state.pool, id, 40).await;

    // Snapshot the live response.
    let before_req = actix_web::test::TestRequest::get()
        .uri(&format!("/task/{id}"))
        .to_request();
    let before: serde_json::Value =
        actix_web::test::call_and_read_body_json(&app, before_req).await;

    assert_eq!(run_cleanup(&state).await, 1);
    assert_eq!(task_count(&state.pool, id).await, 0);

    // Snapshot the archived response — must still be 200 with the same fields.
    let after_req = actix_web::test::TestRequest::get()
        .uri(&format!("/task/{id}"))
        .to_request();
    let resp = actix_web::test::call_service(&app, after_req).await;
    assert_eq!(
        resp.status(),
        actix_web::http::StatusCode::OK,
        "archived task still served by GET"
    );
    let after: serde_json::Value = actix_web::test::read_body_json(resp).await;

    let mut before_cmp = before.clone();
    let mut after_cmp = after.clone();
    // actions are intentionally dropped on archive — the only expected diff.
    before_cmp.as_object_mut().unwrap().remove("actions");
    after_cmp.as_object_mut().unwrap().remove("actions");
    assert_eq!(
        before_cmp, after_cmp,
        "archived GET response matches the live one (except actions)"
    );
    assert_eq!(
        after["actions"].as_array().unwrap().len(),
        0,
        "archived task exposes an empty actions array"
    );
}

/// An archived task is served by `GET /task/{id}` but does NOT appear in `GET /task`
/// listings — those are hot-table-only queries.
#[tokio::test]
async fn test_archived_task_absent_from_listings() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let created = create_tasks_ok(
        &app,
        &[task_with_metadata(
            "t",
            "Listed",
            "list-kind",
            json!({"k": "v"}),
        )],
    )
    .await;
    let id = created[0].id;
    succeed_task(&state, id).await;
    backdate_ended_at(&state.pool, id, 40).await;
    assert_eq!(run_cleanup(&state).await, 1);

    let req = actix_web::test::TestRequest::get()
        .uri("/task?page_size=100")
        .to_request();
    let listed: Vec<arcrun::dtos::BasicTaskDto> =
        actix_web::test::call_and_read_body_json(&app, req).await;
    assert!(
        listed.iter().all(|t| t.id != id),
        "archived task must not appear in the listing"
    );
}

/// Writes never see the archive: a PATCH against an archived task is a 404, identical to
/// the pre-D6 post-DELETE behavior (PATCH only reads `task`).
#[tokio::test]
async fn test_patch_on_archived_task_is_404() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let created = create_tasks_ok(
        &app,
        &[task_with_metadata(
            "t",
            "Patch",
            "patch-kind",
            json!({"k": "v"}),
        )],
    )
    .await;
    let id = created[0].id;
    succeed_task(&state, id).await;
    backdate_ended_at(&state.pool, id, 40).await;
    assert_eq!(run_cleanup(&state).await, 1);

    let req = actix_web::test::TestRequest::patch()
        .uri(&format!("/task/{id}"))
        .set_json(json!({"status": "Success"}))
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(
        resp.status(),
        actix_web::http::StatusCode::NOT_FOUND,
        "PATCH on an archived task must be 404"
    );
}

/// `RETENTION_ARCHIVE_DAYS > 0`: the archive purge removes rows older than the window and
/// spares newer ones.
#[tokio::test]
async fn test_archive_purge_respects_window() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // Two terminal tasks, both archived.
    let created = create_tasks_ok(
        &app,
        &[
            task_with_metadata("old", "Old", "purge-kind", json!({"k": "old"})),
            task_with_metadata("new", "New", "purge-kind", json!({"k": "new"})),
        ],
    )
    .await;
    let old_id = created[0].id;
    let new_id = created[1].id;
    succeed_task(&state, old_id).await;
    succeed_task(&state, new_id).await;
    backdate_ended_at(&state.pool, old_id, 40).await;
    backdate_ended_at(&state.pool, new_id, 40).await;
    assert_eq!(run_cleanup(&state).await, 2, "both archived");

    // Make the "old" archive row look 100 days old; leave "new" fresh.
    backdate_archived_at(&state.pool, old_id, 100).await;

    // Purge with a 90-day window.
    let purged = {
        let mut conn = state.pool.get().await.unwrap();
        arcrun::db_operation::purge_old_archived_tasks(&mut conn, 90, 1000)
            .await
            .unwrap()
    };
    assert_eq!(purged, 1, "only the old archive row is purged");
    assert_eq!(
        archive_count(&state.pool, old_id).await,
        0,
        "old archive row purged"
    );
    assert_eq!(
        archive_count(&state.pool, new_id).await,
        1,
        "recent archive row survives"
    );
}

/// The orphan-batch sweep still works after archiving: once every task of a batch (with a
/// `batch` row but no pending batch_complete signal) is archived, the batch row is swept.
#[tokio::test]
async fn test_orphan_batch_swept_after_archiving() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // Object form with `scope` forces a `batch` row, but with an empty on_complete there
    // is NO batch_complete outbox row, so the orphan sweep is not spared.
    let body = json!({
        "tasks": [task_with_metadata("t", "Batched", "sweep-kind", json!({"k": "v"}))],
        "scope": "archive-sweep"
    });
    let req = actix_web::test::TestRequest::post()
        .uri("/task")
        .set_json(&body)
        .to_request();
    let created: Vec<arcrun::dtos::BasicTaskDto> =
        actix_web::test::call_and_read_body_json(&app, req).await;
    let id = created[0].id;
    let batch_id = created[0]
        .batch_id
        .expect("batch row created for scope batch");

    assert_eq!(
        count_by_id(
            &state.pool,
            "SELECT COUNT(*) AS count FROM batch WHERE id = $1",
            batch_id
        )
        .await,
        1,
        "batch row exists before cleanup"
    );

    succeed_task(&state, id).await;
    backdate_ended_at(&state.pool, id, 40).await;
    assert_eq!(run_cleanup(&state).await, 1, "task archived");

    assert_eq!(archive_count(&state.pool, id).await, 1, "task in archive");
    assert_eq!(
        count_by_id(
            &state.pool,
            "SELECT COUNT(*) AS count FROM batch WHERE id = $1",
            batch_id
        )
        .await,
        0,
        "orphaned batch row swept once all its tasks are archived"
    );
    // The archived task keeps the (now-deleted) batch_id — no FK, dangling by design.
}
