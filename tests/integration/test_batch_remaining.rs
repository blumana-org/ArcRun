// Diesel QueryableByName emits separate impls with explicit field initializers;
// an allow on the struct does not cover those generated impls.
#![allow(clippy::redundant_field_names)]

//! Audit 2, D2 — `batch.remaining` counter.
//!
//! D2 replaces the old batch-complete detection (`FOR UPDATE` on the `batch` row +
//! a `NOT EXISTS (task active)` probe) with a denormalized `batch.remaining` counter:
//! it is initialized to the number of tasks actually inserted (dedupe-skips excluded)
//! and each terminal transition decrements it in the SAME transaction with
//! `UPDATE batch SET remaining = GREATEST(remaining - N, 0) … RETURNING remaining`.
//! `remaining = 0` IS the completion signal (gated on a non-empty `on_complete`, #601)
//! and doubles as free progress reporting via `GET /batches`.
//!
//! These tests cover: fires-once on the last task; dedupe-skips not counted; stop_batch
//! zeroing; timeout; the vacuous empty batch; a scope-only batch (counter tracked but no
//! enqueue); `remaining` visible + correct through `GET /batches`; and the B5 cascade
//! (one transition terminalizes many tasks — the counter decrements by exactly that many).

use crate::common::*;

use serde_json::json;

// ============================================================================
// Helpers
// ============================================================================

/// Read `batch.remaining` for a batch row (None if the batch has no row).
async fn batch_remaining(pool: &arcrun::DbPool, batch_id: uuid::Uuid) -> Option<i32> {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Integer)]
        remaining: i32,
    }
    let mut conn = pool.get().await.unwrap();
    let r: Option<Row> = diesel_async::RunQueryDsl::get_result(
        diesel::sql_query("SELECT remaining FROM batch WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(batch_id),
        &mut *conn,
    )
    .await
    .ok();
    r.map(|r| r.remaining)
}

/// Total `batch_complete` outbox rows for a batch (any status).
async fn batch_complete_total(pool: &arcrun::DbPool, batch_id: uuid::Uuid) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Cnt {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        c: i64,
    }
    let mut conn = pool.get().await.unwrap();
    let r: Cnt = diesel_async::RunQueryDsl::get_result(
        diesel::sql_query(
            // D3: the batch_complete signal is in the queue (webhook_outbox) until
            // delivered, then in the history (webhook_execution) — count both.
            "SELECT (\
                (SELECT count(*) FROM webhook_execution WHERE batch_id = $1 AND trigger = 'batch_complete') + \
                (SELECT count(*) FROM webhook_outbox    WHERE batch_id = $1 AND trigger = 'batch_complete')\
             ) AS c",
        )
        .bind::<diesel::sql_types::Uuid, _>(batch_id),
        &mut *conn,
    )
    .await
    .unwrap();
    r.c
}

/// POST an object-form body, return (status, batch_id).
async fn post_body<S, B>(
    app: &S,
    body: &serde_json::Value,
) -> (actix_web::http::StatusCode, Option<uuid::Uuid>)
where
    S: actix_web::dev::Service<
            actix_http::Request,
            Response = actix_web::dev::ServiceResponse<B>,
            Error = actix_web::Error,
        >,
    B: actix_web::body::MessageBody,
{
    let req = actix_web::test::TestRequest::post()
        .uri("/task")
        .insert_header(("requester", "test"))
        .set_json(body)
        .to_request();
    let resp = actix_web::test::call_service(app, req).await;
    let status = resp.status();
    let batch_id = resp
        .headers()
        .get("X-Batch-ID")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| uuid::Uuid::parse_str(s).ok());
    (status, batch_id)
}

/// Resolve the task ids of a batch (order-independent, sorted by name).
async fn batch_task_ids(pool: &arcrun::DbPool, batch_id: uuid::Uuid) -> Vec<uuid::Uuid> {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: uuid::Uuid,
    }
    let mut conn = pool.get().await.unwrap();
    let rows: Vec<Row> = diesel_async::RunQueryDsl::get_results(
        diesel::sql_query("SELECT id FROM task WHERE batch_id = $1 ORDER BY name")
            .bind::<diesel::sql_types::Uuid, _>(batch_id),
        &mut *conn,
    )
    .await
    .unwrap();
    rows.into_iter().map(|r| r.id).collect()
}

/// GET /batches and return this batch's `remaining` field (as reported by the API).
async fn api_remaining<S, B>(app: &S, batch_id: uuid::Uuid) -> Option<i64>
where
    S: actix_web::dev::Service<
            actix_http::Request,
            Response = actix_web::dev::ServiceResponse<B>,
            Error = actix_web::Error,
        >,
    B: actix_web::body::MessageBody,
{
    let req = actix_web::test::TestRequest::get()
        .uri("/batches")
        .to_request();
    let resp = actix_web::test::call_service(app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::OK);
    let list: Vec<serde_json::Value> = actix_web::test::read_body_json(resp).await;
    let target = list
        .into_iter()
        .find(|b| b["batch_id"].as_str() == Some(&batch_id.to_string()))
        .expect("batch should appear in GET /batches");
    // `remaining` is Option<i32> in the DTO; JSON null when the batch has no row.
    target["remaining"].as_i64()
}

fn webhook_batch_body(tasks: serde_json::Value, url: &str) -> serde_json::Value {
    json!({
        "tasks": tasks,
        "on_batch_complete": [
            {"kind": "Webhook", "params": {"url": url, "verb": "Post"}}
        ]
    })
}

// ============================================================================
// Tests
// ============================================================================

/// remaining counts down and the batch_complete row is enqueued ONLY on the last task.
#[tokio::test]
async fn test_remaining_fires_once_on_last_task() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let body = webhook_batch_body(
        json!([
            task_json("r1", "R1", "rem"),
            task_json("r2", "R2", "rem"),
            task_json("r3", "R3", "rem"),
        ]),
        "https://example.com/done",
    );
    let (status, batch_id) = post_body(&app, &body).await;
    assert_eq!(status, actix_web::http::StatusCode::CREATED);
    let batch_id = batch_id.unwrap();
    let ids = batch_task_ids(&state.pool, batch_id).await;

    assert_eq!(
        batch_remaining(&state.pool, batch_id).await,
        Some(3),
        "remaining initialized to the number of inserted tasks"
    );

    succeed_task(&state, ids[0]).await;
    assert_eq!(batch_remaining(&state.pool, batch_id).await, Some(2));
    assert_eq!(batch_complete_total(&state.pool, batch_id).await, 0);

    fail_task(&state, ids[1], "boom").await;
    assert_eq!(batch_remaining(&state.pool, batch_id).await, Some(1));
    assert_eq!(batch_complete_total(&state.pool, batch_id).await, 0);

    succeed_task(&state, ids[2]).await;
    assert_eq!(
        batch_remaining(&state.pool, batch_id).await,
        Some(0),
        "remaining reaches 0 on the last terminal transition"
    );
    assert_eq!(
        batch_complete_total(&state.pool, batch_id).await,
        1,
        "exactly one batch_complete row, on the last task"
    );
}

/// Dedupe-skipped tasks are NOT counted in `remaining` (known at insert time).
#[tokio::test]
async fn test_remaining_dedupe_skipped_not_counted() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // Pre-create a task the dedupe will match, so the "dup" task below is skipped.
    let pre = json!([task_with_metadata("p", "P", "remd", json!({"k": "x"}))]);
    create_tasks_ok(&app, pre.as_array().unwrap()).await;

    // Batch: one dedupe task that WILL be skipped + one real task, plus a webhook.
    let body = json!({
        "tasks": [
            {
                "id": "dup", "name": "Dup", "kind": "remd", "timeout": 60,
                "metadata": {"k": "x"}, "on_start": webhook_action(),
                "dedupe_strategy": [{"kind": "remd", "status": "Pending", "fields": ["k"]}]
            },
            task_json("real", "Real", "remd")
        ],
        "on_batch_complete": [
            {"kind": "Webhook", "params": {"url": "https://example.com/done", "verb": "Post"}}
        ]
    });
    let (status, batch_id) = post_body(&app, &body).await;
    assert_eq!(status, actix_web::http::StatusCode::CREATED);
    let batch_id = batch_id.unwrap();

    assert_eq!(
        batch_remaining(&state.pool, batch_id).await,
        Some(1),
        "remaining counts only the 1 task actually inserted (dedupe-skip excluded)"
    );

    // The batch has exactly one (real) task; complete it -> remaining 0 -> fires once.
    let ids = batch_task_ids(&state.pool, batch_id).await;
    assert_eq!(ids.len(), 1, "only the real task was inserted");
    succeed_task(&state, ids[0]).await;

    assert_eq!(batch_remaining(&state.pool, batch_id).await, Some(0));
    assert_eq!(
        batch_complete_total(&state.pool, batch_id).await,
        1,
        "the single real task completing fires batch_complete exactly once"
    );
}

/// stop_batch drives `remaining` straight to 0 and fires the signal.
#[tokio::test]
async fn test_remaining_stop_batch_zeroes() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let body = webhook_batch_body(
        json!([task_json("s1", "S1", "rems"), task_json("s2", "S2", "rems"),]),
        "https://example.com/done",
    );
    let (_s, batch_id) = post_body(&app, &body).await;
    let batch_id = batch_id.unwrap();
    assert_eq!(batch_remaining(&state.pool, batch_id).await, Some(2));

    let stop_req = actix_web::test::TestRequest::delete()
        .uri(&format!("/batch/{}", batch_id))
        .to_request();
    let stop_resp = actix_web::test::call_service(&app, stop_req).await;
    assert_eq!(stop_resp.status(), actix_web::http::StatusCode::OK);

    assert_eq!(
        batch_remaining(&state.pool, batch_id).await,
        Some(0),
        "stop_batch sets remaining to 0 in one sweep"
    );
    assert_eq!(
        batch_complete_total(&state.pool, batch_id).await,
        1,
        "stop_batch fires batch_complete exactly once"
    );
}

/// A timeout is a terminal transition: it decrements `remaining` and can complete a batch.
#[tokio::test]
async fn test_remaining_timeout_decrements() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let body = webhook_batch_body(
        json!([task_json("to", "TO", "remt")]),
        "https://example.com/done",
    );
    let (_s, batch_id) = post_body(&app, &body).await;
    let batch_id = batch_id.unwrap();
    let ids = batch_task_ids(&state.pool, batch_id).await;
    assert_eq!(batch_remaining(&state.pool, batch_id).await, Some(1));

    // Force the only task into Running with a stale last_updated so the timeout loop trips it.
    {
        use diesel_async::RunQueryDsl;
        let past = chrono::Utc::now() - chrono::Duration::seconds(120);
        let mut conn = state.pool.get().await.unwrap();
        diesel::sql_query(
            "UPDATE task SET status = 'running', started_at = $1, last_updated = $1 WHERE id = $2",
        )
        .bind::<diesel::sql_types::Timestamptz, _>(past)
        .bind::<diesel::sql_types::Uuid, _>(ids[0])
        .execute(&mut *conn)
        .await
        .unwrap();
    }

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let pool = state.pool.clone();
    let handle = tokio::spawn(async move {
        arcrun::workers::timeout_loop(
            pool,
            std::time::Duration::from_millis(50),
            std::time::Duration::from_secs(30),
            true,
            100,
            shutdown_rx,
            arcrun::workers::WorkerNudges::new(),
        )
        .await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let _ = shutdown_tx.send(true);
    let _ = handle.await;

    assert_eq!(
        get_task_ok(&app, ids[0]).await.status,
        arcrun::models::StatusKind::Failure,
        "task should be timed out"
    );
    assert_eq!(
        batch_remaining(&state.pool, batch_id).await,
        Some(0),
        "timeout decremented remaining to 0"
    );
    assert_eq!(
        batch_complete_total(&state.pool, batch_id).await,
        1,
        "timeout of the last task fires batch_complete"
    );
}

/// A vacuously-complete batch (0 tasks inserted) has remaining=0 and fires immediately.
#[tokio::test]
async fn test_remaining_empty_batch_vacuous() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // Pre-create a task the dedupe will match, so the single batch task is skipped.
    let pre = json!([task_with_metadata("p", "P", "reme", json!({"k": "x"}))]);
    create_tasks_ok(&app, pre.as_array().unwrap()).await;

    let body = json!({
        "tasks": [{
            "id": "dup", "name": "Dup", "kind": "reme", "timeout": 60,
            "metadata": {"k": "x"}, "on_start": webhook_action(),
            "dedupe_strategy": [{"kind": "reme", "status": "Pending", "fields": ["k"]}]
        }],
        "on_batch_complete": [
            {"kind": "Webhook", "params": {"url": "https://example.com/done", "verb": "Post"}}
        ]
    });
    let (status, batch_id) = post_body(&app, &body).await;
    assert_eq!(
        status,
        actix_web::http::StatusCode::NO_CONTENT,
        "all tasks deduped -> 204"
    );
    let batch_id = batch_id.unwrap();

    assert_eq!(
        batch_remaining(&state.pool, batch_id).await,
        Some(0),
        "an all-deduped batch is vacuously complete: remaining = 0"
    );
    assert_eq!(
        batch_complete_total(&state.pool, batch_id).await,
        1,
        "vacuously-complete batch enqueues the signal immediately"
    );
}

/// A scope-only batch (`on_complete = '[]'`) keeps `remaining` tracked for progress
/// reporting but NEVER enqueues a batch_complete row.
#[tokio::test]
async fn test_remaining_scope_only_tracked_no_enqueue() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let body = json!({
        "tasks": [
            task_json("sc1", "SC1", "remsc"),
            task_json("sc2", "SC2", "remsc"),
        ],
        "scope": "rem-scope-only",
        "metadata": {"env": "test"}
    });
    let (status, batch_id) = post_body(&app, &body).await;
    assert_eq!(status, actix_web::http::StatusCode::CREATED);
    let batch_id = batch_id.unwrap();

    assert_eq!(
        batch_remaining(&state.pool, batch_id).await,
        Some(2),
        "scope-only batch still tracks remaining"
    );

    let ids = batch_task_ids(&state.pool, batch_id).await;
    succeed_task(&state, ids[0]).await;
    assert_eq!(
        batch_remaining(&state.pool, batch_id).await,
        Some(1),
        "scope-only remaining decrements for progress reporting"
    );
    succeed_task(&state, ids[1]).await;
    assert_eq!(
        batch_remaining(&state.pool, batch_id).await,
        Some(0),
        "scope-only remaining reaches 0"
    );

    assert_eq!(
        batch_complete_total(&state.pool, batch_id).await,
        0,
        "a scope-only batch (empty on_complete) must NOT enqueue a batch_complete row"
    );
}

/// `remaining` is visible and correct via GET /batches across transitions.
#[tokio::test]
async fn test_remaining_visible_via_get_batches() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let body = webhook_batch_body(
        json!([task_json("g1", "G1", "remg"), task_json("g2", "G2", "remg"),]),
        "https://example.com/done",
    );
    let (_s, batch_id) = post_body(&app, &body).await;
    let batch_id = batch_id.unwrap();

    assert_eq!(
        api_remaining(&app, batch_id).await,
        Some(2),
        "GET /batches reports remaining = 2 at creation"
    );

    let ids = batch_task_ids(&state.pool, batch_id).await;
    succeed_task(&state, ids[0]).await;
    assert_eq!(
        api_remaining(&app, batch_id).await,
        Some(1),
        "GET /batches reports remaining = 1 after one task"
    );

    succeed_task(&state, ids[1]).await;
    assert_eq!(
        api_remaining(&app, batch_id).await,
        Some(0),
        "GET /batches reports remaining = 0 when complete"
    );
}

/// A batch with NO `batch` row reports `remaining = null` via GET /batches.
#[tokio::test]
async fn test_remaining_null_for_no_batch_row() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // Plain batch: no on_batch_complete / scope / metadata -> no `batch` row.
    let created = create_tasks_ok(&app, &[task_json("nb1", "NB1", "remnb")]).await;
    let batch_id = created[0].batch_id.expect("tasks share a batch_id");

    assert_eq!(
        batch_remaining(&state.pool, batch_id).await,
        None,
        "no batch row -> no remaining counter"
    );
    assert_eq!(
        api_remaining(&app, batch_id).await,
        None,
        "GET /batches reports remaining = null for a batch with no batch row"
    );
}

/// B5 cascade: one terminal transition that fails a root AND cascade-fails N children
/// decrements `remaining` by exactly N+1 (in that single transaction) and fires once.
#[tokio::test]
async fn test_remaining_cascade_b5_multi_decrement() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // root + 3 children each requiring the root's success.
    let body = json!({
        "tasks": [
            task_json("root", "root", "remc"),
            task_with_deps("c1", "c1", "remc", vec![("root", true)]),
            task_with_deps("c2", "c2", "remc", vec![("root", true)]),
            task_with_deps("c3", "c3", "remc", vec![("root", true)]),
        ],
        "on_batch_complete": [
            {"kind": "Webhook", "params": {"url": "https://example.com/done", "verb": "Post"}}
        ]
    });
    let (status, batch_id) = post_body(&app, &body).await;
    assert_eq!(status, actix_web::http::StatusCode::CREATED);
    let batch_id = batch_id.unwrap();

    assert_eq!(
        batch_remaining(&state.pool, batch_id).await,
        Some(4),
        "remaining initialized to root + 3 children"
    );

    // Resolve the root id (name = "root").
    let root = {
        let ids = batch_task_ids(&state.pool, batch_id).await; // sorted by name: c1,c2,c3,root
        *ids.last().unwrap()
    };

    // Fail the root: propagation cascade-fails all 3 children in the SAME transaction,
    // so remaining is decremented by 4 (root + 3) in one shot -> 0.
    fail_task(&state, root, "root failed").await;

    assert_eq!(
        batch_remaining(&state.pool, batch_id).await,
        Some(0),
        "one cascade transition decrements remaining by N+1 (root + 3 children)"
    );
    assert_eq!(
        batch_complete_total(&state.pool, batch_id).await,
        1,
        "the cascade transition fires batch_complete exactly once"
    );
}
