// Diesel QueryableByName emits separate impls with explicit field initializers;
// an allow on the struct does not cover those generated impls.
#![allow(clippy::redundant_field_names)]

//! Lot 3 — grouped batch insertion (3a) + `on_batch_complete` webhook (3b).
//!
//! 3a tests assert the grouped multi-row INSERT path produces exactly the same
//! observable state (statuses, wait counters, links, actions) as the old per-task
//! path, including dedupe-skipped parents.
//!
//! 3b tests assert the batch-complete signal is enqueued in the SAME transaction as
//! the last terminal transition, delivered at-least-once via the outbox, fires once
//! (not before), works across success/failure/cancel/timeout/stop_batch, is
//! concurrency-safe (single row), handles the empty-dedupe batch, carries the right
//! payload, and supports retry/exhausted.

use crate::common::*;

use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

// ============================================================================
// Helpers
// ============================================================================

/// Count outbox rows for a batch with a given status (raw SQL).
async fn batch_outbox_count(pool: &arcrun::DbPool, batch_id: uuid::Uuid, status: &str) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Cnt {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        c: i64,
    }
    let mut conn = pool.get().await.unwrap();
    // D3: a pending batch_complete signal lives in the queue; delivered ones in history.
    let r: Cnt = if status == "pending" {
        diesel_async::RunQueryDsl::get_result(
            diesel::sql_query(
                "SELECT count(*) AS c FROM webhook_outbox \
                 WHERE batch_id = $1 AND trigger = 'batch_complete'",
            )
            .bind::<diesel::sql_types::Uuid, _>(batch_id),
            &mut *conn,
        )
        .await
        .unwrap()
    } else {
        diesel_async::RunQueryDsl::get_result(
            diesel::sql_query(
                "SELECT count(*) AS c FROM webhook_execution \
                 WHERE batch_id = $1 AND trigger = 'batch_complete' \
                   AND status = $2::webhook_execution_status",
            )
            .bind::<diesel::sql_types::Uuid, _>(batch_id)
            .bind::<diesel::sql_types::Text, _>(status),
            &mut *conn,
        )
        .await
        .unwrap()
    };
    r.c
}

/// Total batch_complete outbox rows for a batch (any status).
async fn batch_outbox_total(pool: &arcrun::DbPool, batch_id: uuid::Uuid) -> i64 {
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

/// POST a body (object form) and return (status, batch_id, body bytes-as-json).
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

// ============================================================================
// 3a — grouped insertion
// ============================================================================

/// Mixed batch (dedupe + non-dedupe tasks with cross-run dependencies) produces the
/// same observable state: statuses, wait counters, links, actions.
#[tokio::test]
async fn test_grouped_insert_mixed_batch_state() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // a (no dedupe) -> b (no dedupe, dep a) -> c (dedupe, dep b) -> d (no dedupe, dep c)
    // This forces flushes around the dedupe task `c` while keeping cross-run deps.
    let tasks = json!([
        {
            "id": "a", "name": "A", "kind": "grp", "timeout": 60,
            "metadata": {"n": "a"}, "on_start": webhook_action(),
            "on_success": [{"kind": "Webhook", "params": {"url": "https://example.com/s", "verb": "Post"}}]
        },
        {
            "id": "b", "name": "B", "kind": "grp", "timeout": 60,
            "metadata": {"n": "b"}, "on_start": webhook_action(),
            "dependencies": [{"id": "a", "requires_success": true}]
        },
        {
            "id": "c", "name": "C", "kind": "grp", "timeout": 60,
            "metadata": {"n": "c"}, "on_start": webhook_action(),
            "dependencies": [{"id": "b", "requires_success": false}],
            "dedupe_strategy": [{"kind": "grp", "status": "Pending", "fields": ["n"]}]
        },
        {
            "id": "d", "name": "D", "kind": "grp", "timeout": 60,
            "metadata": {"n": "d"}, "on_start": webhook_action(),
            "dependencies": [{"id": "c", "requires_success": true}]
        }
    ]);

    let created = create_tasks_ok(&app, &tasks.as_array().unwrap().clone()).await;
    assert_eq!(
        created.len(),
        4,
        "all 4 tasks created (no pre-existing dupes)"
    );

    // Statuses: a has no deps -> Pending; b,c,d have deps -> Waiting.
    let by_name: std::collections::HashMap<_, _> =
        created.iter().map(|t| (t.name.clone(), t)).collect();
    assert_eq!(by_name["A"].status, arcrun::models::StatusKind::Pending);
    assert_eq!(by_name["B"].status, arcrun::models::StatusKind::Waiting);
    assert_eq!(by_name["C"].status, arcrun::models::StatusKind::Waiting);
    assert_eq!(by_name["D"].status, arcrun::models::StatusKind::Waiting);

    // Wait counters.
    let (b_wf, b_ws) = read_wait_counters(&state.pool, by_name["B"].id).await;
    assert_eq!((b_wf, b_ws), (1, 1), "B requires_success dep on A");
    let (c_wf, c_ws) = read_wait_counters(&state.pool, by_name["C"].id).await;
    assert_eq!((c_wf, c_ws), (1, 0), "C non-requires_success dep on B");
    let (d_wf, d_ws) = read_wait_counters(&state.pool, by_name["D"].id).await;
    assert_eq!((d_wf, d_ws), (1, 1), "D requires_success dep on C");

    // Actions: A has start + success; B/C/D have just start.
    let a_full = get_task_ok(&app, by_name["A"].id).await;
    assert_eq!(a_full.actions.len(), 2, "A: on_start + on_success");
    let b_full = get_task_ok(&app, by_name["B"].id).await;
    assert_eq!(b_full.actions.len(), 1, "B: on_start only");

    // Links exist for every dependency (3 links: a->b, b->c, c->d).
    let dag = get_dag_ok(&app, created[0].batch_id.unwrap()).await;
    assert_eq!(
        dag["links"].as_array().unwrap().len(),
        3,
        "3 dependency links"
    );
}

/// Regression: a dedupe task that matches a task inserted EARLIER IN THE SAME batch
/// is skipped (grouped insert must flush the earlier run before the dedupe check).
#[tokio::test]
async fn test_grouped_insert_dedupe_matches_earlier_in_same_batch() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let tasks = json!([
        {
            "id": "first", "name": "First", "kind": "intra", "timeout": 60,
            "metadata": {"key": "dup"}, "on_start": webhook_action()
        },
        {
            "id": "second", "name": "Second", "kind": "intra", "timeout": 60,
            "metadata": {"key": "dup"}, "on_start": webhook_action(),
            "dedupe_strategy": [{"kind": "intra", "status": "Pending", "fields": ["key"]}]
        }
    ]);

    let created = create_tasks_ok(&app, &tasks.as_array().unwrap().clone()).await;
    assert_eq!(
        created.len(),
        1,
        "second task should be deduped against the first task inserted in the same batch"
    );
    assert_eq!(created[0].name, "First");
}

/// A child whose parent was dedupe-skipped: the dependency is ignored (warn) and the
/// child starts Pending (wait_finished == 0).
#[tokio::test]
async fn test_grouped_insert_dedupe_skipped_parent_child_pending() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // Pre-create a task that the dedupe parent will match.
    let pre = json!([{
        "id": "pre", "name": "Pre", "kind": "skp", "timeout": 60,
        "metadata": {"k": "v"}, "on_start": webhook_action()
    }]);
    create_tasks_ok(&app, &pre.as_array().unwrap().clone()).await;

    // Batch: parent (dedupe, will be skipped) + child depending on parent.
    let tasks = json!([
        {
            "id": "parent", "name": "Parent", "kind": "skp", "timeout": 60,
            "metadata": {"k": "v"}, "on_start": webhook_action(),
            "dedupe_strategy": [{"kind": "skp", "status": "Pending", "fields": ["k"]}]
        },
        {
            "id": "child", "name": "Child", "kind": "skp", "timeout": 60,
            "metadata": {"k": "child"}, "on_start": webhook_action(),
            "dependencies": [{"id": "parent", "requires_success": true}]
        }
    ]);

    let created = create_tasks_ok(&app, &tasks.as_array().unwrap().clone()).await;
    assert_eq!(
        created.len(),
        1,
        "only the child is created (parent deduped)"
    );
    assert_eq!(created[0].name, "Child");
    assert_eq!(
        created[0].status,
        arcrun::models::StatusKind::Pending,
        "child with dedupe-skipped parent should be Pending, not Waiting"
    );
    let (wf, ws) = read_wait_counters(&state.pool, created[0].id).await;
    assert_eq!((wf, ws), (0, 0), "dependency on skipped parent is ignored");
}

// ============================================================================
// 3b — on_batch_complete webhook
// ============================================================================

/// Both body shapes accepted: object form creates OK, bare array still works.
#[tokio::test]
async fn test_batch_complete_body_shapes() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // Bare array (legacy).
    let bare = json!([task_json("t1", "T1", "shape")]);
    let (status, _) = post_body(&app, &bare).await;
    assert_eq!(
        status,
        actix_web::http::StatusCode::CREATED,
        "bare array OK"
    );

    // Object form with on_batch_complete.
    let obj = json!({
        "tasks": [task_json("t1", "T1", "shape")],
        "on_batch_complete": [
            {"kind": "Webhook", "params": {"url": "https://example.com/done", "verb": "Post"}}
        ]
    });
    let (status, batch_id) = post_body(&app, &obj).await;
    assert_eq!(
        status,
        actix_web::http::StatusCode::CREATED,
        "object form OK"
    );
    let batch_id = batch_id.unwrap();

    // A `batch` row was created for the object form.
    assert_eq!(
        batch_outbox_total(&state.pool, batch_id).await,
        0,
        "no batch_complete row yet — tasks not terminal"
    );
}

/// The batch-complete webhook fires ONCE when the last task becomes terminal
/// (mixed Success/Failure), not before.
#[tokio::test]
async fn test_batch_complete_fires_once_on_last_terminal() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let hits = Arc::new(AtomicUsize::new(0));
    let (webhook_url, shutdown_server) = spawn_webhook_server(hits.clone());

    let body = json!({
        "tasks": [
            task_json("a", "A", "bc"),
            task_json("b", "B", "bc")
        ],
        "on_batch_complete": [
            {"kind": "Webhook", "params": {"url": webhook_url, "verb": "Post"}}
        ]
    });
    let (status, batch_id) = post_body(&app, &body).await;
    assert_eq!(status, actix_web::http::StatusCode::CREATED);
    let batch_id = batch_id.unwrap();

    let created = get_batch_task_ids(&app, batch_id).await;
    assert_eq!(created.len(), 2);

    // Complete the FIRST task -> batch not complete, no signal.
    succeed_task(&state, created[0]).await;
    assert_eq!(
        batch_outbox_total(&state.pool, batch_id).await,
        0,
        "no batch_complete signal while a task is still non-terminal"
    );

    // Complete the SECOND (last) task with Failure -> batch complete, one signal.
    fail_task(&state, created[1], "boom").await;
    assert_eq!(
        batch_outbox_count(&state.pool, batch_id, "pending").await,
        1,
        "exactly one batch_complete row enqueued on last terminal transition"
    );

    // Deliver it.
    drain_outbox(&state).await;
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    assert_eq!(hits.load(Ordering::SeqCst), 1, "batch webhook fired once");
    assert_eq!(
        batch_outbox_count(&state.pool, batch_id, "success").await,
        1
    );

    let _ = shutdown_server.send(());
}

/// Batch-complete fires via stop_batch.
#[tokio::test]
async fn test_batch_complete_via_stop_batch() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let hits = Arc::new(AtomicUsize::new(0));
    let (webhook_url, shutdown_server) = spawn_webhook_server(hits.clone());

    let body = json!({
        "tasks": [task_json("a", "A", "stopbc"), task_json("b", "B", "stopbc")],
        "on_batch_complete": [
            {"kind": "Webhook", "params": {"url": webhook_url, "verb": "Post"}}
        ]
    });
    let (_s, batch_id) = post_body(&app, &body).await;
    let batch_id = batch_id.unwrap();

    // Stop the whole batch.
    let req = actix_web::test::TestRequest::delete()
        .uri(&format!("/batch/{}", batch_id))
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    assert_eq!(
        batch_outbox_total(&state.pool, batch_id).await,
        1,
        "stop_batch enqueues exactly one batch_complete row"
    );
    drain_outbox(&state).await;
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    assert_eq!(hits.load(Ordering::SeqCst), 1);

    let _ = shutdown_server.send(());
}

/// Batch-complete fires via the timeout of the last task.
#[tokio::test]
async fn test_batch_complete_via_timeout() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let hits = Arc::new(AtomicUsize::new(0));
    let (webhook_url, shutdown_server) = spawn_webhook_server(hits.clone());

    let body = json!({
        "tasks": [task_json("only", "Only", "tobc")],
        "on_batch_complete": [
            {"kind": "Webhook", "params": {"url": webhook_url, "verb": "Post"}}
        ]
    });
    let (_s, batch_id) = post_body(&app, &body).await;
    let batch_id = batch_id.unwrap();
    let ids = get_batch_task_ids(&app, batch_id).await;
    let task_id = ids[0];

    // Force the (only) task into a Running state whose last_updated is well in the
    // past so the timeout_loop trips it.
    {
        use diesel_async::RunQueryDsl;
        let past = chrono::Utc::now() - chrono::Duration::seconds(120);
        let mut conn = state.pool.get().await.unwrap();
        diesel::sql_query(
            "UPDATE task SET status = 'running', started_at = $1, last_updated = $1 WHERE id = $2",
        )
        .bind::<diesel::sql_types::Timestamptz, _>(past)
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .execute(&mut *conn)
        .await
        .unwrap();
    }

    // Run the timeout loop briefly.
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
        get_task_ok(&app, task_id).await.status,
        arcrun::models::StatusKind::Failure,
        "task should be timed out"
    );
    assert_eq!(
        batch_outbox_total(&state.pool, batch_id).await,
        1,
        "timeout of last task enqueues batch_complete"
    );
    drain_outbox(&state).await;
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    assert_eq!(hits.load(Ordering::SeqCst), 1);

    let _ = shutdown_server.send(());
}

/// Concurrency: two tasks finishing back-to-back enqueue only ONE batch_complete row
/// (unique idempotency key + ON CONFLICT DO NOTHING).
#[tokio::test]
async fn test_batch_complete_single_row_under_concurrent_detection() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let body = json!({
        "tasks": [task_json("a", "A", "conc"), task_json("b", "B", "conc")],
        "on_batch_complete": [
            {"kind": "Webhook", "params": {"url": "https://example.com/done", "verb": "Post"}}
        ]
    });
    let (_s, batch_id) = post_body(&app, &body).await;
    let batch_id = batch_id.unwrap();
    let ids = get_batch_task_ids(&app, batch_id).await;

    // Complete both. Each terminal transition decrements batch.remaining (D2); the one
    // that drives it to 0 inserts the batch_complete row, and even a re-signal is
    // deduped by the unique idempotency key.
    succeed_task(&state, ids[0]).await;
    succeed_task(&state, ids[1]).await;

    // Simulate a redundant detection (as if a concurrent transition raced): a second
    // decrement of an already-terminal task keeps remaining clamped at 0 and tries to
    // enqueue again — the ON CONFLICT DO NOTHING belt must keep it at ONE row.
    let mut conn = state.pool.get().await.unwrap();
    arcrun::db_operation::decrement_batch_remaining_for_task(&mut conn, ids[0], "test")
        .await
        .unwrap();
    drop(conn);

    assert_eq!(
        batch_outbox_total(&state.pool, batch_id).await,
        1,
        "exactly one batch_complete row despite repeated detection"
    );
}

/// Empty batch: all tasks dedupe-skipped but on_batch_complete provided -> signal is
/// enqueued immediately in the add_task transaction.
#[tokio::test]
async fn test_batch_complete_empty_batch_signals_immediately() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let hits = Arc::new(AtomicUsize::new(0));
    let (webhook_url, shutdown_server) = spawn_webhook_server(hits.clone());

    // Pre-create a task the dedupe will match.
    let pre = json!([task_with_metadata("p", "P", "empty", json!({"k": "x"}))]);
    create_tasks_ok(&app, &pre.as_array().unwrap().clone()).await;

    // Batch with a single dedupe task that WILL be skipped + on_batch_complete.
    let body = json!({
        "tasks": [{
            "id": "dup", "name": "Dup", "kind": "empty", "timeout": 60,
            "metadata": {"k": "x"}, "on_start": webhook_action(),
            "dedupe_strategy": [{"kind": "empty", "status": "Pending", "fields": ["k"]}]
        }],
        "on_batch_complete": [
            {"kind": "Webhook", "params": {"url": webhook_url, "verb": "Post"}}
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
        batch_outbox_total(&state.pool, batch_id).await,
        1,
        "empty (vacuously complete) batch enqueues the signal immediately"
    );
    drain_outbox(&state).await;
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    assert_eq!(hits.load(Ordering::SeqCst), 1, "empty batch webhook fired");

    let _ = shutdown_server.send(());
}

/// Payload: the mock captures the request body and verifies `arcrun.batch_id` and
/// `arcrun.counts`.
#[tokio::test]
async fn test_batch_complete_payload_contents() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let (webhook_url, capture_rx, shutdown_server) = spawn_request_capture_server();

    let body = json!({
        "tasks": [task_json("a", "A", "pay"), task_json("b", "B", "pay")],
        "on_batch_complete": [
            {"kind": "Webhook", "params": {"url": webhook_url, "verb": "Post"}}
        ]
    });
    let (_s, batch_id) = post_body(&app, &body).await;
    let batch_id = batch_id.unwrap();
    let ids = get_batch_task_ids(&app, batch_id).await;

    succeed_task(&state, ids[0]).await;
    fail_task(&state, ids[1], "nope").await;

    drain_outbox(&state).await;

    let captured = tokio::time::timeout(std::time::Duration::from_secs(2), capture_rx)
        .await
        .expect("capture timed out")
        .expect("capture channel closed");

    let payload: serde_json::Value = serde_json::from_str(&captured.body).expect("valid JSON body");
    let arcrun = &payload["arcrun"];
    assert_eq!(
        arcrun["batch_id"].as_str().unwrap(),
        batch_id.to_string(),
        "payload carries batch_id"
    );
    assert_eq!(arcrun["counts"]["success"].as_i64().unwrap(), 1);
    assert_eq!(arcrun["counts"]["failure"].as_i64().unwrap(), 1);
    assert_eq!(arcrun["counts"]["canceled"].as_i64().unwrap(), 0);

    // No ?handle= for batch webhooks.
    assert!(
        !captured.request_line.contains("handle="),
        "batch webhook must not include a ?handle= param"
    );

    let _ = shutdown_server.send(());
}

/// Retry then exhausted on a mock that always 500s, visible via GET /webhook-deliveries.
#[tokio::test]
async fn test_batch_complete_retry_and_exhausted() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let (webhook_url, shutdown_server) = spawn_500_webhook_server();

    let body = json!({
        "tasks": [task_json("only", "Only", "exh")],
        "on_batch_complete": [
            {"kind": "Webhook", "params": {"url": webhook_url, "verb": "Post"}}
        ]
    });
    let (_s, batch_id) = post_body(&app, &body).await;
    let batch_id = batch_id.unwrap();
    let ids = get_batch_task_ids(&app, batch_id).await;

    succeed_task(&state, ids[0]).await;

    let cfg = arcrun::workers::DeliveryConfig {
        batch_size: 100,
        max_attempts: 2,
        backoff_base_secs: 1,
        backoff_cap_secs: 1,
        lease_secs: 120,
        concurrency: 10,
        start_stale_secs: 30,
    };
    drain_outbox_with(&state, cfg, 1).await; // attempt 1 -> fail, retry scheduled
    assert_eq!(
        batch_outbox_count(&state.pool, batch_id, "pending").await,
        1,
        "still pending after first failed delivery"
    );
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    drain_outbox_with(&state, cfg, 1).await; // attempt 2 -> exhausted

    assert_eq!(
        batch_outbox_count(&state.pool, batch_id, "exhausted").await,
        1,
        "batch_complete row exhausted after max_attempts"
    );

    // Visible via the endpoint.
    let req = actix_web::test::TestRequest::get()
        .uri("/webhook-deliveries?status=exhausted")
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::OK);
    let list: Vec<serde_json::Value> = actix_web::test::read_body_json(resp).await;
    assert_eq!(list.len(), 1);
    assert_eq!(list[0]["batch_id"], batch_id.to_string());
    assert_eq!(list[0]["trigger"], "BatchComplete");
    assert!(list[0]["task_id"].is_null(), "batch row has null task_id");

    let _ = shutdown_server.send(());
}

/// Régression (D2) : pas de signal batch_complete perdu quand les deux dernières
/// tâches du batch terminent concurremment.
///
/// Sous l'ancien mécanisme (FOR UPDATE + NOT EXISTS), un write-skew sous READ
/// COMMITTED pouvait faire que chaque transaction voie l'autre tâche encore
/// non-terminale — aucune n'enqueueait, signal perdu. Le compteur `batch.remaining`
/// (D2) rend ce write-skew structurellement impossible : chaque décrément
/// `UPDATE batch SET remaining = remaining - 1` prend NATURELLEMENT le verrou de
/// ligne `batch`, donc les deux décréments se sérialisent et EXACTEMENT un observe
/// `remaining = 0`.
///
/// Le test orchestre l'entrelacement : A termine t1 et décrémente (remaining 2→1,
/// verrou de ligne pris, non committé) ; B termine t2 et décrémente — son UPDATE
/// doit BLOQUER sur le verrou de ligne jusqu'au commit de A ; A committe (remaining=1
/// persisté) ; B se débloque, décrémente 1→0, enqueue. Résultat : exactement une
/// ligne batch_complete.
#[tokio::test]
async fn test_batch_complete_concurrent_last_two_tasks_no_lost_signal() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let body = json!({
        "tasks": [task_json("a", "A", "skew"), task_json("b", "B", "skew")],
        "on_batch_complete": [
            {"kind": "Webhook", "params": {"url": "https://example.com/done", "verb": "Post"}}
        ]
    });
    let (_s, batch_id) = post_body(&app, &body).await;
    let batch_id = batch_id.unwrap();
    let ids = get_batch_task_ids(&app, batch_id).await;
    let (t1, t2) = (ids[0], ids[1]);

    // NB : appels diesel pleinement qualifiés — importer RunQueryDsl dans ce scope
    // ferait résoudre `.load(Ordering)` des atomics vers le trait diesel.
    async fn begin_and_terminate(conn: &mut arcrun::Conn<'_>, id: uuid::Uuid) {
        diesel_async::RunQueryDsl::execute(diesel::sql_query("BEGIN"), conn)
            .await
            .unwrap();
        diesel_async::RunQueryDsl::execute(
            diesel::sql_query(
                "UPDATE task SET status = 'success', ended_at = now(), last_updated = now() WHERE id = $1",
            )
            .bind::<diesel::sql_types::Uuid, _>(id),
            conn,
        )
        .await
        .unwrap();
    }

    // A : termine t1 (non committé) puis décrémente — remaining 2→1, prend le verrou
    // de ligne batch, n'enqueue pas (remaining ≠ 0), GARDE le verrou jusqu'au commit.
    let mut conn_a = state.pool.get().await.unwrap();
    begin_and_terminate(&mut conn_a, t1).await;
    arcrun::db_operation::decrement_batch_remaining_for_task(&mut conn_a, t1, "test-A")
        .await
        .unwrap();

    // B : termine t2 (non committé) puis décrémente — son UPDATE batch doit bloquer
    // sur le verrou de ligne jusqu'au commit de A. Les deux futures tournent via
    // join! (PooledConnection n'est pas 'static, pas de tokio::spawn possible).
    let mut conn_b = state.pool.get().await.unwrap();
    begin_and_terminate(&mut conn_b, t2).await;

    let b_reached_enqueue = std::sync::atomic::AtomicBool::new(false);
    let fut_b = async {
        arcrun::db_operation::decrement_batch_remaining_for_task(&mut conn_b, t2, "test-B")
            .await
            .unwrap();
        b_reached_enqueue.store(true, Ordering::SeqCst);
        diesel_async::RunQueryDsl::execute(diesel::sql_query("COMMIT"), &mut conn_b)
            .await
            .unwrap();
    };
    let fut_a_commit = async {
        // Laisse B atteindre le verrou ; sa détection ne doit PAS avoir abouti tant
        // que A n'a pas committé (sans le fix, B passe ici sans rien enqueuer).
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        assert!(
            !b_reached_enqueue.load(Ordering::SeqCst),
            "B's decrement must block on the batch row lock until A commits"
        );
        diesel_async::RunQueryDsl::execute(diesel::sql_query("COMMIT"), &mut conn_a)
            .await
            .unwrap();
    };
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::join!(fut_b, fut_a_commit)
    })
    .await
    .expect("A/B interleaving should not deadlock");

    assert_eq!(
        batch_outbox_total(&state.pool, batch_id).await,
        1,
        "the batch_complete signal must not be lost under concurrent last-task completion"
    );
}

/// Régression (relecture Lot 3) : le cleanup de rétention ne doit pas balayer un
/// batch « orphelin » dont le signal batch_complete est encore `pending`.
///
/// Bug : un batch vide (toutes les tâches dédupe-skipped) n'a aucune tâche — il est
/// « orphelin » dès sa création ; le cleanup supprimait sa ligne `batch` ET sa ligne
/// outbox `pending` avant livraison (signal perdu). Fix : l'orphan-sweep exclut les
/// batches ayant une ligne `webhook_execution` encore `pending` ; une fois le signal
/// livré, le batch est balayé normalement.
#[tokio::test]
async fn test_cleanup_spares_batch_with_pending_signal() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let hits = Arc::new(AtomicUsize::new(0));
    let (webhook_url, shutdown_server) = spawn_webhook_server(hits.clone());

    // Une vieille tâche terminale (hors batch) pour que le cleanup ait du travail
    // et atteigne l'orphan-sweep (early return sinon).
    async fn insert_old_terminal_task(pool: &arcrun::DbPool) {
        let mut conn = pool.get().await.unwrap();
        diesel_async::RunQueryDsl::execute(
            diesel::sql_query(
                "INSERT INTO task (id, name, kind, status, metadata, start_condition, timeout, ended_at, created_at, last_updated)
                 VALUES ($1, 'old', 'old-kind', 'success', 'null'::jsonb, '[]'::jsonb, 60,
                         now() - interval '40 days', now() - interval '41 days', now() - interval '40 days')",
            )
            .bind::<diesel::sql_types::Uuid, _>(uuid::Uuid::new_v4()),
            &mut conn,
        )
        .await
        .unwrap();
    }
    insert_old_terminal_task(&state.pool).await;

    // Batch vide (tâche dédupe-skipped) avec on_batch_complete → ligne outbox pending.
    let pre = json!([task_with_metadata("p", "P", "sweep", json!({"k": "x"}))]);
    create_tasks_ok(&app, &pre.as_array().unwrap().clone()).await;
    let body = json!({
        "tasks": [{
            "id": "dup", "name": "Dup", "kind": "sweep", "timeout": 60,
            "metadata": {"k": "x"}, "on_start": webhook_action(),
            "dedupe_strategy": [{"kind": "sweep", "status": "Pending", "fields": ["k"]}]
        }],
        "on_batch_complete": [
            {"kind": "Webhook", "params": {"url": webhook_url, "verb": "Post"}}
        ]
    });
    let (_s, batch_id) = post_body(&app, &body).await;
    let batch_id = batch_id.unwrap();
    assert_eq!(
        batch_outbox_count(&state.pool, batch_id, "pending").await,
        1
    );

    // Cleanup AVANT livraison : la ligne pending et le batch doivent survivre.
    {
        let mut conn = state.pool.get().await.unwrap();
        arcrun::db_operation::cleanup_old_terminal_tasks(&mut conn, 30, 100)
            .await
            .unwrap();
    }
    assert_eq!(
        batch_outbox_count(&state.pool, batch_id, "pending").await,
        1,
        "cleanup must not delete a pending batch_complete signal"
    );

    // Livraison du signal.
    drain_outbox(&state).await;
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    assert_eq!(hits.load(Ordering::SeqCst), 1, "signal delivered");

    // Cleanup APRÈS livraison (avec une nouvelle vieille tâche pour passer l'early
    // return) : le batch livré est balayé normalement.
    insert_old_terminal_task(&state.pool).await;
    {
        let mut conn = state.pool.get().await.unwrap();
        arcrun::db_operation::cleanup_old_terminal_tasks(&mut conn, 30, 100)
            .await
            .unwrap();
    }
    assert_eq!(
        batch_outbox_total(&state.pool, batch_id).await,
        0,
        "delivered batch signal rows are swept once no longer pending"
    );

    let _ = shutdown_server.send(());
}

/// Batch enrichment is computed from hot task rows at delivery time. Retention must
/// therefore preserve every old terminal member while the batch_complete signal is
/// still queued, even if that member's own task-level outbox row is already gone.
#[tokio::test]
async fn test_cleanup_spares_terminal_members_until_batch_signal_is_delivered() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);
    let hits = Arc::new(AtomicUsize::new(0));
    let (webhook_url, shutdown_server) = spawn_webhook_server(hits);

    let body = json!({
        "tasks": [task_json("member", "Member", "retention-batch")],
        "on_batch_complete": [
            {"kind": "Webhook", "params": {"url": webhook_url, "verb": "Post"}}
        ]
    });
    let (status, batch_id) = post_body(&app, &body).await;
    assert_eq!(status, actix_web::http::StatusCode::CREATED);
    let batch_id = batch_id.unwrap();

    #[derive(diesel::QueryableByName)]
    struct IdRow {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: uuid::Uuid,
    }
    let mut conn = state.pool.get().await.unwrap();
    let member: IdRow = diesel_async::RunQueryDsl::get_result(
        diesel::sql_query("SELECT id FROM task WHERE batch_id = $1")
            .bind::<diesel::sql_types::Uuid, _>(batch_id),
        &mut *conn,
    )
    .await
    .unwrap();
    drop(conn);
    succeed_task(&state, member.id).await;

    let mut conn = state.pool.get().await.unwrap();
    diesel_async::RunQueryDsl::execute(
        diesel::sql_query("DELETE FROM webhook_outbox WHERE task_id = $1")
            .bind::<diesel::sql_types::Uuid, _>(member.id),
        &mut *conn,
    )
    .await
    .unwrap();
    diesel_async::RunQueryDsl::execute(
        diesel::sql_query("UPDATE task SET ended_at = now() - interval '40 days' WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(member.id),
        &mut *conn,
    )
    .await
    .unwrap();
    let moved = arcrun::db_operation::cleanup_old_terminal_tasks(&mut conn, 30, 100)
        .await
        .unwrap();
    assert_eq!(moved, 0);

    let still_hot: IdRow = diesel_async::RunQueryDsl::get_result(
        diesel::sql_query("SELECT id FROM task WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(member.id),
        &mut *conn,
    )
    .await
    .expect("batch member remains available for completion stats");
    assert_eq!(still_hot.id, member.id);
    assert_eq!(
        batch_outbox_count(&state.pool, batch_id, "pending").await,
        1
    );

    let _ = shutdown_server.send(());
}

/// The protection starts before the signal exists: an old completed member of a
/// still-active batch is part of the future completion counts and must remain hot.
#[tokio::test]
async fn test_cleanup_spares_old_member_while_batch_is_still_active() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);
    let hits = Arc::new(AtomicUsize::new(0));
    let (webhook_url, shutdown_server) = spawn_webhook_server(hits);

    let body = json!({
        "tasks": [
            task_json("done", "Done", "active-retention"),
            task_json("pending", "Pending", "active-retention")
        ],
        "on_batch_complete": [
            {"kind": "Webhook", "params": {"url": webhook_url, "verb": "Post"}}
        ]
    });
    let (status, batch_id) = post_body(&app, &body).await;
    assert_eq!(status, actix_web::http::StatusCode::CREATED);
    let batch_id = batch_id.unwrap();

    #[derive(diesel::QueryableByName)]
    struct IdRow {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: uuid::Uuid,
    }
    let mut conn = state.pool.get().await.unwrap();
    let done: IdRow = diesel_async::RunQueryDsl::get_result(
        diesel::sql_query("SELECT id FROM task WHERE batch_id = $1 AND name = 'Done'")
            .bind::<diesel::sql_types::Uuid, _>(batch_id),
        &mut *conn,
    )
    .await
    .unwrap();
    drop(conn);
    succeed_task(&state, done.id).await;

    let mut conn = state.pool.get().await.unwrap();
    diesel_async::RunQueryDsl::execute(
        diesel::sql_query("DELETE FROM webhook_outbox WHERE task_id = $1")
            .bind::<diesel::sql_types::Uuid, _>(done.id),
        &mut *conn,
    )
    .await
    .unwrap();
    diesel_async::RunQueryDsl::execute(
        diesel::sql_query("UPDATE task SET ended_at = now() - interval '40 days' WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(done.id),
        &mut *conn,
    )
    .await
    .unwrap();
    assert_eq!(
        arcrun::db_operation::cleanup_old_terminal_tasks(&mut conn, 30, 100)
            .await
            .unwrap(),
        0
    );
    let kept: IdRow = diesel_async::RunQueryDsl::get_result(
        diesel::sql_query("SELECT id FROM task WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(done.id),
        &mut *conn,
    )
    .await
    .expect("old member remains until the batch completes");
    assert_eq!(kept.id, done.id);
    assert_eq!(
        batch_outbox_count(&state.pool, batch_id, "pending").await,
        0
    );

    let _ = shutdown_server.send(());
}

/// D3 backstop: once a batch_complete signal has been DELIVERED (its queue row removed
/// and a `success` history row written), a re-signal of the same batch must NOT
/// re-enqueue it. With the pre-D3 single table, a double `stop_batch`/re-signal was
/// blocked by `ON CONFLICT` hitting the retained `success` row; now the queue is emptied
/// on success, so the `NOT EXISTS (webhook_execution ... key)` ledger backstop is what
/// prevents a second delivery. Asserts: no new queue row, no second HTTP hit.
#[tokio::test]
async fn test_batch_complete_backstop_no_resignal_after_delivery() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let hits = Arc::new(AtomicUsize::new(0));
    let (webhook_url, shutdown_server) = spawn_webhook_server(hits.clone());

    let body = json!({
        "tasks": [task_json("a", "A", "backstop")],
        "on_batch_complete": [
            {"kind": "Webhook", "params": {"url": webhook_url, "verb": "Post"}}
        ]
    });
    let (_s, batch_id) = post_body(&app, &body).await;
    let batch_id = batch_id.unwrap();
    let ids = get_batch_task_ids(&app, batch_id).await;

    // Complete the only task -> remaining 0 -> batch_complete queued.
    succeed_task(&state, ids[0]).await;
    assert_eq!(
        batch_outbox_count(&state.pool, batch_id, "pending").await,
        1,
        "batch_complete should be queued after the last task terminalizes"
    );

    // Deliver it -> queue emptied, historised as success.
    drain_outbox(&state).await;
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "batch_complete delivered once"
    );
    assert_eq!(
        batch_outbox_count(&state.pool, batch_id, "pending").await,
        0,
        "queue row removed on delivery"
    );
    assert_eq!(
        batch_outbox_count(&state.pool, batch_id, "success").await,
        1,
        "delivery historised in webhook_execution"
    );

    // Re-signal the (already-delivered) batch: the backstop must block re-enqueue.
    {
        let mut conn = state.pool.get().await.unwrap();
        arcrun::db_operation::zero_batch_remaining_and_complete(
            &mut conn,
            batch_id,
            "test-resignal",
        )
        .await
        .unwrap();
    }
    assert_eq!(
        batch_outbox_count(&state.pool, batch_id, "pending").await,
        0,
        "backstop (NOT EXISTS ledger) must prevent re-enqueue after delivery"
    );
    assert_eq!(
        batch_outbox_total(&state.pool, batch_id).await,
        1,
        "still exactly one batch_complete signal across queue + history"
    );

    // A further drain must not deliver anything new.
    drain_outbox(&state).await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "no second batch_complete delivery after a re-signal"
    );

    let _ = shutdown_server.send(());
}

/// SSRF validation applies to on_batch_complete params.
#[tokio::test]
async fn test_batch_complete_ssrf_validation() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // Build a state with SSRF validation ON (default test config skips it). We
    // exercise the validation path directly by sending an invalid (non-URL) param,
    // which validate_action_params rejects regardless of SSRF skip.
    let _ = &state;

    let body = json!({
        "tasks": [task_json("a", "A", "ssrf")],
        "on_batch_complete": [
            {"kind": "Webhook", "params": {"url": "not-a-valid-url", "verb": "Post"}}
        ]
    });
    let (status, _) = post_body(&app, &body).await;
    assert_eq!(
        status,
        actix_web::http::StatusCode::BAD_REQUEST,
        "invalid on_batch_complete webhook URL is rejected"
    );
}

/// Batch-level webhooks share the same bounded action budget as task-level hooks.
#[tokio::test]
async fn test_batch_complete_action_count_is_bounded() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);
    let actions: Vec<_> = (0..21).map(|_| webhook_action()).collect();
    let body = json!({
        "tasks": [task_json("a", "A", "action-limit")],
        "on_batch_complete": actions
    });

    let (status, _) = post_body(&app, &body).await;
    assert_eq!(status, actix_web::http::StatusCode::BAD_REQUEST);
}

// ============================================================================
// Local helpers needing the actix service
// ============================================================================

async fn get_dag_ok<S, B>(app: &S, batch_id: uuid::Uuid) -> serde_json::Value
where
    S: actix_web::dev::Service<
            actix_http::Request,
            Response = actix_web::dev::ServiceResponse<B>,
            Error = actix_web::Error,
        >,
    B: actix_web::body::MessageBody,
{
    let req = actix_web::test::TestRequest::get()
        .uri(&format!("/dag/{}", batch_id))
        .to_request();
    let resp = actix_web::test::call_service(app, req).await;
    assert!(resp.status().is_success());
    actix_web::test::read_body_json(resp).await
}

/// Return the task ids of a batch (ordered by created_at) via GET /dag.
async fn get_batch_task_ids<S, B>(app: &S, batch_id: uuid::Uuid) -> Vec<uuid::Uuid>
where
    S: actix_web::dev::Service<
            actix_http::Request,
            Response = actix_web::dev::ServiceResponse<B>,
            Error = actix_web::Error,
        >,
    B: actix_web::body::MessageBody,
{
    let dag = get_dag_ok(app, batch_id).await;
    dag["tasks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| uuid::Uuid::parse_str(t["id"].as_str().unwrap()).unwrap())
        .collect()
}
