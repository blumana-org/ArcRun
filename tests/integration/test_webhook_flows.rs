use crate::common::*;

use arcrun::models::StatusKind;
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

/// Priority 2 — Webhook flow integration tests.
/// Tests on_success, on_failure (via PATCH), and start_loop E2E.
///
/// Test on_success webhook fires when task completes successfully.
#[tokio::test]
async fn test_on_success_webhook_fires() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let hits = Arc::new(AtomicUsize::new(0));
    let (webhook_url, shutdown_server) = spawn_webhook_server(hits.clone());

    let task_payload = json!({
        "id": "success-wh",
        "name": "Success Webhook Task",
        "kind": "success-wh-kind",
        "timeout": 60,
        "metadata": {"test": true},
        "on_start": webhook_action(),
        "on_success": [{
            "kind": "Webhook",
            "params": {
                "url": webhook_url,
                "verb": "Post"
            }
        }]
    });

    let created = create_tasks_ok(&app, &[task_payload]).await;
    let task_id = created[0].id;

    // Complete the task as Success (claim -> running -> success). The on_success
    // webhook is now enqueued into the transactional outbox; drive the delivery
    // loop to deliver it.
    succeed_task(&state, task_id).await;
    drain_outbox(&state).await;

    // Give webhook a moment to fire
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let hit_count = hits.load(Ordering::SeqCst);
    assert_eq!(
        hit_count, 1,
        "on_success webhook should be called exactly once"
    );

    let _ = shutdown_server.send(());
}

/// Test on_failure webhook fires when task is explicitly marked as Failure via PATCH.
#[tokio::test]
async fn test_on_failure_webhook_fires_on_explicit_failure() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let hits = Arc::new(AtomicUsize::new(0));
    let (webhook_url, shutdown_server) = spawn_webhook_server(hits.clone());

    let task_payload = json!({
        "id": "failure-wh",
        "name": "Failure Webhook Task",
        "kind": "failure-wh-kind",
        "timeout": 60,
        "metadata": {"test": true},
        "on_start": webhook_action(),
        "on_failure": [{
            "kind": "Webhook",
            "params": {
                "url": webhook_url,
                "verb": "Post"
            }
        }]
    });

    let created = create_tasks_ok(&app, &[task_payload]).await;
    let task_id = created[0].id;

    // Mark task as Failure via claim_and_complete, then drain the outbox so the
    // on_failure notification is delivered.
    fail_task(&state, task_id, "explicit test failure").await;
    drain_outbox(&state).await;

    // Give webhook a moment to fire
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let hit_count = hits.load(Ordering::SeqCst);
    assert_eq!(
        hit_count, 1,
        "on_failure webhook should be called on explicit failure"
    );

    let _ = shutdown_server.send(());
}

/// Test start_loop E2E: task goes Pending -> Claimed -> Running via start_loop,
/// and on_start webhook is called.
#[tokio::test]
async fn test_start_loop_e2e_happy_path() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let hits = Arc::new(AtomicUsize::new(0));
    let (webhook_url, shutdown_server) = spawn_webhook_server(hits.clone());

    let task_payload = json!({
        "id": "start-e2e",
        "name": "Start E2E Task",
        "kind": "start-e2e-kind",
        "timeout": 60,
        "metadata": {"test": true},
        "on_start": {
            "kind": "Webhook",
            "params": {
                "url": webhook_url,
                "verb": "Post"
            }
        }
    });

    let created = create_tasks_ok(&app, &[task_payload]).await;
    let task_id = created[0].id;
    assert_eq!(created[0].status, StatusKind::Pending);

    // Run start_loop
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let evaluator = state.action_executor.clone();
    let pool = state.pool.clone();
    let handle = tokio::spawn(async move {
        arcrun::workers::start_loop(
            &evaluator,
            pool,
            std::time::Duration::from_millis(50),
            true,
            50,
            10,
            shutdown_rx,
            arcrun::workers::WorkerNudges::new(),
            std::time::Duration::from_secs(30),
        )
        .await;
    });

    // Wait for the task to be picked up
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let _ = shutdown_tx.send(true);
    let _ = handle.await;

    // Verify on_start was called
    let hit_count = hits.load(Ordering::SeqCst);
    assert_eq!(hit_count, 1, "on_start webhook should be called once");

    // Verify task is now Running
    assert_task_status(
        &app,
        task_id,
        StatusKind::Running,
        "task should be Running after start_loop",
    )
    .await;

    let _ = shutdown_server.send(());
}

/// Test PATCH /task/{id} with metadata-only update (no status change).
#[tokio::test]
async fn test_metadata_only_update() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let tasks = vec![task_json("meta-update", "Metadata Update", "meta-kind")];
    let created = create_tasks_ok(&app, &tasks).await;
    let task_id = created[0].id;

    // Move to Running
    let mut conn = state.pool.get().await.unwrap();
    arcrun::db_operation::claim_task(&mut conn, &task_id)
        .await
        .unwrap();
    arcrun::db_operation::mark_task_running(&mut conn, &task_id)
        .await
        .unwrap();
    drop(conn);

    let original = get_task_ok(&app, task_id).await;
    let original_last_updated = original.last_updated;

    // Small delay so last_updated will differ
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Update only metadata (no status)
    let update_req = actix_web::test::TestRequest::patch()
        .uri(&format!("/task/{}", task_id))
        .set_json(json!({
            "metadata": {"new_key": "new_value", "test": true}
        }))
        .to_request();
    let update_resp = actix_web::test::call_service(&app, update_req).await;
    assert!(
        update_resp.status().is_success(),
        "metadata-only update should return 200"
    );

    let updated = get_task_ok(&app, task_id).await;
    assert_eq!(
        updated.status,
        StatusKind::Running,
        "status should be unchanged"
    );
    assert_eq!(
        updated.metadata["new_key"], "new_value",
        "metadata should be updated"
    );
    assert!(
        updated.last_updated >= original_last_updated,
        "last_updated should be refreshed"
    );
}

/// Test that a webhook returning 500 triggers the on_start failure path.
#[tokio::test]
async fn test_webhook_500_triggers_failure() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let (webhook_url, shutdown_server) = spawn_500_webhook_server();

    let task_payload = json!({
        "id": "wh-500",
        "name": "Webhook 500 Task",
        "kind": "wh-500-kind",
        "timeout": 60,
        "metadata": {"test": true},
        "on_start": {
            "kind": "Webhook",
            "params": {
                "url": webhook_url,
                "verb": "Post"
            }
        }
    });

    let created = create_tasks_ok(&app, &[task_payload]).await;
    let task_id = created[0].id;

    // Run start_loop — it should call the webhook, get 500, and mark task as Failed
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let evaluator = state.action_executor.clone();
    let pool = state.pool.clone();
    let handle = tokio::spawn(async move {
        arcrun::workers::start_loop(
            &evaluator,
            pool,
            std::time::Duration::from_millis(50),
            true,
            50,
            10,
            shutdown_rx,
            arcrun::workers::WorkerNudges::new(),
            std::time::Duration::from_secs(30),
        )
        .await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let _ = shutdown_tx.send(true);
    let _ = handle.await;
    let _ = shutdown_server.send(());

    let task = get_task_ok(&app, task_id).await;
    assert_eq!(
        task.status,
        StatusKind::Failure,
        "task should be Failed after webhook returns 500"
    );
    assert!(
        task.failure_reason
            .as_deref()
            .unwrap_or("")
            .contains("on_start"),
        "failure_reason should mention on_start"
    );
}

/// Audit #7: Verify webhook receives `?handle=<host>/task/<uuid>` query param
/// and diagnostic headers (X-Task-Id, X-Task-Trigger).
#[tokio::test]
async fn test_webhook_receives_handle_query_param() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let (webhook_url, capture_rx, shutdown_server) = spawn_request_capture_server();

    let task_payload = json!({
        "id": "handle-qp",
        "name": "Handle Query Param Task",
        "kind": "handle-qp-kind",
        "timeout": 60,
        "metadata": {"test": true},
        "on_start": {
            "kind": "Webhook",
            "params": {
                "url": webhook_url,
                "verb": "Post"
            }
        }
    });

    let created = create_tasks_ok(&app, &[task_payload]).await;
    let task_id = created[0].id;

    // Run start_loop to trigger on_start webhook
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let evaluator = state.action_executor.clone();
    let pool = state.pool.clone();
    let handle = tokio::spawn(async move {
        arcrun::workers::start_loop(
            &evaluator,
            pool,
            std::time::Duration::from_millis(50),
            true,
            50,
            10,
            shutdown_rx,
            arcrun::workers::WorkerNudges::new(),
            std::time::Duration::from_secs(30),
        )
        .await;
    });

    // Await captured request with timeout
    let captured = tokio::time::timeout(std::time::Duration::from_secs(2), capture_rx)
        .await
        .expect("timed out waiting for webhook request")
        .expect("capture channel closed");

    let _ = shutdown_tx.send(true);
    let _ = handle.await;
    let _ = shutdown_server.send(());

    // Assert request line contains handle= with the task UUID
    // reqwest URL-encodes the query value: handle=http%3A%2F%2Flocalhost%3A8080%2Ftask%2F<uuid>
    assert!(
        captured.request_line.contains("handle="),
        "request line should contain handle= query param, got: {}",
        captured.request_line
    );
    let task_id_str = task_id.to_string();
    assert!(
        captured.request_line.contains(&task_id_str),
        "request line should contain the task UUID {}, got: {}",
        task_id_str,
        captured.request_line
    );

    // Assert diagnostic headers
    assert_eq!(
        captured.headers.get("x-task-id").map(|s| s.as_str()),
        Some(task_id_str.as_str()),
        "X-Task-Id header should match task UUID"
    );
    assert_eq!(
        captured.headers.get("x-task-trigger").map(|s| s.as_str()),
        Some("start"),
        "X-Task-Trigger header should be 'start'"
    );
}

/// Audit #21: Verify custom headers and body are forwarded to webhook endpoint.
#[tokio::test]
async fn test_webhook_custom_headers_and_body() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let (webhook_url, capture_rx, shutdown_server) = spawn_request_capture_server();

    let task_payload = json!({
        "id": "custom-hdr",
        "name": "Custom Headers Task",
        "kind": "custom-hdr-kind",
        "timeout": 60,
        "metadata": {"test": true},
        "on_start": {
            "kind": "Webhook",
            "params": {
                "url": webhook_url,
                "verb": "Post",
                "headers": {
                    "X-Custom": "test-value",
                    "Authorization": "Bearer tok123"
                },
                "body": {
                    "foo": "bar",
                    "nested": {"key": "val"}
                }
            }
        }
    });

    let created = create_tasks_ok(&app, &[task_payload]).await;
    let task_id = created[0].id;

    // Run start_loop to trigger on_start webhook
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let evaluator = state.action_executor.clone();
    let pool = state.pool.clone();
    let handle = tokio::spawn(async move {
        arcrun::workers::start_loop(
            &evaluator,
            pool,
            std::time::Duration::from_millis(50),
            true,
            50,
            10,
            shutdown_rx,
            arcrun::workers::WorkerNudges::new(),
            std::time::Duration::from_secs(30),
        )
        .await;
    });

    // Await captured request with timeout
    let captured = tokio::time::timeout(std::time::Duration::from_secs(2), capture_rx)
        .await
        .expect("timed out waiting for webhook request")
        .expect("capture channel closed");

    let _ = shutdown_tx.send(true);
    let _ = handle.await;
    let _ = shutdown_server.send(());

    // Assert custom headers are forwarded
    assert_eq!(
        captured.headers.get("x-custom").map(|s| s.as_str()),
        Some("test-value"),
        "X-Custom header should be forwarded"
    );
    assert_eq!(
        captured.headers.get("authorization").map(|s| s.as_str()),
        Some("Bearer tok123"),
        "Authorization header should be forwarded"
    );

    // Assert body is correct JSON
    let body: serde_json::Value =
        serde_json::from_str(&captured.body).expect("webhook body should be valid JSON");
    assert_eq!(body["foo"], "bar", "body.foo should be 'bar'");
    assert_eq!(
        body["nested"]["key"], "val",
        "body.nested.key should be 'val'"
    );

    // Verify task transitioned to Running (webhook succeeded)
    assert_task_status(
        &app,
        task_id,
        StatusKind::Running,
        "task should be Running after successful webhook with custom headers/body",
    )
    .await;
}

/// Audit #22: Verify webhook returning 3xx redirect is treated as failure (SSRF protection).
/// The ActionExecutor uses `redirect(Policy::none())` and explicitly checks `status.is_redirection()`.
#[tokio::test]
async fn test_webhook_redirect_rejected() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let (webhook_url, shutdown_server) = spawn_302_redirect_server();

    let task_payload = json!({
        "id": "wh-302",
        "name": "Webhook 302 Task",
        "kind": "wh-302-kind",
        "timeout": 60,
        "metadata": {"test": true},
        "on_start": {
            "kind": "Webhook",
            "params": {
                "url": webhook_url,
                "verb": "Post"
            }
        }
    });

    let created = create_tasks_ok(&app, &[task_payload]).await;
    let task_id = created[0].id;

    // Run start_loop — it should call the webhook, get 302, and mark task as Failed
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let evaluator = state.action_executor.clone();
    let pool = state.pool.clone();
    let handle = tokio::spawn(async move {
        arcrun::workers::start_loop(
            &evaluator,
            pool,
            std::time::Duration::from_millis(50),
            true,
            50,
            10,
            shutdown_rx,
            arcrun::workers::WorkerNudges::new(),
            std::time::Duration::from_secs(30),
        )
        .await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let _ = shutdown_tx.send(true);
    let _ = handle.await;
    let _ = shutdown_server.send(());

    let task = get_task_ok(&app, task_id).await;
    assert_eq!(
        task.status,
        StatusKind::Failure,
        "task should be Failed after webhook returns 302 redirect"
    );
    assert!(
        task.failure_reason
            .as_deref()
            .unwrap_or("")
            .contains("on_start"),
        "failure_reason should mention on_start, got: {:?}",
        task.failure_reason
    );
}
