use crate::common::*;

use arcrun::models::StatusKind;
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Priority 1 — Cancel webhook flow end-to-end
/// Tests that on_start can return a cancel action, and canceling the task fires the cancel webhook.
///
/// Test canceling a Running task (DELETE /task/{id} on Running).
/// The cancel webhook should fire and children should be propagated.
#[tokio::test]
async fn test_cancel_running_task() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let tasks = vec![
        task_json("cancel-run-parent", "Cancel Running Parent", "cancel-run"),
        task_with_deps(
            "cancel-run-child",
            "Cancel Running Child",
            "cancel-run",
            vec![("cancel-run-parent", true)],
        ),
    ];

    let created = create_tasks_ok(&app, &tasks).await;
    let parent_id = created[0].id;
    let child_id = created[1].id;

    // Move parent to Running
    let mut conn = state.pool.get().await.unwrap();
    arcrun::db_operation::claim_task(&mut conn, &parent_id)
        .await
        .unwrap();
    arcrun::db_operation::mark_task_running(&mut conn, &parent_id)
        .await
        .unwrap();
    drop(conn);

    assert_task_status(
        &app,
        parent_id,
        StatusKind::Running,
        "parent should be running",
    )
    .await;

    // Cancel the running parent
    let cancel_req = actix_web::test::TestRequest::delete()
        .uri(&format!("/task/{}", parent_id))
        .to_request();
    let cancel_resp = actix_web::test::call_service(&app, cancel_req).await;
    assert!(
        cancel_resp.status().is_success(),
        "cancel running task should succeed"
    );

    assert_task_status(
        &app,
        parent_id,
        StatusKind::Canceled,
        "parent should be canceled",
    )
    .await;

    // Child with requires_success=true should be marked as Failure
    let child = get_task_ok(&app, child_id).await;
    assert_eq!(
        child.status,
        StatusKind::Failure,
        "child should fail when running parent is canceled"
    );
    assert!(child.failure_reason.is_some());
}

/// Test that on_start returning a cancel action saves it, and that canceling fires the cancel webhook.
#[tokio::test]
async fn test_cancel_webhook_fires_after_on_start_registers_cancel_action() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // Set up a webhook server that returns a cancel action in the on_start response
    let cancel_hits = Arc::new(AtomicUsize::new(0));
    let (cancel_url, cancel_shutdown) = spawn_webhook_server(cancel_hits.clone());

    let (start_url, start_shutdown) = spawn_server_returning_cancel_action(&cancel_url);

    let task_payload = json!({
        "id": "cancel-wh-test",
        "name": "Cancel Webhook Test",
        "kind": "cancel-wh",
        "timeout": 60,
        "metadata": {"test": true},
        "on_start": {
            "kind": "Webhook",
            "params": {
                "url": start_url,
                "verb": "Post"
            }
        }
    });

    let created = create_tasks_ok(&app, &[task_payload]).await;
    let task_id = created[0].id;

    // Run start_loop to pick up the task and call on_start
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

    // Wait for start_loop to process the task
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let _ = shutdown_tx.send(true);
    let _ = handle.await;

    // Verify task is now Running (on_start succeeded)
    assert_task_status(
        &app,
        task_id,
        StatusKind::Running,
        "task should be running after start_loop",
    )
    .await;

    // Now cancel the running task — this should fire the cancel webhook
    let cancel_req = actix_web::test::TestRequest::delete()
        .uri(&format!("/task/{}", task_id))
        .to_request();
    let cancel_resp = actix_web::test::call_service(&app, cancel_req).await;
    assert!(cancel_resp.status().is_success());

    assert_task_status(
        &app,
        task_id,
        StatusKind::Canceled,
        "task should be canceled",
    )
    .await;

    // The cancel webhook is enqueued into the outbox; drive delivery.
    drain_outbox(&state).await;

    // Give the cancel webhook a moment to fire
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let hits = cancel_hits.load(Ordering::SeqCst);
    assert_eq!(
        hits, 1,
        "cancel webhook should have been called exactly once"
    );

    let _ = cancel_shutdown.send(());
    let _ = start_shutdown.send(());
}

/// Spawn a webhook server that responds to on_start with a NewActionDto (cancel action).
fn spawn_server_returning_cancel_action(
    cancel_url: &str,
) -> (String, tokio::sync::oneshot::Sender<()>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    let listener = tokio::net::TcpListener::from_std(listener).unwrap();

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let cancel_url = cancel_url.to_string();

    tokio::spawn(async move {
        loop {
            tokio::select! {
                result = listener.accept() => {
                    if let Ok((mut stream, _)) = result {
                        let cancel_url = cancel_url.clone();
                        tokio::spawn(async move {
                            let mut buf = [0u8; 4096];
                            let _ = stream.read(&mut buf).await;

                            let body = serde_json::json!({
                                "kind": "Webhook",
                                "params": {
                                    "url": cancel_url,
                                    "verb": "Post"
                                }
                            });
                            let body_str = serde_json::to_string(&body).unwrap();
                            let response = format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                                body_str.len(),
                                body_str
                            );
                            let _ = stream.write_all(response.as_bytes()).await;
                        });
                    }
                }
                _ = &mut shutdown_rx => break,
            }
        }
    });

    (format!("http://{}/start", addr), shutdown_tx)
}
