use crate::common::*;

use arcrun::dtos::StopBatchResponseDto;
use arcrun::models::StatusKind;

/// Priority 1 — DELETE /batch/{batch_id} (stop_batch)
/// Production-critical operation for killing runaway batches.
///
/// Test that stop_batch cancels Waiting, Pending, and Paused tasks.
#[tokio::test]
async fn test_stop_batch_cancels_non_terminal_tasks() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // Create a DAG: parent (Pending), child (Waiting)
    let tasks = vec![
        task_json("sb-parent", "Stop Batch Parent", "stop-batch"),
        task_with_deps(
            "sb-child",
            "Stop Batch Child",
            "stop-batch",
            vec![("sb-parent", true)],
        ),
    ];

    let created = create_tasks_ok(&app, &tasks).await;
    let batch_id = created[0].batch_id.expect("should have batch_id");
    let parent_id = created[0].id;
    let child_id = created[1].id;

    assert_eq!(created[0].status, StatusKind::Pending);
    assert_eq!(created[1].status, StatusKind::Waiting);

    // Pause the child so we test paused cancellation
    // First succeed the parent so the child goes Pending, then pause it
    // Actually, let's just pause the parent to test Paused -> Canceled
    let pause_req = actix_web::test::TestRequest::patch()
        .uri(&format!("/task/pause/{}", parent_id))
        .to_request();
    let pause_resp = actix_web::test::call_service(&app, pause_req).await;
    assert!(pause_resp.status().is_success());

    // Stop the batch
    let stop_req = actix_web::test::TestRequest::delete()
        .uri(&format!("/batch/{}", batch_id))
        .to_request();
    let stop_resp = actix_web::test::call_service(&app, stop_req).await;
    assert_eq!(
        stop_resp.status(),
        actix_web::http::StatusCode::OK,
        "stop_batch should return 200"
    );

    let body: StopBatchResponseDto = actix_web::test::read_body_json(stop_resp).await;
    assert_eq!(body.batch_id, batch_id);
    // Parent was paused, child was waiting
    assert_eq!(body.canceled_paused, 1, "paused parent should be canceled");
    assert_eq!(body.canceled_waiting, 1, "waiting child should be canceled");

    // Verify tasks are actually canceled
    assert_task_status(
        &app,
        parent_id,
        StatusKind::Canceled,
        "parent should be canceled",
    )
    .await;
    assert_task_status(
        &app,
        child_id,
        StatusKind::Canceled,
        "child should be canceled",
    )
    .await;
}

/// Test stop_batch with Running tasks — they should be canceled.
#[tokio::test]
async fn test_stop_batch_cancels_running_tasks() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let tasks = vec![task_json("sb-running", "Stop Batch Running", "stop-batch")];
    let created = create_tasks_ok(&app, &tasks).await;
    let batch_id = created[0].batch_id.unwrap();
    let task_id = created[0].id;

    // Move task to Running via claim + mark_running
    let mut conn = state.pool.get().await.unwrap();
    arcrun::db_operation::claim_task(&mut conn, &task_id)
        .await
        .unwrap();
    arcrun::db_operation::mark_task_running(&mut conn, &task_id)
        .await
        .unwrap();
    drop(conn);

    assert_task_status(&app, task_id, StatusKind::Running, "should be running").await;

    let stop_req = actix_web::test::TestRequest::delete()
        .uri(&format!("/batch/{}", batch_id))
        .to_request();
    let stop_resp = actix_web::test::call_service(&app, stop_req).await;
    assert_eq!(stop_resp.status(), actix_web::http::StatusCode::OK);

    let body: StopBatchResponseDto = actix_web::test::read_body_json(stop_resp).await;
    assert_eq!(body.canceled_running, 1);

    assert_task_status(
        &app,
        task_id,
        StatusKind::Canceled,
        "running task should be canceled",
    )
    .await;
}

/// Test stop_batch already_terminal count is correct.
#[tokio::test]
async fn test_stop_batch_already_terminal_count() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let tasks = vec![
        task_json("sb-done1", "Done Task 1", "stop-batch"),
        task_json("sb-done2", "Done Task 2", "stop-batch"),
        task_json("sb-pending", "Pending Task", "stop-batch"),
    ];

    let created = create_tasks_ok(&app, &tasks).await;
    let batch_id = created[0].batch_id.unwrap();

    // Complete task 1 with success, task 2 with failure
    succeed_task(&state, created[0].id).await;
    fail_task(&state, created[1].id, "test failure").await;

    let stop_req = actix_web::test::TestRequest::delete()
        .uri(&format!("/batch/{}", batch_id))
        .to_request();
    let stop_resp = actix_web::test::call_service(&app, stop_req).await;
    assert_eq!(stop_resp.status(), actix_web::http::StatusCode::OK);

    let body: StopBatchResponseDto = actix_web::test::read_body_json(stop_resp).await;
    assert_eq!(
        body.already_terminal, 2,
        "two tasks were already terminal (Success + Failure)"
    );
    assert_eq!(
        body.canceled_pending, 1,
        "one pending task should be canceled"
    );
}

/// Test stop_batch with non-existent batch returns 404.
#[tokio::test]
async fn test_stop_batch_not_found() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let fake_batch = uuid::Uuid::new_v4();
    let stop_req = actix_web::test::TestRequest::delete()
        .uri(&format!("/batch/{}", fake_batch))
        .to_request();
    let stop_resp = actix_web::test::call_service(&app, stop_req).await;
    assert_eq!(
        stop_resp.status(),
        actix_web::http::StatusCode::NOT_FOUND,
        "non-existent batch should return 404"
    );
}
