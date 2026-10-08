use crate::common::*;

use arcrun::dtos::TaskDto;
use arcrun::models::StatusKind;
use serde_json::json;

#[tokio::test]
async fn test_pause_task() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let tasks = vec![task_json("pausable", "Pausable Task", "pausable-kind")];
    let created = create_tasks_ok(&app, &tasks).await;
    let task_id = created[0].id;

    // Pause the task
    let pause_req = actix_web::test::TestRequest::patch()
        .uri(&format!("/task/pause/{}", task_id))
        .to_request();
    let pause_resp = actix_web::test::call_service(&app, pause_req).await;
    assert!(pause_resp.status().is_success());

    assert_task_status(&app, task_id, StatusKind::Paused, "task should be paused").await;
}

#[tokio::test]
async fn test_cancel_pending_task() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let tasks = vec![task_json(
        "cancelable",
        "Cancelable Task",
        "cancelable-kind",
    )];
    let created = create_tasks_ok(&app, &tasks).await;
    let task_id = created[0].id;

    // Cancel the task
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
}

#[tokio::test]
async fn test_update_task_status_via_api() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let tasks = vec![task_json("updatable", "Updatable Task", "update-test")];
    let created = create_tasks_ok(&app, &tasks).await;
    let task_id = created[0].id;

    // Claim the task (Pending -> Claimed -> Running)
    let mut conn = state.pool.get().await.unwrap();
    arcrun::db_operation::claim_task(&mut conn, &task_id)
        .await
        .unwrap();
    arcrun::db_operation::mark_task_running(&mut conn, &task_id)
        .await
        .unwrap();

    // Update task to Success via PATCH
    let update_req = actix_web::test::TestRequest::patch()
        .uri(&format!("/task/{}", task_id))
        .set_json(json!({"status": "Success"}))
        .to_request();
    let update_resp = actix_web::test::call_service(&app, update_req).await;
    assert!(update_resp.status().is_success());

    assert_task_status(&app, task_id, StatusKind::Success, "task should be success").await;
}

#[tokio::test]
async fn test_update_task_with_failure_reason() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let tasks = vec![task_json(
        "fail-reason",
        "Fail Reason Task",
        "fail-reason-test",
    )];
    let created = create_tasks_ok(&app, &tasks).await;
    let task_id = created[0].id;

    // Claim the task (Pending -> Claimed -> Running)
    let mut conn = state.pool.get().await.unwrap();
    arcrun::db_operation::claim_task(&mut conn, &task_id)
        .await
        .unwrap();
    arcrun::db_operation::mark_task_running(&mut conn, &task_id)
        .await
        .unwrap();

    // Update task to Failure with reason
    let update_req = actix_web::test::TestRequest::patch()
        .uri(&format!("/task/{}", task_id))
        .set_json(json!({
            "status": "Failure",
            "failure_reason": "Connection timeout after 30 seconds"
        }))
        .to_request();
    let update_resp = actix_web::test::call_service(&app, update_req).await;
    assert!(update_resp.status().is_success());

    let updated: TaskDto = get_task_ok(&app, task_id).await;
    assert_eq!(updated.status, StatusKind::Failure);
    assert_eq!(
        updated.failure_reason,
        Some("Connection timeout after 30 seconds".to_string())
    );
}

#[tokio::test]
async fn test_cancel_task_with_waiting_children() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let tasks = vec![
        task_json("cancel-parent", "Cancel Parent", "cancel-cascade"),
        task_with_deps(
            "cancel-child1",
            "Cancel Child 1",
            "cancel-cascade",
            vec![("cancel-parent", true)],
        ),
        task_with_deps(
            "cancel-child2",
            "Cancel Child 2",
            "cancel-cascade",
            vec![("cancel-parent", true)],
        ),
    ];

    let created = create_tasks_ok(&app, &tasks).await;
    let parent_id = created[0].id;
    let child1_id = created[1].id;
    let child2_id = created[2].id;

    assert_eq!(created[0].status, StatusKind::Pending);
    assert_eq!(created[1].status, StatusKind::Waiting);
    assert_eq!(created[2].status, StatusKind::Waiting);

    // Cancel the parent
    let cancel_req = actix_web::test::TestRequest::delete()
        .uri(&format!("/task/{}", parent_id))
        .to_request();
    let cancel_resp = actix_web::test::call_service(&app, cancel_req).await;
    assert!(cancel_resp.status().is_success());

    assert_task_status(
        &app,
        parent_id,
        StatusKind::Canceled,
        "parent should be canceled",
    )
    .await;

    // Children with requires_success=true should be marked as Failure
    let child1 = get_task_ok(&app, child1_id).await;
    assert_eq!(
        child1.status,
        StatusKind::Failure,
        "Child 1 should be Failure because required parent was canceled"
    );
    assert!(
        child1.failure_reason.is_some(),
        "Child 1 should have a failure reason"
    );

    let child2 = get_task_ok(&app, child2_id).await;
    assert_eq!(
        child2.status,
        StatusKind::Failure,
        "Child 2 should be Failure because required parent was canceled"
    );
    assert!(
        child2.failure_reason.is_some(),
        "Child 2 should have a failure reason"
    );
}
