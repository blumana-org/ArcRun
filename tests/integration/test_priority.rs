use crate::common::*;

use serde_json::json;

/// Task created without a priority field should default to priority 0.
#[actix_web::test]
async fn test_priority_default_is_zero() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let tasks = create_tasks_ok(&app, &[task_json("t1", "Task 1", "ci")]).await;
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].priority, 0);

    // Also verify via GET
    let detail = get_task_ok(&app, tasks[0].id).await;
    assert_eq!(detail.priority, 0);
}

/// Task created with an explicit priority should retain it.
#[actix_web::test]
async fn test_priority_set_on_creation() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let mut t = task_json("t1", "High Priority", "ci");
    t["priority"] = json!(500);

    let tasks = create_tasks_ok(&app, &[t]).await;
    assert_eq!(tasks[0].priority, 500);

    let detail = get_task_ok(&app, tasks[0].id).await;
    assert_eq!(detail.priority, 500);
}

/// Negative priority values should be accepted.
#[actix_web::test]
async fn test_priority_negative_value() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let mut t = task_json("t1", "Low Priority", "ci");
    t["priority"] = json!(-500);

    let tasks = create_tasks_ok(&app, &[t]).await;
    assert_eq!(tasks[0].priority, -500);
}

/// Priority outside the valid range (-1000..1000) should be rejected with 400.
#[actix_web::test]
async fn test_priority_validation_out_of_range() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // Too high
    let mut t = task_json("t1", "Too High", "ci");
    t["priority"] = json!(1001);

    let req = actix_web::test::TestRequest::post()
        .uri("/task")
        .insert_header(("requester", "test"))
        .set_json(&[t])
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::BAD_REQUEST);

    // Too low
    let mut t = task_json("t2", "Too Low", "ci");
    t["priority"] = json!(-1001);

    let req = actix_web::test::TestRequest::post()
        .uri("/task")
        .insert_header(("requester", "test"))
        .set_json(&[t])
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::BAD_REQUEST);
}

/// Priority can be updated via PATCH on a running task.
#[actix_web::test]
async fn test_priority_update() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let mut t = task_json("t1", "Task", "ci");
    t["priority"] = json!(100);
    let tasks = create_tasks_ok(&app, &[t]).await;
    let task_id = tasks[0].id;

    // Claim and mark running so we can PATCH
    let mut conn = state.pool.get().await.unwrap();
    arcrun::db_operation::claim_task(&mut conn, &task_id)
        .await
        .unwrap();
    arcrun::db_operation::mark_task_running(&mut conn, &task_id)
        .await
        .unwrap();

    // PATCH to update priority
    let req = actix_web::test::TestRequest::patch()
        .uri(&format!("/task/{}", task_id))
        .set_json(json!({ "priority": 999 }))
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert!(resp.status().is_success(), "PATCH should succeed");

    let detail = get_task_ok(&app, task_id).await;
    assert_eq!(detail.priority, 999);
}

/// Priority should appear in BasicTaskDto (list endpoint responses).
#[actix_web::test]
async fn test_priority_in_basic_task_dto() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let mut t1 = task_json("t1", "Low", "ci");
    t1["priority"] = json!(-100);
    let mut t2 = task_json("t2", "High", "ci");
    t2["priority"] = json!(100);

    let tasks = create_tasks_ok(&app, &[t1, t2]).await;
    assert_eq!(tasks[0].priority, -100);
    assert_eq!(tasks[1].priority, 100);
}
