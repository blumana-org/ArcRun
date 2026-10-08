use crate::common::*;

use arcrun::models::StatusKind;

/// Priority 3 — End-to-end validation tests via HTTP API.
///
/// Test POST /task with empty array returns appropriate response.
#[tokio::test]
async fn test_create_task_empty_array() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let req = actix_web::test::TestRequest::post()
        .uri("/task")
        .insert_header(("requester", "test"))
        .set_json(Vec::<serde_json::Value>::new())
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;

    // Empty array should return 204 No Content (all tasks deduplicated / nothing to create)
    assert_eq!(
        resp.status(),
        actix_web::http::StatusCode::NO_CONTENT,
        "empty task array should return 204"
    );
}

/// Test DELETE /task/{id} on a terminal task (Success) returns 400.
#[tokio::test]
async fn test_cancel_terminal_task_returns_400() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let tasks = vec![task_json(
        "cancel-term",
        "Cancel Terminal",
        "cancel-term-kind",
    )];
    let created = create_tasks_ok(&app, &tasks).await;
    let task_id = created[0].id;

    // Complete as Success
    succeed_task(&state, task_id).await;
    assert_task_status(&app, task_id, StatusKind::Success, "should be success").await;

    // Try to cancel a Success task
    let cancel_req = actix_web::test::TestRequest::delete()
        .uri(&format!("/task/{}", task_id))
        .to_request();
    let cancel_resp = actix_web::test::call_service(&app, cancel_req).await;
    assert_eq!(
        cancel_resp.status(),
        actix_web::http::StatusCode::BAD_REQUEST,
        "canceling a terminal task should return 400"
    );
}

/// Test DELETE /task/{id} on a Failure task returns 400.
#[tokio::test]
async fn test_cancel_failed_task_returns_400() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let tasks = vec![task_json(
        "cancel-fail",
        "Cancel Failed",
        "cancel-fail-kind",
    )];
    let created = create_tasks_ok(&app, &tasks).await;
    let task_id = created[0].id;

    fail_task(&state, task_id, "test failure").await;

    let cancel_req = actix_web::test::TestRequest::delete()
        .uri(&format!("/task/{}", task_id))
        .to_request();
    let cancel_resp = actix_web::test::call_service(&app, cancel_req).await;
    assert_eq!(
        cancel_resp.status(),
        actix_web::http::StatusCode::BAD_REQUEST,
        "canceling a failed task should return 400"
    );
}

/// Test PATCH /task/pause on a Waiting task.
#[tokio::test]
async fn test_pause_waiting_task() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let tasks = vec![
        task_json("pause-parent", "Pause Parent", "pause-kind"),
        task_with_deps(
            "pause-wait",
            "Pause Waiting",
            "pause-kind",
            vec![("pause-parent", true)],
        ),
    ];
    let created = create_tasks_ok(&app, &tasks).await;
    let waiting_id = created[1].id;
    assert_eq!(created[1].status, StatusKind::Waiting);

    let pause_req = actix_web::test::TestRequest::patch()
        .uri(&format!("/task/pause/{}", waiting_id))
        .to_request();
    let pause_resp = actix_web::test::call_service(&app, pause_req).await;
    assert!(
        pause_resp.status().is_success(),
        "pausing a waiting task should succeed"
    );

    assert_task_status(
        &app,
        waiting_id,
        StatusKind::Paused,
        "waiting task should be paused",
    )
    .await;
}

/// Test PATCH /task/pause on an already-paused task returns 400.
#[tokio::test]
async fn test_pause_already_paused_task_returns_400() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let tasks = vec![task_json("pause-twice", "Pause Twice", "pause-kind")];
    let created = create_tasks_ok(&app, &tasks).await;
    let task_id = created[0].id;

    // Pause once
    let pause_req = actix_web::test::TestRequest::patch()
        .uri(&format!("/task/pause/{}", task_id))
        .to_request();
    let resp = actix_web::test::call_service(&app, pause_req).await;
    assert!(resp.status().is_success());

    // Pause again
    let pause_req2 = actix_web::test::TestRequest::patch()
        .uri(&format!("/task/pause/{}", task_id))
        .to_request();
    let resp2 = actix_web::test::call_service(&app, pause_req2).await;
    assert_eq!(
        resp2.status(),
        actix_web::http::StatusCode::BAD_REQUEST,
        "pausing an already-paused task should return 400"
    );
}

/// Test PATCH /task/pause on a terminal task returns 400.
#[tokio::test]
async fn test_pause_terminal_task_returns_400() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let tasks = vec![task_json("pause-terminal", "Pause Terminal", "pause-kind")];
    let created = create_tasks_ok(&app, &tasks).await;
    let task_id = created[0].id;

    succeed_task(&state, task_id).await;

    let pause_req = actix_web::test::TestRequest::patch()
        .uri(&format!("/task/pause/{}", task_id))
        .to_request();
    let resp = actix_web::test::call_service(&app, pause_req).await;
    assert_eq!(
        resp.status(),
        actix_web::http::StatusCode::BAD_REQUEST,
        "pausing a terminal task should return 400"
    );
}

/// Test diamond DAG with mixed success/failure.
/// Diamond: A -> B, A -> C, B -> D, C -> D (all requires_success=true).
/// A succeeds, B succeeds, C fails -> D should fail (because C failed with requires_success).
#[tokio::test]
async fn test_diamond_dag_mixed_results() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let tasks = vec![
        task_json("diamond-a", "Diamond A", "diamond"),
        task_with_deps(
            "diamond-b",
            "Diamond B",
            "diamond",
            vec![("diamond-a", true)],
        ),
        task_with_deps(
            "diamond-c",
            "Diamond C",
            "diamond",
            vec![("diamond-a", true)],
        ),
        task_with_deps(
            "diamond-d",
            "Diamond D",
            "diamond",
            vec![("diamond-b", true), ("diamond-c", true)],
        ),
    ];

    let created = create_tasks_ok(&app, &tasks).await;
    let a_id = created[0].id;
    let b_id = created[1].id;
    let c_id = created[2].id;
    let d_id = created[3].id;

    // A succeeds -> B and C become Pending
    succeed_task(&state, a_id).await;
    assert_task_status(&app, b_id, StatusKind::Pending, "B should be Pending").await;
    assert_task_status(&app, c_id, StatusKind::Pending, "C should be Pending").await;

    // B succeeds
    succeed_task(&state, b_id).await;
    // D is still Waiting (needs C to finish)
    assert_task_status(&app, d_id, StatusKind::Waiting, "D should still be Waiting").await;

    // C fails -> D should fail because requires_success=true on C->D link
    fail_task(&state, c_id, "C failed").await;

    let d = get_task_ok(&app, d_id).await;
    assert_eq!(
        d.status,
        StatusKind::Failure,
        "D should fail because C failed and requires_success=true"
    );
    assert!(d.failure_reason.is_some());
}

/// PATCH /task/pause on a Running task is refused (400) — contract change
/// (audit 2, A3, décision utilisateur 2026-07-09): a Running task is already
/// executing at the consumer, so a server-side pause would strand it (it escapes
/// the timeout loop and the worker's terminal PATCH would 404). Cancel is the
/// right tool for in-flight work. Previously this test asserted the opposite
/// (pause of Running succeeded), which was part of the A3 trap state.
#[tokio::test]
async fn test_pause_running_task_returns_400() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let tasks = vec![task_json("pause-run", "Pause Running", "pause-kind")];
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

    assert_task_status(&app, task_id, StatusKind::Running, "should be running").await;

    let pause_req = actix_web::test::TestRequest::patch()
        .uri(&format!("/task/pause/{}", task_id))
        .to_request();
    let resp = actix_web::test::call_service(&app, pause_req).await;
    assert_eq!(
        resp.status(),
        actix_web::http::StatusCode::BAD_REQUEST,
        "pausing a running task must be refused (cancel it instead)"
    );

    assert_task_status(
        &app,
        task_id,
        StatusKind::Running,
        "running task must stay Running after a refused pause",
    )
    .await;
}

/// Test GET /task/not-a-uuid returns 400.
#[tokio::test]
async fn test_invalid_uuid_returns_400() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let req = actix_web::test::TestRequest::get()
        .uri("/task/not-a-uuid")
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(
        resp.status(),
        actix_web::http::StatusCode::NOT_FOUND,
        "invalid UUID path should return 404 (no route match)"
    );
}

/// Test POST /task with invalid JSON returns 400.
#[tokio::test]
async fn test_malformed_json_returns_400() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let req = actix_web::test::TestRequest::post()
        .uri("/task")
        .insert_header(("content-type", "application/json"))
        .insert_header(("requester", "test"))
        .set_payload("{not valid json")
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(
        resp.status(),
        actix_web::http::StatusCode::BAD_REQUEST,
        "malformed JSON should return 400"
    );
}

/// Test PATCH /task/{id} with invalid JSON returns 400.
#[tokio::test]
async fn test_patch_malformed_json_returns_400() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let tasks = vec![task_json("patch-bad", "Patch Bad JSON", "patch-kind")];
    let created = create_tasks_ok(&app, &tasks).await;
    let task_id = created[0].id;

    let req = actix_web::test::TestRequest::patch()
        .uri(&format!("/task/{}", task_id))
        .insert_header(("content-type", "application/json"))
        .set_payload("{bad json")
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(
        resp.status(),
        actix_web::http::StatusCode::BAD_REQUEST,
        "malformed JSON on PATCH should return 400"
    );
}

/// Test PUT /task/{id} with invalid JSON returns 400.
#[tokio::test]
async fn test_put_malformed_json_returns_400() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let tasks = vec![task_json("put-bad", "Put Bad JSON", "put-kind")];
    let created = create_tasks_ok(&app, &tasks).await;
    let task_id = created[0].id;

    let req = actix_web::test::TestRequest::put()
        .uri(&format!("/task/{}", task_id))
        .insert_header(("content-type", "application/json"))
        .set_payload("{bad json")
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(
        resp.status(),
        actix_web::http::StatusCode::BAD_REQUEST,
        "malformed JSON on PUT should return 400"
    );
}
