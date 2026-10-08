use crate::common::*;

use arcrun::dtos::BasicTaskDto;
use serde_json::json;

/// Bug #9: Duplicate local IDs in a batch should be rejected.
#[tokio::test]
async fn test_bug9_duplicate_ids_in_batch() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let payload = json!([
        {
            "id": "dup",
            "name": "First",
            "kind": "test",
            "timeout": 60,
            "on_start": webhook_action()
        },
        {
            "id": "dup",
            "name": "Second",
            "kind": "test",
            "timeout": 60,
            "on_start": webhook_action()
        }
    ]);

    let req = actix_web::test::TestRequest::post()
        .uri("/task")
        .set_json(&payload)
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;

    assert_eq!(
        resp.status(),
        actix_web::http::StatusCode::BAD_REQUEST,
        "Duplicate local IDs in a batch should be rejected with 400"
    );
}

/// Bug #10: Unknown dependency references should be rejected.
#[tokio::test]
async fn test_bug10_unknown_dependency() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let payload = json!([
        {
            "id": "child",
            "name": "Child",
            "kind": "test",
            "timeout": 60,
            "on_start": webhook_action(),
            "dependencies": [{"id": "nonexistent", "requires_success": true}]
        }
    ]);

    let req = actix_web::test::TestRequest::post()
        .uri("/task")
        .set_json(&payload)
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;

    assert_eq!(
        resp.status(),
        actix_web::http::StatusCode::BAD_REQUEST,
        "Unknown dependency IDs should be rejected with 400"
    );
}

/// Bug #11: Forward dependency references should be rejected.
#[tokio::test]
async fn test_bug11_forward_reference() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // task A depends on B, but B comes after A in the array
    let payload = json!([
        {
            "id": "A",
            "name": "Task A",
            "kind": "test",
            "timeout": 60,
            "on_start": webhook_action(),
            "dependencies": [{"id": "B", "requires_success": true}]
        },
        {
            "id": "B",
            "name": "Task B",
            "kind": "test",
            "timeout": 60,
            "on_start": webhook_action()
        }
    ]);

    let req = actix_web::test::TestRequest::post()
        .uri("/task")
        .set_json(&payload)
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;

    assert_eq!(
        resp.status(),
        actix_web::http::StatusCode::BAD_REQUEST,
        "Forward dependency references should be rejected with 400"
    );
}

/// Verify valid backward references still work after strict DAG validation.
#[tokio::test]
async fn test_valid_backward_reference() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let payload = json!([
        {
            "id": "A",
            "name": "Task A",
            "kind": "test",
            "timeout": 60,
            "metadata": {"test": true},
            "on_start": webhook_action()
        },
        {
            "id": "B",
            "name": "Task B",
            "kind": "test",
            "timeout": 60,
            "metadata": {"test": true},
            "on_start": webhook_action(),
            "dependencies": [{"id": "A", "requires_success": true}]
        }
    ]);

    let req = actix_web::test::TestRequest::post()
        .uri("/task")
        .set_json(&payload)
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;

    assert_eq!(
        resp.status(),
        actix_web::http::StatusCode::CREATED,
        "Valid backward references should be accepted with 201"
    );
}

/// Bug #16: list_task_filtered_paged excludes tasks with NULL metadata.
#[tokio::test]
async fn test_bug16_list_includes_null_metadata() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let payload = json!([{
        "id": "no-meta",
        "name": "No Metadata Task",
        "kind": "null-meta-test",
        "timeout": 60,
        "on_start": webhook_action()
    }]);

    let create_req = actix_web::test::TestRequest::post()
        .uri("/task")
        .set_json(&payload)
        .to_request();
    let create_resp = actix_web::test::call_service(&app, create_req).await;
    assert_eq!(create_resp.status(), actix_web::http::StatusCode::CREATED);

    let list_req = actix_web::test::TestRequest::get()
        .uri("/task?kind=null-meta-test")
        .to_request();
    let list_resp = actix_web::test::call_service(&app, list_req).await;
    assert_eq!(list_resp.status(), actix_web::http::StatusCode::OK);

    let tasks: Vec<BasicTaskDto> = actix_web::test::read_body_json(list_resp).await;
    assert!(
        !tasks.is_empty(),
        "Tasks with NULL metadata should appear in listing when no metadata filter is applied"
    );
    assert_eq!(tasks[0].kind, "null-meta-test");
}

/// Bug #18: timeout filter is declared in FilterDto but never applied.
#[tokio::test]
async fn test_bug18_timeout_filter() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let payload = json!([
        {
            "id": "t60",
            "name": "Timeout 60",
            "kind": "timeout-test",
            "timeout": 60,
            "metadata": {"test": true},
            "on_start": webhook_action()
        },
        {
            "id": "t120",
            "name": "Timeout 120",
            "kind": "timeout-test",
            "timeout": 120,
            "metadata": {"test": true},
            "on_start": webhook_action()
        }
    ]);

    let create_req = actix_web::test::TestRequest::post()
        .uri("/task")
        .set_json(&payload)
        .to_request();
    let create_resp = actix_web::test::call_service(&app, create_req).await;
    assert_eq!(create_resp.status(), actix_web::http::StatusCode::CREATED);

    let list_req = actix_web::test::TestRequest::get()
        .uri("/task?kind=timeout-test&timeout=60")
        .to_request();
    let list_resp = actix_web::test::call_service(&app, list_req).await;
    assert_eq!(list_resp.status(), actix_web::http::StatusCode::OK);

    let tasks: Vec<BasicTaskDto> = actix_web::test::read_body_json(list_resp).await;
    assert_eq!(
        tasks.len(),
        1,
        "Only tasks with timeout=60 should be returned"
    );
    assert_eq!(tasks[0].name, "Timeout 60");
}

/// Bug #19: PUT /task/{id} over-validates status.
///
/// Previously, PUT handler called validate_update_task which rejects
/// status=Failure without failure_reason. Now it uses validate_update_task_counters.
#[tokio::test]
async fn test_bug19_put_ignores_status_validation() {
    let (_g, test_state) = setup_test_app_with_batch_updater().await;
    let state = test_state.state;
    let app = test_service!(state);

    let payload = json!([{
        "id": "put-test",
        "name": "PUT Test",
        "kind": "test",
        "timeout": 60,
        "metadata": {"test": true},
        "on_start": webhook_action()
    }]);

    let create_req = actix_web::test::TestRequest::post()
        .uri("/task")
        .set_json(&payload)
        .to_request();
    let create_resp = actix_web::test::call_service(&app, create_req).await;
    assert_eq!(create_resp.status(), actix_web::http::StatusCode::CREATED);
    let tasks: Vec<BasicTaskDto> = actix_web::test::read_body_json(create_resp).await;
    let task_id = tasks[0].id;

    // PUT with status=Failure and no failure_reason (plus a valid counter)
    // Should succeed because PUT only validates counters, not status
    let put_req = actix_web::test::TestRequest::put()
        .uri(&format!("/task/{}", task_id))
        .set_json(json!({"status": "Failure", "new_success": 1}))
        .to_request();
    let put_resp = actix_web::test::call_service(&app, put_req).await;

    assert_eq!(
        put_resp.status(),
        actix_web::http::StatusCode::ACCEPTED,
        "PUT should accept status=Failure without failure_reason (status is ignored by PUT)"
    );
}

/// Bug #19 (complement): PATCH still validates status.
#[tokio::test]
async fn test_bug19_patch_still_validates_status() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let payload = json!([{
        "id": "patch-validate",
        "name": "PATCH Validate",
        "kind": "test",
        "timeout": 60,
        "metadata": {"test": true},
        "on_start": webhook_action()
    }]);

    let create_req = actix_web::test::TestRequest::post()
        .uri("/task")
        .set_json(&payload)
        .to_request();
    let create_resp = actix_web::test::call_service(&app, create_req).await;
    assert_eq!(create_resp.status(), actix_web::http::StatusCode::CREATED);
    let tasks: Vec<BasicTaskDto> = actix_web::test::read_body_json(create_resp).await;
    let task_id = tasks[0].id;

    // PATCH with status=Failure and no failure_reason — should be rejected
    let patch_req = actix_web::test::TestRequest::patch()
        .uri(&format!("/task/{}", task_id))
        .set_json(json!({"status": "Failure"}))
        .to_request();
    let patch_resp = actix_web::test::call_service(&app, patch_req).await;

    assert_eq!(
        patch_resp.status(),
        actix_web::http::StatusCode::BAD_REQUEST,
        "PATCH should still reject status=Failure without failure_reason"
    );
}
