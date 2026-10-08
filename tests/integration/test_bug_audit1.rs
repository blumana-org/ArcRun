// Diesel QueryableByName emits separate impls with explicit field initializers;
// an allow on the struct does not cover those generated impls.
#![allow(clippy::redundant_field_names)]

use crate::common::*;

use arcrun::models::StatusKind;
use serde_json::json;

/// Bug #1: Duplicate webhook execution across multiple workers (multi-instance race).
///
/// Fix: claim_task uses an atomic UPDATE ... WHERE status=Pending transition.
/// Only one caller gets updated_count=1; the other gets 0 and skips the webhook.
#[tokio::test]
async fn test_bug1_claim_task_atomic_prevents_duplicate_execution() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let tasks = vec![task_json(
        "atomic-claim",
        "Atomic Claim Test",
        "atomic-test",
    )];
    let created = create_tasks_ok(&app, &tasks).await;
    let task_id = created[0].id;

    // Spawn two concurrent claim attempts to simulate multi-instance race
    let pool1 = state.pool.clone();
    let pool2 = state.pool.clone();

    let (claim1, claim2) = tokio::join!(
        tokio::spawn(async move {
            let mut conn = pool1.get().await.unwrap();
            arcrun::db_operation::claim_task(&mut conn, &task_id)
                .await
                .unwrap()
        }),
        tokio::spawn(async move {
            let mut conn = pool2.get().await.unwrap();
            arcrun::db_operation::claim_task(&mut conn, &task_id)
                .await
                .unwrap()
        })
    );

    let claim1 = claim1.unwrap();
    let claim2 = claim2.unwrap();

    // Exactly one claim should succeed
    assert!(
        (claim1 && !claim2) || (!claim1 && claim2),
        "Exactly one concurrent claim should succeed: claim1={}, claim2={}",
        claim1,
        claim2
    );

    assert_task_status(&app, task_id, StatusKind::Claimed, "task should be Claimed").await;
}

/// Bug #2: Repeated end-task propagation causing negative counters.
///
/// Fix: update_running_task wraps the update + propagation in a transaction with
/// WHERE status=Running. The second call sees status=Success, updates 0 rows, and
/// skips propagation entirely.
#[tokio::test]
async fn test_bug2_double_update_no_negative_counters() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let tasks = vec![
        task_json("neg-parent", "Neg Counter Parent", "neg-counter-test"),
        task_with_deps(
            "neg-child",
            "Neg Counter Child",
            "neg-counter-test",
            vec![("neg-parent", true)],
        ),
    ];

    let created = create_tasks_ok(&app, &tasks).await;
    let parent_id = created[0].id;
    let child_id = created[1].id;

    assert_eq!(created[0].status, StatusKind::Pending);
    assert_eq!(created[1].status, StatusKind::Waiting);

    // Verify initial counters
    let (wf, ws) = read_wait_counters(&state.pool, child_id).await;
    assert_eq!(wf, 1, "Initial wait_finished should be 1");
    assert_eq!(ws, 1, "Initial wait_success should be 1");

    let mut conn = state.pool.get().await.unwrap();
    arcrun::db_operation::claim_task(&mut conn, &parent_id)
        .await
        .unwrap();
    arcrun::db_operation::mark_task_running(&mut conn, &parent_id)
        .await
        .unwrap();

    // First update: should succeed and propagate
    let res1 = arcrun::db_operation::update_running_task(&mut conn, parent_id, success_dto(), true)
        .await
        .unwrap();
    assert_eq!(
        res1,
        arcrun::db_operation::UpdateTaskResult::Updated,
        "First update should succeed"
    );

    // Second update: should be no-op (task is no longer Running)
    let res2 = arcrun::db_operation::update_running_task(&mut conn, parent_id, success_dto(), true)
        .await
        .unwrap();
    // A10: a re-PATCH of the same terminal status is now an idempotent no-op
    // (AlreadyInRequestedState) rather than a blanket NotFound — but it STILL skips
    // propagation, so the anti-double-propagation guarantee this test checks holds.
    assert_eq!(
        res2,
        arcrun::db_operation::UpdateTaskResult::AlreadyInRequestedState,
        "Second update should be an idempotent no-op (task already Success; no double propagation)"
    );

    assert_task_status(
        &app,
        child_id,
        StatusKind::Pending,
        "Child should be Pending after single propagation",
    )
    .await;

    // Verify wait counters are NOT negative
    let (wf, ws) = read_wait_counters(&state.pool, child_id).await;
    assert!(wf >= 0, "wait_finished must not be negative, got {}", wf);
    assert!(ws >= 0, "wait_success must not be negative, got {}", ws);
    assert_eq!(wf, 0, "wait_finished should be exactly 0");
    assert_eq!(ws, 0, "wait_success should be exactly 0");
}

/// Bug #3: End-task actions (webhooks) executed multiple times.
///
/// Fix: update_running_task returns the number of rows affected. When res != 1,
/// webhooks are not fired.
#[tokio::test]
async fn test_bug3_double_success_update_returns_zero_second_time() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let task = json!({
        "id": "double-update",
        "name": "Double Update Test",
        "kind": "double-test",
        "timeout": 60,
        "metadata": {"test": true},
        "on_start": webhook_action(),
        "on_success": [webhook_action()]
    });

    let created = create_tasks_ok(&app, &[task]).await;
    let task_id = created[0].id;

    let mut conn = state.pool.get().await.unwrap();
    arcrun::db_operation::claim_task(&mut conn, &task_id)
        .await
        .unwrap();
    arcrun::db_operation::mark_task_running(&mut conn, &task_id)
        .await
        .unwrap();

    let res1 = arcrun::db_operation::update_running_task(&mut conn, task_id, success_dto(), true)
        .await
        .unwrap();
    assert_eq!(res1, arcrun::db_operation::UpdateTaskResult::Updated);

    let res2 = arcrun::db_operation::update_running_task(&mut conn, task_id, success_dto(), true)
        .await
        .unwrap();
    // A10: re-PATCH of the same terminal status is now an idempotent no-op — no
    // duplicate webhook execution (the outbox row is not re-enqueued).
    assert_eq!(
        res2,
        arcrun::db_operation::UpdateTaskResult::AlreadyInRequestedState,
        "Second update should be an idempotent no-op — prevents duplicate webhook execution"
    );

    assert_task_status(
        &app,
        task_id,
        StatusKind::Success,
        "task should still be Success",
    )
    .await;
}

/// Bug #3 (variant): a double update via the HTTP API must not double-deliver.
///
/// A10 update: the second identical PATCH is now an **idempotent 200** (was 404),
/// but the anti-double-delivery guarantee this test protects still holds — the
/// second call re-runs no propagation and enqueues no second end outbox row.
#[tokio::test]
async fn test_bug3_double_update_via_api_second_call_idempotent() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let tasks = vec![task_json(
        "api-double",
        "API Double Update",
        "api-double-test",
    )];
    let created = create_tasks_ok(&app, &tasks).await;
    let task_id = created[0].id;

    let mut conn = state.pool.get().await.unwrap();
    arcrun::db_operation::claim_task(&mut conn, &task_id)
        .await
        .unwrap();
    arcrun::db_operation::mark_task_running(&mut conn, &task_id)
        .await
        .unwrap();

    // First PATCH: Success
    let patch1 = actix_web::test::TestRequest::patch()
        .uri(&format!("/task/{}", task_id))
        .set_json(json!({"status": "Success"}))
        .to_request();
    let resp1 = actix_web::test::call_service(&app, patch1).await;
    assert_eq!(
        resp1.status(),
        actix_web::http::StatusCode::OK,
        "First update should return 200"
    );

    // Second PATCH with the SAME status: idempotent no-op → 200 (A10).
    let patch2 = actix_web::test::TestRequest::patch()
        .uri(&format!("/task/{}", task_id))
        .set_json(json!({"status": "Success"}))
        .to_request();
    let resp2 = actix_web::test::call_service(&app, patch2).await;
    assert_eq!(
        resp2.status(),
        actix_web::http::StatusCode::OK,
        "Second identical update should be an idempotent 200 (A10)"
    );

    // The idempotent no-op must NOT enqueue a second end outbox row.
    #[derive(diesel::QueryableByName)]
    struct Cnt {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        c: i64,
    }
    let cnt: Cnt = diesel_async::RunQueryDsl::get_result(
        diesel::sql_query(
            // D3: the end row is in the queue until delivered, then in history — count both.
            "SELECT (\
                (SELECT count(*) FROM webhook_execution WHERE task_id = $1 AND trigger = 'end') + \
                (SELECT count(*) FROM webhook_outbox    WHERE task_id = $1 AND trigger = 'end')\
             ) AS c",
        )
        .bind::<diesel::sql_types::Uuid, _>(task_id),
        &mut *conn,
    )
    .await
    .unwrap();
    assert_eq!(
        cnt.c, 1,
        "an idempotent re-PATCH must NOT create a second end outbox row"
    );
}

/// Bug #4: Update endpoint allows invalid transitions and returns misleading 404.
///
/// Fix: validate_update_task rejects non-Success/Failure statuses with 400.
#[tokio::test]
async fn test_bug4_update_rejects_invalid_status_transitions() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let tasks = vec![task_json(
        "invalid-trans",
        "Invalid Transition Test",
        "invalid-trans-test",
    )];
    let created = create_tasks_ok(&app, &tasks).await;
    let task_id = created[0].id;

    let mut conn = state.pool.get().await.unwrap();
    arcrun::db_operation::claim_task(&mut conn, &task_id)
        .await
        .unwrap();
    arcrun::db_operation::mark_task_running(&mut conn, &task_id)
        .await
        .unwrap();

    for invalid_status in &["Running", "Pending", "Waiting"] {
        let req = actix_web::test::TestRequest::patch()
            .uri(&format!("/task/{}", task_id))
            .set_json(json!({"status": invalid_status}))
            .to_request();
        let resp = actix_web::test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            actix_web::http::StatusCode::BAD_REQUEST,
            "Setting status to {} should return 400",
            invalid_status
        );
    }

    assert_task_status(
        &app,
        task_id,
        StatusKind::Running,
        "Task should still be Running after all invalid update attempts",
    )
    .await;
}

/// Bug #4 (variant): Failure without failure_reason should return 400.
#[tokio::test]
async fn test_bug4_failure_requires_reason() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let tasks = vec![task_json("need-reason", "Need Reason Test", "reason-test")];
    let created = create_tasks_ok(&app, &tasks).await;
    let task_id = created[0].id;

    let mut conn = state.pool.get().await.unwrap();
    arcrun::db_operation::claim_task(&mut conn, &task_id)
        .await
        .unwrap();
    arcrun::db_operation::mark_task_running(&mut conn, &task_id)
        .await
        .unwrap();

    let req_no_reason = actix_web::test::TestRequest::patch()
        .uri(&format!("/task/{}", task_id))
        .set_json(json!({"status": "Failure"}))
        .to_request();
    let resp_no_reason = actix_web::test::call_service(&app, req_no_reason).await;
    assert_eq!(
        resp_no_reason.status(),
        actix_web::http::StatusCode::BAD_REQUEST,
        "Failure without failure_reason should return 400"
    );

    let body: serde_json::Value = actix_web::test::read_body_json(resp_no_reason).await;
    assert!(
        body.get("error").is_some(),
        "Response should contain 'error' field"
    );
    assert!(
        body.get("details").is_some(),
        "Response should contain 'details' field"
    );
}

/// Bug #4 (variant): Negative counters should be rejected with 400.
#[tokio::test]
async fn test_bug4_rejects_negative_counters() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let tasks = vec![task_json(
        "neg-counter",
        "Negative Counter Test",
        "neg-counter-test",
    )];
    let created = create_tasks_ok(&app, &tasks).await;
    let task_id = created[0].id;

    let mut conn = state.pool.get().await.unwrap();
    arcrun::db_operation::claim_task(&mut conn, &task_id)
        .await
        .unwrap();
    arcrun::db_operation::mark_task_running(&mut conn, &task_id)
        .await
        .unwrap();

    // Negative new_success
    let req_neg_success = actix_web::test::TestRequest::patch()
        .uri(&format!("/task/{}", task_id))
        .set_json(json!({"new_success": -5}))
        .to_request();
    let resp_neg_success = actix_web::test::call_service(&app, req_neg_success).await;
    assert_eq!(
        resp_neg_success.status(),
        actix_web::http::StatusCode::BAD_REQUEST,
        "Negative new_success should return 400"
    );

    // Negative new_failures
    let req_neg_failures = actix_web::test::TestRequest::patch()
        .uri(&format!("/task/{}", task_id))
        .set_json(json!({"new_failures": -3}))
        .to_request();
    let resp_neg_failures = actix_web::test::call_service(&app, req_neg_failures).await;
    assert_eq!(
        resp_neg_failures.status(),
        actix_web::http::StatusCode::BAD_REQUEST,
        "Negative new_failures should return 400"
    );
}

/// Bug: PATCH /task/{id} returned 409 Conflict for non-existent task IDs.
/// The correct response is 404 Not Found.
#[tokio::test]
async fn test_bug8_update_nonexistent_task_returns_404() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let fake_id = uuid::Uuid::new_v4();
    let update_req = actix_web::test::TestRequest::patch()
        .uri(&format!("/task/{}", fake_id))
        .set_json(json!({"status": "Success"}))
        .to_request();
    let update_resp = actix_web::test::call_service(&app, update_req).await;

    assert_eq!(
        update_resp.status(),
        actix_web::http::StatusCode::NOT_FOUND,
        "Updating a non-existent task should return 404 Not Found, not 409 Conflict"
    );
}

/// Bug: A task that exists but is not Running must be distinguishable from an
/// unknown id. A10 update: it is now a **409 Conflict** carrying the current status
/// (was a generic 404) — the caller can tell "wrong state" from "wrong id".
#[tokio::test]
async fn test_bug8_update_non_running_task_returns_409() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let created = create_tasks_ok(&app, &[task_json("pending-task", "Pending Task", "test")]).await;
    let task_id = created[0].id;

    // Attempt to update a Pending task — the task exists but is not Running, so 409.
    let update_req = actix_web::test::TestRequest::patch()
        .uri(&format!("/task/{}", task_id))
        .set_json(json!({"status": "Success"}))
        .to_request();
    let update_resp = actix_web::test::call_service(&app, update_req).await;

    assert_eq!(
        update_resp.status(),
        actix_web::http::StatusCode::CONFLICT,
        "Updating a Pending (non-Running) task should return 409 with the current status"
    );
    let body: serde_json::Value = actix_web::test::read_body_json(update_resp).await;
    assert_eq!(
        body["current_status"], "Pending",
        "409 body must carry the current status, got: {}",
        body
    );
}
