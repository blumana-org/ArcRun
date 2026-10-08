use crate::common::*;

use arcrun::dtos::TaskDto;
use serde_json::json;

/// Poll GET /task/{id} until both counters reach the expected values, or panic
/// after a generous deadline. The batch updater flushes asynchronously (100 ms
/// interval in the test config), so a fixed sleep is flaky under a loaded
/// parallel suite — the flush tick + pool acquisition can easily exceed it.
/// Polling asserts the same contract (the counts eventually land) without
/// depending on scheduler timing.
async fn wait_for_counters<S, B>(
    app: &S,
    task_id: uuid::Uuid,
    expected_success: i32,
    expected_failures: i32,
    what: &str,
) -> TaskDto
where
    S: actix_web::dev::Service<
            actix_http::Request,
            Response = actix_web::dev::ServiceResponse<B>,
            Error = actix_web::Error,
        >,
    B: actix_web::body::MessageBody,
{
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let task: TaskDto = get_task_ok(app, task_id).await;
        if task.success == expected_success && task.failures == expected_failures {
            return task;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{what}: counters did not reach ({expected_success}, {expected_failures}) \
             within 10s — last seen ({}, {})",
            task.success,
            task.failures
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn test_batch_update_increments_counters() {
    let (_g, test_state) = setup_test_app_with_batch_updater().await;
    let state = test_state.state;
    let app = test_service!(state);

    let tasks = vec![task_json("batch-test", "Batch Update Test", "batch-test")];
    let created = create_tasks_ok(&app, &tasks).await;
    let task_id = created[0].id;

    assert_eq!(created[0].success, 0);
    assert_eq!(created[0].failures, 0);

    // Send batch update via PUT endpoint
    let update_req = actix_web::test::TestRequest::put()
        .uri(&format!("/task/{}", task_id))
        .set_json(json!({"new_success": 5, "new_failures": 2}))
        .to_request();
    let update_resp = actix_web::test::call_service(&app, update_req).await;
    assert_eq!(
        update_resp.status(),
        actix_web::http::StatusCode::ACCEPTED,
        "Batch update should be accepted"
    );

    // Wait (poll, not a fixed sleep — the async flush is timing-sensitive under a
    // loaded suite) for the batch updater to persist the counts.
    wait_for_counters(&app, task_id, 5, 2, "increments_counters").await;
}

#[tokio::test]
async fn test_batch_update_accumulates_multiple_updates() {
    let (_g, test_state) = setup_test_app_with_batch_updater().await;
    let state = test_state.state;
    let app = test_service!(state);

    let tasks = vec![task_json("batch-multi", "Batch Multi Test", "batch-test")];
    let created = create_tasks_ok(&app, &tasks).await;
    let task_id = created[0].id;

    // Send multiple rapid updates
    for _ in 0..10 {
        let update_req = actix_web::test::TestRequest::put()
            .uri(&format!("/task/{}", task_id))
            .set_json(json!({"new_success": 1, "new_failures": 0}))
            .to_request();
        actix_web::test::call_service(&app, update_req).await;
    }

    // Wait (poll) for the batch updater to persist the accumulated counts.
    wait_for_counters(&app, task_id, 10, 0, "accumulates_multiple_updates").await;
}

#[tokio::test]
async fn test_batch_update_rejects_zero_counters() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let tasks = vec![task_json("batch-reject", "Batch Reject Test", "batch-test")];
    let created = create_tasks_ok(&app, &tasks).await;
    let task_id = created[0].id;

    // Try to send update with zero counters
    let update_req = actix_web::test::TestRequest::put()
        .uri(&format!("/task/{}", task_id))
        .set_json(json!({"new_success": 0, "new_failures": 0}))
        .to_request();
    let update_resp = actix_web::test::call_service(&app, update_req).await;

    assert_eq!(
        update_resp.status(),
        actix_web::http::StatusCode::BAD_REQUEST,
        "Should reject updates with all zero counters"
    );
}

/// Terminal tasks have FROZEN counters — a late batch flush must NOT mutate them.
///
/// Contract change (audit 2, A7): the batch-updater flush is now gated by
/// `AND task.status NOT IN ('success','failure','canceled')`. A PUT whose counts
/// land after the task became terminal (Success/Canceled) is silently dropped,
/// because a terminal task's counters are immutable and were already delivered
/// with its end notification — re-applying them would diverge from what consumers
/// observed. This intentionally reverses commit 2e50620 ("ensure we don't skip
/// the last updates"), which had removed the guard so racing counts still landed;
/// audit A7 re-decided the trade-off in favor of terminal immutability. The PUT
/// is still accepted (202) so callers racing the transition are not surprised by
/// an error; the counts simply have no effect once the task is terminal.
#[tokio::test]
async fn test_batch_update_dropped_on_terminal_tasks() {
    let (_g, test_state) = setup_test_app_with_batch_updater().await;
    let state = test_state.state;
    let app = test_service!(state);

    // Success case: counters must NOT change after the task is terminal.
    let tasks = vec![task_json(
        "terminal-success",
        "Terminal Success",
        "batch-test",
    )];
    let created = create_tasks_ok(&app, &tasks).await;
    let task_id = created[0].id;

    succeed_task(&state, task_id).await;
    let before: TaskDto = get_task_ok(&app, task_id).await;

    let update_req = actix_web::test::TestRequest::put()
        .uri(&format!("/task/{}", task_id))
        .set_json(json!({"new_success": 3, "new_failures": 1}))
        .to_request();
    let update_resp = actix_web::test::call_service(&app, update_req).await;
    assert_eq!(
        update_resp.status(),
        actix_web::http::StatusCode::ACCEPTED,
        "Batch update is still accepted even if task is terminal"
    );

    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    let updated: TaskDto = get_task_ok(&app, task_id).await;
    assert_eq!(
        updated.success, before.success,
        "Success counter must NOT change on a terminal (Success) task"
    );
    assert_eq!(
        updated.failures, before.failures,
        "Failure counter must NOT change on a terminal (Success) task"
    );

    // Canceled case: counters must NOT change after cancellation.
    let tasks = vec![task_json(
        "terminal-canceled",
        "Terminal Canceled",
        "batch-test",
    )];
    let created = create_tasks_ok(&app, &tasks).await;
    let cancel_id = created[0].id;

    let cancel_req = actix_web::test::TestRequest::delete()
        .uri(&format!("/task/{}", cancel_id))
        .to_request();
    let cancel_resp = actix_web::test::call_service(&app, cancel_req).await;
    assert!(cancel_resp.status().is_success());
    let before: TaskDto = get_task_ok(&app, cancel_id).await;

    let update_req = actix_web::test::TestRequest::put()
        .uri(&format!("/task/{}", cancel_id))
        .set_json(json!({"new_success": 2, "new_failures": 2}))
        .to_request();
    let update_resp = actix_web::test::call_service(&app, update_req).await;
    assert_eq!(
        update_resp.status(),
        actix_web::http::StatusCode::ACCEPTED,
        "Batch update is still accepted even if task is canceled"
    );

    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    let updated: TaskDto = get_task_ok(&app, cancel_id).await;
    assert_eq!(
        updated.success, before.success,
        "Success counter must NOT change on a terminal (Canceled) task"
    );
    assert_eq!(
        updated.failures, before.failures,
        "Failure counter must NOT change on a terminal (Canceled) task"
    );
}
