// Diesel QueryableByName emits separate impls with explicit field initializers;
// an allow on the struct does not cover those generated impls.
#![allow(clippy::redundant_field_names)]

use actix_http::Request;
use actix_web::body::MessageBody;
use actix_web::dev::{Service, ServiceResponse};
use arcrun::dtos::{BasicTaskDto, TaskDto};
use arcrun::handlers::AppState;
use arcrun::models::StatusKind;

use super::setup::{TestApp, setup_test_db};
use super::state::{
    TestStateWithBatchUpdater, create_test_state, create_test_state_with_batch_updater,
};

/// Setup a complete test app: database + state. Returns (TestApp guard, AppState).
/// The TestApp must be kept alive (_g pattern) to keep the container running.
pub async fn setup_test_app() -> (TestApp, AppState) {
    let test_app = setup_test_db().await;
    let state = create_test_state(test_app.pool.clone());
    (test_app, state)
}

/// Setup a test app with a running batch updater.
pub async fn setup_test_app_with_batch_updater() -> (TestApp, TestStateWithBatchUpdater) {
    let test_app = setup_test_db().await;
    let test_state = create_test_state_with_batch_updater(test_app.pool.clone());
    (test_app, test_state)
}

/// POST /task with given tasks, assert 201, return deserialized Vec<BasicTaskDto>.
pub async fn create_tasks_ok<S, B>(app: &S, tasks: &[serde_json::Value]) -> Vec<BasicTaskDto>
where
    S: Service<Request, Response = ServiceResponse<B>, Error = actix_web::Error>,
    B: MessageBody,
{
    let req = actix_web::test::TestRequest::post()
        .uri("/task")
        .insert_header(("requester", "test"))
        .set_json(tasks)
        .to_request();
    let resp = actix_web::test::call_service(app, req).await;
    assert_eq!(
        resp.status(),
        actix_web::http::StatusCode::CREATED,
        "POST /task should return 201 Created"
    );
    actix_web::test::read_body_json(resp).await
}

/// GET /task/{id}, assert 200, return deserialized TaskDto.
pub async fn get_task_ok<S, B>(app: &S, task_id: uuid::Uuid) -> TaskDto
where
    S: Service<Request, Response = ServiceResponse<B>, Error = actix_web::Error>,
    B: MessageBody,
{
    let req = actix_web::test::TestRequest::get()
        .uri(&format!("/task/{}", task_id))
        .to_request();
    let resp = actix_web::test::call_service(app, req).await;
    assert!(
        resp.status().is_success(),
        "GET /task/{} returned {}",
        task_id,
        resp.status()
    );
    actix_web::test::read_body_json(resp).await
}

/// Assert a task has the expected status.
pub async fn assert_task_status<S, B>(app: &S, task_id: uuid::Uuid, expected: StatusKind, msg: &str)
where
    S: Service<Request, Response = ServiceResponse<B>, Error = actix_web::Error>,
    B: MessageBody,
{
    let task = get_task_ok(app, task_id).await;
    assert_eq!(task.status, expected, "{}", msg);
}

/// Claim a task (Pending -> Running) then update it to the given terminal status.
pub async fn claim_and_complete(
    state: &AppState,
    task_id: uuid::Uuid,
    status: StatusKind,
    failure_reason: Option<String>,
) {
    let mut conn = state.pool.get().await.unwrap();
    let claimed = arcrun::db_operation::claim_task(&mut conn, &task_id)
        .await
        .unwrap();
    assert!(claimed, "Failed to claim task {}", task_id);
    let marked = arcrun::db_operation::mark_task_running(&mut conn, &task_id)
        .await
        .unwrap();
    assert!(marked, "Failed to mark task {} as running", task_id);
    let dto = arcrun::dtos::UpdateTaskDto {
        status: Some(status),
        metadata: None,
        new_success: None,
        new_failures: None,
        failure_reason,
        expected_count: None,
        priority: None,
    };
    let result = arcrun::db_operation::update_running_task(
        &mut conn, task_id, dto, true, // dead_end_cancel_enabled
    )
    .await
    .unwrap();
    assert_eq!(
        result,
        arcrun::db_operation::UpdateTaskResult::Updated,
        "Failed to complete task {}",
        task_id
    );
}

/// Shortcut: claim task then mark as Success.
pub async fn succeed_task(state: &AppState, task_id: uuid::Uuid) {
    claim_and_complete(state, task_id, StatusKind::Success, None).await;
}

/// Shortcut: claim task then mark as Failure with the given reason.
pub async fn fail_task(state: &AppState, task_id: uuid::Uuid, reason: &str) {
    claim_and_complete(
        state,
        task_id,
        StatusKind::Failure,
        Some(reason.to_string()),
    )
    .await;
}

/// Helper struct for reading raw wait counters directly from DB.
#[derive(diesel::QueryableByName, Debug)]
pub struct TaskCounters {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    pub wait_finished: i32,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    pub wait_success: i32,
}

/// Drive the webhook delivery loop (outbox) deterministically until it has no
/// more mature rows to process (or `max_passes` is reached). Returns the total
/// number of outbox rows delivered across all passes.
///
/// Use this in tests after a status transition to deliver end/cancel webhooks,
/// which are now enqueued into the transactional outbox rather than fired inline.
pub async fn drain_outbox(state: &AppState) -> usize {
    drain_outbox_with(state, default_delivery_cfg(), 20).await
}

/// Default delivery config for tests (high max_attempts, small backoff).
pub fn default_delivery_cfg() -> arcrun::workers::DeliveryConfig {
    arcrun::workers::DeliveryConfig {
        batch_size: 100,
        max_attempts: 10,
        backoff_base_secs: 2,
        backoff_cap_secs: 300,
        lease_secs: 120,
        concurrency: 10,
        start_stale_secs: 30,
    }
}

/// Like `drain_outbox` but with an explicit `DeliveryConfig` and pass cap.
pub async fn drain_outbox_with(
    state: &AppState,
    cfg: arcrun::workers::DeliveryConfig,
    max_passes: usize,
) -> usize {
    let mut total = 0usize;
    for _ in 0..max_passes {
        let mut conn = state.pool.get().await.unwrap();
        let n = arcrun::workers::run_delivery_once(&state.action_executor, &mut conn, cfg)
            .await
            .expect("run_delivery_once should not error");
        total += n;
        if n == 0 {
            break;
        }
    }
    total
}

/// Read wait_finished and wait_success counters directly from the task table.
pub async fn read_wait_counters(pool: &arcrun::DbPool, task_id: uuid::Uuid) -> (i32, i32) {
    let mut conn = pool.get().await.unwrap();
    let c: TaskCounters = diesel_async::RunQueryDsl::get_result(
        diesel::sql_query("SELECT wait_finished, wait_success FROM task WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(task_id),
        &mut *conn,
    )
    .await
    .unwrap();
    (c.wait_finished, c.wait_success)
}
