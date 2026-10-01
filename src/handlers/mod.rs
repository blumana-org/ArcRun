//! HTTP handlers for ArcRun endpoints.
//!
//! This module contains all HTTP handler functions that can be used by both
//! the main application and integration tests.

mod batch;
mod dag;
mod health;
pub mod response;
mod task;
mod webhook;

use std::sync::Arc;

use actix_web::{error, web};
use tokio::sync::mpsc::Sender;

use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};
use utoipa::{Modify, OpenApi};
use utoipa_swagger_ui::SwaggerUi;

/// Registers the optional static bearer-token scheme (Audit 2, A6) on the
/// generated OpenAPI document. The scheme is only enforced at runtime when
/// `AUTH_TOKEN` is set; it is documented here unconditionally.
struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components = openapi
            .components
            .get_or_insert_with(utoipa::openapi::Components::new);
        components.add_security_scheme(
            "bearer_auth",
            SecurityScheme::Http(
                HttpBuilder::new()
                    .scheme(HttpAuthScheme::Bearer)
                    .description(Some(
                        "Optional static bearer token. When AUTH_TOKEN is configured on the \
                         server, all endpoints except /health and /ready require this token.",
                    ))
                    .build(),
            ),
        );
    }
}

use crate::{
    DbPool,
    action::ActionExecutor,
    circuit_breaker::CircuitBreaker,
    config::Config,
    dtos, metrics,
    workers::{UpdateEvent, WorkerNudges},
};

// Re-export handlers for route configuration
pub use batch::{get_batch_stats, list_batches, stop_batch, update_batch_rules};
pub use dag::{get_dag, view_dag_page};
pub use health::{favicon, health_check, readiness_check};
pub use task::{
    add_task, batch_task_updater, cancel_task, get_task, list_task, pause_task, resume_task,
    update_task,
};
pub use webhook::list_webhook_deliveries;

/// Shared application state
#[derive(Clone)]
pub struct AppState {
    pub action_executor: ActionExecutor,
    pub pool: DbPool,
    pub sender: Sender<UpdateEvent>,
    pub config: Arc<Config>,
    pub circuit_breaker: Arc<CircuitBreaker>,
    /// In-process wakeups for the start/delivery worker loops (Audit 2, B4). Handlers
    /// nudge the relevant loop after a committing transaction so DAG edges don't each
    /// pay a full poll tick. Best-effort: the poll is the correctness (see
    /// `WorkerNudges`).
    pub nudges: WorkerNudges,
}

impl AppState {
    /// Get a database connection with retry logic and circuit breaker protection.
    pub async fn conn(&self) -> Result<crate::Conn<'_>, actix_web::Error> {
        get_conn_with_retry(
            &self.pool,
            &self.circuit_breaker,
            self.config.pool.acquire_retries,
            self.config.pool.retry_delay.as_millis() as u64,
        )
        .await
    }
}

/// Health check response showing service and database status.
#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct HealthResponse {
    /// Overall service status: "ok" or "degraded"
    pub status: String,
    /// Database connectivity status: "healthy" or error details
    pub database: String,
    /// Total number of connections in the pool
    pub pool_size: u32,
    /// Number of idle (available) connections in the pool
    pub pool_idle: u32,
}

/// Helper to get a connection from pool with retry logic and circuit breaker protection
pub async fn get_conn_with_retry<'a>(
    pool: &'a DbPool,
    circuit_breaker: &CircuitBreaker,
    max_retries: u32,
    retry_delay_ms: u64,
) -> Result<crate::Conn<'a>, actix_web::Error> {
    // Check circuit breaker first
    if let Err(state) = circuit_breaker.check() {
        log::warn!("Circuit breaker is {:?}, rejecting request", state);
        metrics::record_circuit_breaker_rejection();
        return Err(error::ErrorServiceUnavailable(
            "Service temporarily unavailable (circuit breaker open)",
        ));
    }

    let mut last_error = None;

    let effective_retries = max_retries.max(1);
    let acquire_start = std::time::Instant::now();

    for attempt in 0..effective_retries {
        match pool.get().await {
            Ok(conn) => {
                // Record success for circuit breaker
                circuit_breaker.record_success();
                metrics::record_db_pool_acquire_wait(acquire_start.elapsed().as_secs_f64());
                return Ok(conn);
            }
            Err(e) => {
                // Record EACH failed acquisition (Audit 2, B7), not just once after
                // all retries are exhausted. With a 30s pool timeout and 3 retries,
                // the old "count once at the end" meant a single request's ~90s of
                // retries produced ONE failure — which could almost never land two
                // failures inside the 10s counting window, so the breaker never
                // tripped sequentially. Counting per attempt lets consecutive
                // failures accumulate within the window and actually open the circuit.
                circuit_breaker.record_failure();
                last_error = Some(e);
                if attempt + 1 < effective_retries {
                    log::warn!(
                        "Failed to acquire connection (attempt {}/{}), retrying in {}ms",
                        attempt + 1,
                        effective_retries,
                        retry_delay_ms
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(retry_delay_ms)).await;
                }
            }
        }
    }

    let err = last_error.unwrap();
    log::error!(
        "Failed to acquire connection after {} attempts: {}",
        effective_retries,
        err
    );
    metrics::record_db_pool_acquire_failure();
    Err(error::ErrorServiceUnavailable(
        "Database connection unavailable",
    ))
}

// =============================================================================
// OpenAPI Documentation
// =============================================================================

#[derive(OpenApi)]
#[openapi(
    paths(
        health::health_check,
        health::readiness_check,
        task::list_task,
        task::add_task,
        task::get_task,
        task::update_task,
        task::batch_task_updater,
        task::cancel_task,
        task::pause_task,
        task::resume_task,
        batch::get_batch_stats,
        batch::stop_batch,
        batch::update_batch_rules,
        batch::list_batches,
        webhook::list_webhook_deliveries,
        dag::get_dag,
        dag::view_dag_page,
    ),
    modifiers(&SecurityAddon),
    components(schemas(
        HealthResponse,
        dtos::NewTaskDto,
        dtos::CreateTaskBody,
        dtos::CreateTaskBatchDto,
        dtos::TaskDto,
        dtos::BasicTaskDto,
        dtos::UpdateTaskDto,
        dtos::NewActionDto,
        dtos::ActionDto,
        dtos::Dependency,
        dtos::LinkDto,
        dtos::DagDto,
        dtos::StopBatchResponseDto,
        dtos::UpdateBatchRulesDto,
        dtos::UpdateBatchRulesResponseDto,
        dtos::BatchSummaryDto,
        dtos::BatchStatsDto,
        dtos::BatchStatusCounts,
        dtos::WebhookDeliveryDto,
        crate::models::StatusKind,
        crate::models::WebhookExecutionStatus,
        crate::models::ActionKindEnum,
        crate::models::TriggerKind,
        crate::models::TriggerCondition,
        crate::rule::Strategy,
        crate::rule::Rules,
        crate::rule::ConcurencyRule,
        crate::rule::CapacityRule,
        crate::rule::Matcher,
        crate::action::HttpVerb,
        crate::action::WebhookParams,
    )),
    tags(
        (name = "health", description = "Health and readiness probes. Use GET /health for liveness and GET /ready for readiness."),
        (name = "tasks", description = "Task CRUD and lifecycle management. Tasks are created in batches via POST /task, forming a DAG. The worker loop picks up Pending tasks and calls their on_start webhook. External systems report completion via PATCH or PUT on /task/{task_id}."),
        (name = "batches", description = "Batch discovery. List all batches with aggregated task statistics, supporting filtering and pagination."),
        (name = "webhooks", description = "Webhook delivery observability. Inspect the transactional outbox of end/cancel webhook notifications, filtered by status (e.g. exhausted deliveries)."),
        (name = "dag", description = "DAG visualization. Retrieve the full graph (tasks + dependency links) for a batch, or render the built-in HTML viewer."),
    ),
    info(
        title = "ArcRun API",
        version = "1.3.0",
        description = include_str!("../../docs/openapi_description.md"),
    )
)]
pub struct ApiDoc;

// =============================================================================
// Route Configuration
// =============================================================================

/// Configure all routes for the application.
/// This can be used by both the main application and integration tests.
pub fn configure_routes(cfg: &mut web::ServiceConfig) {
    cfg.route("/health", web::get().to(health_check))
        .route("/ready", web::get().to(readiness_check))
        .route("/task", web::get().to(list_task))
        .route("/task", web::post().to(add_task))
        .route("/task/{task_id}", web::get().to(get_task))
        .route("/task/{task_id}", web::patch().to(update_task))
        .route("/task/{task_id}", web::put().to(batch_task_updater))
        .route("/task/{task_id}", web::delete().to(cancel_task))
        .route("/task/pause/{task_id}", web::patch().to(pause_task))
        .route("/task/resume/{task_id}", web::patch().to(resume_task))
        .route("/batch/{batch_id}", web::get().to(get_batch_stats))
        .route("/batch/{batch_id}", web::delete().to(stop_batch))
        .route(
            "/batch/{batch_id}/rules",
            web::patch().to(update_batch_rules),
        )
        .route("/batches", web::get().to(list_batches))
        .route(
            "/webhook-deliveries",
            web::get().to(list_webhook_deliveries),
        )
        .route("/dag/{batch_id}", web::get().to(get_dag))
        .route("/view", web::get().to(view_dag_page))
        .route("/icon.png", web::get().to(favicon))
        .route("/favicon.ico", web::get().to(favicon))
        .service(
            SwaggerUi::new("/swagger-ui/{_:.*}").url("/api-docs/openapi.json", ApiDoc::openapi()),
        );
}
