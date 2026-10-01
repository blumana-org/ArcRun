use actix_web::{HttpResponse, web};
use uuid::Uuid;

use crate::{db_operation, dtos, error::ApiError, metrics, validation, workers};

use super::AppState;
use super::response::validation_error_response;

#[utoipa::path(
    get,
    path = "/task",
    summary = "List tasks",
    description = "Returns a paginated list of tasks. Supports filtering by name, kind, status, batch_id, and metadata (JSONB containment). Default page_size is 50, maximum is 100. Results are returned as lightweight BasicTaskDto (no actions/rules).",
    params(dtos::PaginationDto, dtos::FilterDto),
    responses(
        (status = 200, description = "Paginated array of tasks matching the filters", body = Vec<dtos::BasicTaskDto>),
        (status = 400, description = "A filter value is invalid (e.g. `metadata` is not valid JSON)"),
    ),
    tag = "tasks"
)]
/// List tasks with filtering and pagination
pub async fn list_task(
    state: web::Data<AppState>,
    pagination: web::Query<dtos::PaginationDto>,
    filter: web::Query<dtos::FilterDto>,
) -> actix_web::Result<HttpResponse> {
    let mut conn = state.conn().await?;
    let pagination = pagination.0.resolve(&state.config);
    // A10: a malformed `metadata` filter is a 400, not a silent "return everything".
    let filter = filter.0.resolve().map_err(ApiError::BadRequest)?;

    let tasks = db_operation::list_task_filtered_paged(&mut conn, pagination, filter)
        .await
        .map_err(ApiError::from)?;
    Ok(HttpResponse::Ok().json(tasks))
}

#[utoipa::path(
    patch,
    path = "/task/{task_id}",
    summary = "Update task status (synchronous)",
    description = "Update a Running or Claimed task's status to `Success` or `Failure`. This is the primary way external systems report task completion after receiving the `on_start` webhook.

**Only `Success` and `Failure` are valid target statuses.** Setting `Failure` requires a `failure_reason`.

This endpoint commits the status change, dependency propagation, and end-notification enqueue in one transaction. The delivery loop sends `on_success`/`on_failure` webhooks asynchronously. For high-throughput counter updates, use `PUT /task/{task_id}` instead.

**Idempotent & retry-safe:** re-sending the same terminal status a task already holds returns `200` as a no-op (no duplicate webhooks or propagation), so a client whose response was lost can safely retry. A request for a *different* status on an already-terminal task returns `409` with the current status in the body; an unknown id returns `404`.

**`metadata` is a full replace, not a merge:** the value you send REPLACES the stored metadata entirely. To keep existing fields, send the complete object (partial updates drop omitted keys, including any used by dedupe/concurrency matchers).

The `task_id` is the UUID returned by `POST /task`, also available from the `?handle=` query parameter passed to your webhook.",
    params(("task_id" = Uuid, Path, description = "The UUID of the task to update (returned by POST /task or from the ?handle= webhook query param)")),
    request_body(content = dtos::UpdateTaskDto, description = "Fields to update. Set `status` to `Success` or `Failure`. When setting `Failure`, `failure_reason` is required. `metadata` fully replaces the stored value (not merged)."),
    responses(
        (status = 200, description = "Task updated, OR an idempotent no-op because the task already holds the requested status. Webhooks/propagation run only on a real transition."),
        (status = 400, description = "Validation failed (invalid status transition, missing failure_reason, etc.)"),
        (status = 404, description = "Task not found"),
        (status = 409, description = "Task exists but is neither Running nor Claimed (and not already the requested status); body includes `current_status`"),
    ),
    tag = "tasks"
)]
/// Update a running task's status
pub async fn update_task(
    state: web::Data<AppState>,
    task_id: web::Path<Uuid>,
    form: web::Json<dtos::UpdateTaskDto>,
) -> actix_web::Result<HttpResponse> {
    // Validate update DTO before processing
    if let Err(errors) = validation::validate_update_task(&form) {
        return Ok(validation_error_response(&errors));
    }

    let mut conn = state.conn().await?;

    let result = db_operation::update_running_task(
        &mut conn,
        *task_id,
        form.0,
        state.config.worker.dead_end_cancel_enabled,
    )
    .await
    .map_err(ApiError::from)?;
    Ok(match result {
        db_operation::UpdateTaskResult::Updated => {
            // B4: a real transition just committed — its end/failure outbox rows are
            // pending (nudge delivery) and its propagation may have moved children to
            // Pending (nudge start). Only on a genuine transition, never on the
            // idempotent no-op below (which enqueues/propagates nothing).
            state.nudges.nudge_start();
            state.nudges.nudge_delivery();
            HttpResponse::Ok().body("Task updated successfully")
        }
        // A10: idempotent retry — the task already sits in the requested terminal
        // status, so answer 200 without re-running propagation/outbox.
        db_operation::UpdateTaskResult::AlreadyInRequestedState => {
            HttpResponse::Ok().body("Task already in the requested state (idempotent no-op)")
        }
        db_operation::UpdateTaskResult::NotFound => {
            HttpResponse::NotFound().json(serde_json::json!({
                "error": "Task not found"
            }))
        }
        // A10: the task exists but is not updatable to the requested status; surface
        // the current status so a client can tell "already applied" from "wrong id".
        db_operation::UpdateTaskResult::Conflict(current) => {
            HttpResponse::Conflict().json(serde_json::json!({
                "error": "Task is not in a Running state",
                "current_status": current
            }))
        }
    })
}

#[utoipa::path(
    get,
    path = "/task/{task_id}",
    summary = "Get task details",
    description = "Returns full task details including actions, rules, metadata, counters, and timestamps. Use this to inspect a task's current state, its registered webhooks, and concurrency rules.",
    params(("task_id" = Uuid, Path, description = "The UUID of the task")),
    responses(
        (status = 200, description = "Full task details with actions", body = dtos::TaskDto),
        (status = 404, description = "No task found with this ID"),
    ),
    tag = "tasks"
)]
/// Get a task by ID
pub async fn get_task(
    state: web::Data<AppState>,
    task_id: web::Path<Uuid>,
) -> actix_web::Result<HttpResponse> {
    let mut conn = state.conn().await?;

    let task = db_operation::find_detailed_task_by_id(&mut conn, *task_id)
        .await
        .map_err(ApiError::from)?;

    Ok(match task {
        Some(t) => HttpResponse::Ok().json(t),
        None => HttpResponse::NotFound().body("No task found with UID"),
    })
}

#[utoipa::path(
    put,
    path = "/task/{task_id}",
    summary = "Batch counter update (async)",
    description = "Queue an incremental success/failure counter update for a task. Unlike `PATCH`, this is **asynchronous** — updates are batched and flushed to the database periodically for high throughput.

Use this when your task processes many items and you want to report progress incrementally (e.g., 'processed 10 more items successfully'). At least one of `new_success` or `new_failures` must be non-zero. The `status` field is ignored by this endpoint.

Returns 202 Accepted after queueing in memory, without checking task existence. Persistence happens in the background; an abrupt crash can lose unflushed updates. Only counter fields are applied.",
    params(("task_id" = Uuid, Path, description = "The UUID of the task to update counters for")),
    request_body(content = dtos::UpdateTaskDto, description = "Only `new_success` and `new_failures` are used. At least one must be non-zero."),
    responses(
        (status = 202, description = "Counter update queued for batch processing"),
        (status = 400, description = "Validation failed or both counters are zero"),
        (status = 500, description = "Internal error — failed to queue the update"),
    ),
    tag = "tasks"
)]
/// Push to update event queue for update batching
pub async fn batch_task_updater(
    state: web::Data<AppState>,
    task_id: web::Path<Uuid>,
    form: web::Json<dtos::UpdateTaskDto>,
) -> HttpResponse {
    // Validate only counter fields for PUT (batch counter) endpoint
    if let Err(errors) = validation::validate_update_task_counters(&form) {
        return validation_error_response(&errors);
    }

    let task_id = task_id.into_inner();
    let success = form.new_success.unwrap_or(0);
    let failures = form.new_failures.unwrap_or(0);

    if success == 0 && failures == 0 {
        return HttpResponse::BadRequest()
            .body("At least one of new_success or new_failures must be non-zero");
    }

    // Sample channel capacity *before* the send: this captures the backpressure the
    // request actually faced (a full channel that drains during our await would read
    // misleadingly high afterwards).
    let capacity_available = state.sender.capacity();
    let send_start = std::time::Instant::now();
    let result = state
        .sender
        .send(workers::UpdateEvent {
            success,
            failures,
            task_id,
        })
        .await;
    metrics::record_batch_channel_send(send_start.elapsed().as_secs_f64(), capacity_available);

    match result {
        Ok(_) => HttpResponse::Accepted().body("Queued"),
        Err(_) => HttpResponse::InternalServerError().body("Failed to queue update"),
    }
}

#[utoipa::path(
    post,
    path = "/task",
    summary = "Create task batch (DAG)",
    description = "Create one or more tasks as a batch, optionally forming a DAG via dependencies. All tasks in a single request share the same `batch_id` (returned in the `X-Batch-ID` response header).

**How dependencies work:** Each task has a local `id` (a string you choose, e.g. `\"build\"`, `\"deploy\"`). Other tasks in the same batch can reference this `id` in their `dependencies` array. Tasks with no dependencies (or whose dependencies are all met) start as `Pending`; tasks with unmet dependencies start as `Waiting`.

**Deduplication:** If a task's `dedupe_strategy` matches an existing task in the database (by status, kind, and metadata fields), that task is skipped (not created). If all tasks are deduplicated, the response is 204 No Content.

**Validation:** The entire batch is validated before any inserts. Circular dependencies, invalid webhook URLs, empty names/kinds, and SSRF attempts are rejected with 400.

**Transaction:** All tasks are created in a single database transaction — either all succeed or none are created.",
    request_body(content = dtos::CreateTaskBody, description = "Either a bare array of tasks `[NewTaskDto, …]`, or an object `{ \"tasks\": [NewTaskDto, …], \"on_batch_complete\": [NewActionDto, …] }`. Order matters: a task can only depend on tasks defined earlier in the array. `scope` and `metadata` can attach batch identity. `on_batch_complete` enqueues one event when all inserted tasks are terminal; HTTP delivery may repeat."),
    responses(
        (status = 201, description = "Tasks created successfully. Response body is the array of created tasks with their server-assigned UUIDs. The `X-Batch-ID` header contains the batch UUID.", body = Vec<dtos::BasicTaskDto>),
        (status = 204, description = "All tasks were deduplicated — nothing was created. The `X-Batch-ID` header is still returned."),
        (status = 400, description = "Validation failed. Response body contains `error`, `batch_id`, and `details` (array of error strings)."),
    ),
    tag = "tasks"
)]
/// Create new tasks (batch)
pub async fn add_task(
    state: web::Data<AppState>,
    form: web::Json<dtos::CreateTaskBody>,
) -> actix_web::Result<HttpResponse> {
    // Generate batch_id for tracing this entire DAG
    let batch_id = Uuid::now_v7();

    // Accept both the legacy bare array and the object form
    // `{ "tasks": [...], "on_batch_complete": [...], "scope": ..., "metadata": ... }`.
    let dtos::BatchParts {
        tasks: f,
        on_batch_complete,
        scope: batch_scope,
        metadata: batch_metadata,
    } = form.0.into_parts();

    log::info!(
        "[batch_id={}] Creating task batch from requester={}, task_count={}, on_batch_complete={}",
        batch_id,
        "",
        f.len(),
        on_batch_complete.is_some()
    );

    // Validate the entire batch BEFORE acquiring a connection
    if let Err(errors) = validation::validate_task_batch(&f) {
        let error_messages: Vec<String> = errors.iter().map(|e| e.to_string()).collect();
        log::warn!(
            "[batch_id={}] Task batch validation failed: {:?}",
            batch_id,
            error_messages
        );
        return Ok(HttpResponse::BadRequest().json(serde_json::json!({
            "error": "Validation failed",
            "batch_id": batch_id,
            "details": error_messages
        })));
    }

    // Validate the batch-complete actions (SSRF etc.) the same way as task actions.
    if let Some(ref actions) = on_batch_complete {
        let max_actions = validation::get_limits_config().max_actions_per_task;
        if actions.len() > max_actions {
            return Ok(HttpResponse::BadRequest().json(serde_json::json!({
                "error": "Validation failed",
                "batch_id": batch_id,
                "details": [format!(
                    "on_batch_complete cannot exceed {} actions (received {})",
                    max_actions,
                    actions.len()
                )]
            })));
        }
        for (i, action) in actions.iter().enumerate() {
            if let Err(e) = validation::validate_action_params(&action.kind, &action.params) {
                let msg = format!("on_batch_complete[{}].params: {}", i, e);
                log::warn!(
                    "[batch_id={}] Batch-complete action validation failed: {}",
                    batch_id,
                    msg
                );
                return Ok(HttpResponse::BadRequest().json(serde_json::json!({
                    "error": "Validation failed",
                    "batch_id": batch_id,
                    "details": [msg]
                })));
            }
        }
    }

    // Validate the batch-level scope/metadata (size/format) before the transaction.
    if let Err(errors) =
        validation::validate_batch_meta(batch_scope.as_deref(), batch_metadata.as_ref())
    {
        let error_messages: Vec<String> = errors.iter().map(|e| e.to_string()).collect();
        log::warn!(
            "[batch_id={}] Batch scope/metadata validation failed: {:?}",
            batch_id,
            error_messages
        );
        return Ok(HttpResponse::BadRequest().json(serde_json::json!({
            "error": "Validation failed",
            "batch_id": batch_id,
            "details": error_messages
        })));
    }

    let mut conn = state.conn().await?;

    let result = db_operation::run_in_transaction(&mut conn, |conn| {
        Box::pin(async move {
            // Create the batch row when ANY batch-level field is provided (webhook,
            // scope, or metadata). Done first so the empty-batch case still fires the
            // batch-complete signal in this same transaction. `on_complete` defaults to
            // an empty array for scope/metadata-only batches (no webhook).
            if on_batch_complete.is_some() || batch_scope.is_some() || batch_metadata.is_some() {
                let on_complete = on_batch_complete
                    .map(|actions| {
                        serde_json::to_value(&actions)
                            .unwrap_or_else(|_| serde_json::Value::Array(vec![]))
                    })
                    .unwrap_or_else(|| serde_json::Value::Array(vec![]));
                let metadata =
                    batch_metadata.unwrap_or_else(|| serde_json::Value::Object(Default::default()));
                db_operation::insert_batch(conn, batch_id, on_complete, batch_scope, metadata)
                    .await?;
            }

            // Grouped insertion (Lot 3a): contiguous dedupe-free runs are inserted in
            // a few multi-row INSERTs; dedupe tasks are still evaluated one-at-a-time.
            let result = db_operation::insert_task_batch(conn, f, Some(batch_id)).await?;

            // Initialize `batch.remaining` (D2) to the number of tasks ACTUALLY inserted
            // (dedupe-skips excluded — `result` omits them). When 0 (empty / all-deduped
            // batch) and a webhook was registered, the batch is vacuously complete and
            // the signal is enqueued now so the consumer doesn't wait forever. No-op when
            // no `batch` row was inserted.
            db_operation::init_batch_remaining(conn, batch_id, result.len() as i32, "add_task")
                .await?;

            Ok(result)
        })
    })
    .await
    .map_err(|e| {
        log::error!("[batch_id={}] Failed to create task batch: {}", batch_id, e);
        ApiError::from(e)
    })?;

    log::info!(
        "[batch_id={}] Task batch created successfully, tasks_created={}",
        batch_id,
        result.len()
    );

    if result.is_empty() {
        // Nothing schedulable was created (empty / all-dedupe-skipped batch), but the
        // transaction may have enqueued an immediate batch_complete outbox row for a
        // vacuously-complete batch — nudge the delivery loop so it fires promptly (B4).
        state.nudges.nudge_delivery();
        Ok(HttpResponse::NoContent()
            .insert_header(("X-Batch-ID", batch_id.to_string()))
            .finish())
    } else {
        // Fresh Pending tasks exist — wake the start loop instead of waiting a tick (B4).
        state.nudges.nudge_start();
        Ok(HttpResponse::Created()
            .insert_header(("X-Batch-ID", batch_id.to_string()))
            .json(result))
    }
}

#[utoipa::path(
    delete,
    path = "/task/{task_id}",
    summary = "Cancel a task",
    description = "Cancel a task and propagate cancellation to its dependents. The task is set to `Canceled` status, which behaves like `Failure` for dependency propagation — children with `requires_success=true` will also be marked as `Failure` recursively.

For Claimed/Running tasks, cancel notifications are queued transactionally. Registered `Cancel` actions (returned by the `on_start` webhook response) are delivered asynchronously.

Only tasks in `Pending`, `Waiting`, `Paused`, `Claimed`, or `Running` status can be canceled. Terminal tasks (`Success`/`Failure`/`Canceled`) cannot.",
    params(("task_id" = Uuid, Path, description = "The UUID of the task to cancel")),
    responses(
        (status = 200, description = "Task canceled and propagation completed"),
        (status = 400, description = "Task exists but is in a non-cancelable (terminal) state; the message names the current state"),
        (status = 404, description = "No task found with this ID"),
        (status = 500, description = "Internal error (e.g. database failure) while canceling"),
    ),
    tag = "tasks"
)]
/// Cancel a task
pub async fn cancel_task(
    state: web::Data<AppState>,
    task_id: web::Path<Uuid>,
) -> actix_web::Result<HttpResponse> {
    let mut conn = state.conn().await?;

    // A10: map the worker error precisely — 404 (absent), 400 (non-cancelable
    // state), 500 (DB failure) — instead of collapsing every failure to a bare 400.
    workers::cancel_task(
        &task_id,
        state.config.worker.dead_end_cancel_enabled,
        &mut conn,
    )
    .await
    .map_err(ApiError::from)?;
    // B4: the cancel + cascade committed — its cancel outbox row (if the task was
    // Running/Claimed) matures now (nudge delivery), and cascade-failed children may
    // free descendants to Pending (nudge start).
    state.nudges.nudge_start();
    state.nudges.nudge_delivery();
    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    patch,
    path = "/task/pause/{task_id}",
    summary = "Pause a task",
    description = "Set a task's status to `Paused`. Only tasks that have NOT started executing can be paused: a task in `Pending` or `Waiting` status. Pausing is atomic.

A `Paused` task is NOT scheduled by the worker loop, but it is NOT shielded from its dependencies: when a required parent fails (or is canceled) the paused task is still cascade-failed, and its dependency counters are still decremented as parents complete. A paused task whose counters reach 0 stays `Paused` — it does NOT auto-transition to `Pending`.

To resume, call `PATCH /task/resume/{task_id}` (it returns the task to `Waiting` if dependencies are still outstanding, otherwise to `Pending`). A `Running`/`Claimed` task cannot be paused (cancel it via `DELETE /task/{id}` instead); terminal tasks cannot be paused. A `Paused` task can still be canceled.",
    params(("task_id" = Uuid, Path, description = "The UUID of the task to pause")),
    responses(
        (status = 200, description = "Task paused"),
        (status = 400, description = "Task could not be paused (not in Pending/Waiting state)"),
        (status = 404, description = "Task not found"),
    ),
    tag = "tasks"
)]
/// Pause a task
pub async fn pause_task(
    state: web::Data<AppState>,
    task_id: web::Path<Uuid>,
) -> actix_web::Result<HttpResponse> {
    let mut conn = state.conn().await?;

    db_operation::pause_task(&task_id, &mut conn)
        .await
        .map_err(ApiError::from)?;
    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    patch,
    path = "/task/resume/{task_id}",
    summary = "Resume a paused task",
    description = "Resume a `Paused` task, returning it to the schedulable flow. The target state is derived atomically from the task's outstanding dependency counters: if any dependency is still unmet the task returns to `Waiting`, otherwise straight to `Pending` (where the worker loop can pick it up).

Only a `Paused` task can be resumed; any other state returns 400 (and a missing task returns 404). This is the counterpart of `PATCH /task/pause/{task_id}`.",
    params(("task_id" = Uuid, Path, description = "The UUID of the paused task to resume")),
    responses(
        (status = 200, description = "Task resumed (to Waiting or Pending depending on remaining dependencies)"),
        (status = 400, description = "Task could not be resumed (not in Paused state)"),
        (status = 404, description = "Task not found"),
    ),
    tag = "tasks"
)]
/// Resume a paused task
pub async fn resume_task(
    state: web::Data<AppState>,
    task_id: web::Path<Uuid>,
) -> actix_web::Result<HttpResponse> {
    let mut conn = state.conn().await?;

    db_operation::resume_task(&task_id, &mut conn)
        .await
        .map_err(ApiError::from)?;
    // B4: resume may have moved the task straight to Pending — wake the start loop.
    state.nudges.nudge_start();
    Ok(HttpResponse::Ok().finish())
}
