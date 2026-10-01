use chrono::Utc;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    models::{
        Action, ActionKindEnum, StatusKind, Task, TriggerCondition, TriggerKind, WebhookExecution,
        WebhookExecutionStatus,
    },
    rule::{Matcher, Rules},
};

/// Input DTO for creating a new task. Tasks are always created in batches via `POST /task`.
///
/// ## Webhook callback mechanism
/// When the worker picks up a `Pending` task, it calls the `on_start` webhook with a
/// `?handle=<host>/task/<uuid>` query parameter. Your service uses this URL to report
/// completion via `PATCH` (status update) or `PUT` (counter update).
///
/// ## Example
/// ```json
/// {
///   "id": "build",
///   "name": "Build Application",
///   "kind": "ci",
///   "timeout": 300,
///   "metadata": {"project": "my-app", "branch": "main"},
///   "on_start": {
///     "kind": "Webhook",
///     "params": {"url": "https://ci.example.com/build", "verb": "Post", "body": {"ref": "main"}}
///   },
///   "on_success": [
///     {"kind": "Webhook", "params": {"url": "https://slack.example.com/notify", "verb": "Post"}}
///   ]
/// }
/// ```
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct NewTaskDto {
    /// Local identifier for this task within the batch. This is NOT a UUID — it's a string you
    /// choose (e.g. "build", "deploy", "step-1") used only to wire dependencies between tasks
    /// in the same request. The server assigns the real UUID. Other tasks reference this id in
    /// their `dependencies` array.
    pub id: String,

    /// Human-readable name for the task. Must be non-empty, max 255 UTF-8 bytes.
    pub name: String,

    /// Task category/type. Used for concurrency rule matching and filtering. Must be non-empty,
    /// max 100 UTF-8 bytes. Examples: "ci", "deploy", "clustering", "etl".
    pub kind: String,
    /// Maximum inactivity time in seconds while Running, measured from last_updated.
    /// Must be positive, max 86400 (24 hours). Defaults to 60 seconds.
    pub timeout: Option<i32>,
    /// Arbitrary JSON metadata attached to the task. Used for concurrency rule matching (via
    /// Matcher.fields) and deduplication. Max 64KB. Example: `{"projectId": "abc", "env": "prod"}`
    pub metadata: Option<serde_json::Value>,
    /// Deduplication strategy. If any matcher matches an existing task in the database (same status,
    /// kind, and metadata field values), this task is skipped (not created). Useful to avoid
    /// duplicate task creation in retry scenarios.
    pub dedupe_strategy: Option<Vec<Matcher>>,

    /// Concurrency rules for this task. Each rule is a Strategy that can limit how many tasks of a
    /// given kind can run concurrently. The worker checks these rules before starting a Pending task.
    pub rules: Option<Rules>,

    /// Webhook action called after the worker claims this task (Pending -> Claimed).
    /// The webhook receives a `?handle=<callback_url>` query parameter for reporting completion.
    /// The webhook response body can optionally return a `NewActionDto` JSON to register a cancel action.
    pub on_start: NewActionDto,

    /// Dependencies on other tasks in the same batch. If specified, this task starts as `Waiting`
    /// and transitions to `Pending` only when all dependencies are met.
    pub dependencies: Option<Vec<Dependency>>,

    /// Expected item count for progress tracking and Capacity rules. The task is NOT
    /// auto-completed when `success + failures` reaches this value. Use it to compute
    /// `progress = (success + failures) / expected_count`. Must be >= 0 if provided.
    pub expected_count: Option<i32>,

    /// Webhook actions called when this task ends with `Failure` status. Multiple actions supported.
    pub on_failure: Option<Vec<NewActionDto>>,
    /// Webhook actions called when this task ends with `Success` status. Multiple actions supported.
    pub on_success: Option<Vec<NewActionDto>>,

    /// If true, this task acts as a dead-end barrier: it can be canceled by dead-end detection,
    /// but the upward cascade stops here — its parents are NOT checked. Defaults to false.
    pub dead_end_barrier: Option<bool>,

    /// Scheduling priority. Higher values are processed first. Range: -1000 to 1000.
    /// Defaults to 0. Tasks with equal priority are processed in FIFO order.
    pub priority: Option<i32>,
}

/// Object form of the `POST /task` body. Carries the task array plus an optional
/// batch-level `on_batch_complete` event enqueued when all inserted tasks are terminal.
/// Notification delivery may repeat.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateTaskBatchDto {
    /// The tasks to create (same semantics as the bare-array form).
    pub tasks: Vec<NewTaskDto>,
    /// Webhook actions queued as one event when the last inserted task becomes
    /// terminal. Delivery may repeat; receivers must deduplicate. No `?handle=` param is sent;
    /// the request body carries an `arcrun` object with `batch_id`, per-status
    /// `counts`, and `completed_at`.
    pub on_batch_complete: Option<Vec<NewActionDto>>,
    /// Optional business-level label attached to the batch. Filterable via
    /// `GET /batches?scope=...` (exact match) and searchable via `?search=...`.
    pub scope: Option<String>,
    /// Optional arbitrary structured metadata attached to the batch. Filterable via
    /// `GET /batches?metadata={...}` (JSONB containment) and searchable via `?search=...`.
    pub metadata: Option<serde_json::Value>,
}

/// The batch-level parts extracted from a `POST /task` body.
pub struct BatchParts {
    pub tasks: Vec<NewTaskDto>,
    pub on_batch_complete: Option<Vec<NewActionDto>>,
    pub scope: Option<String>,
    pub metadata: Option<serde_json::Value>,
}

/// The accepted shapes of the `POST /task` request body.
///
/// Untagged + backwards compatible: the existing bare array `[NewTaskDto, …]` still
/// is supported, or an object `{ "tasks": [...], "on_batch_complete": [...] }`.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum CreateTaskBody {
    /// Object form: tasks + optional batch-complete webhook.
    WithBatch(CreateTaskBatchDto),
    /// Legacy bare-array form.
    Bare(Vec<NewTaskDto>),
}

impl CreateTaskBody {
    /// Split the body into its task list and batch-level parts. The legacy bare-array
    /// form carries no batch-level fields.
    pub fn into_parts(self) -> BatchParts {
        match self {
            CreateTaskBody::Bare(tasks) => BatchParts {
                tasks,
                on_batch_complete: None,
                scope: None,
                metadata: None,
            },
            CreateTaskBody::WithBatch(b) => BatchParts {
                tasks: b.tasks,
                on_batch_complete: b.on_batch_complete,
                scope: b.scope,
                metadata: b.metadata,
            },
        }
    }
}

/// A dependency on another task within the same batch.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct Dependency {
    /// The local `id` of the parent task (as defined in its `NewTaskDto.id` field).
    pub id: String,
    /// If true, this child task will be marked as `Failure` (and propagated to its own children)
    /// if the parent fails or is canceled. If false, the child only waits for the parent to
    /// finish (regardless of success or failure) before becoming Pending.
    pub requires_success: bool,
}

/// Lightweight task representation returned by list endpoints and batch creation.
/// Does not include actions, rules, or metadata for performance.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct BasicTaskDto {
    /// Server-assigned UUID. Use this for all subsequent API calls (GET, PATCH, PUT, DELETE).
    pub id: uuid::Uuid,
    /// Human-readable name.
    pub name: String,
    /// Task category used for filtering and concurrency rules.
    pub kind: String,
    /// Current status in the task lifecycle.
    pub status: StatusKind,
    /// When the task was created.
    pub created_at: chrono::DateTime<Utc>,
    /// When the worker started the task (called on_start webhook). Null if not yet started.
    pub started_at: Option<chrono::DateTime<Utc>>,
    /// Accumulated success counter (incremented via PUT endpoint).
    pub success: i32,
    /// Accumulated failure counter (incremented via PUT endpoint).
    pub failures: i32,
    /// Expected total count for progress tracking (`progress = (success + failures) / expected_count`).
    pub expected_count: Option<i32>,
    /// When the task reached a terminal state (Success, Failure, Canceled). Null if still active.
    pub ended_at: Option<chrono::DateTime<Utc>>,
    /// The batch this task belongs to. All tasks created in the same POST /task call share a batch_id.
    pub batch_id: Option<uuid::Uuid>,
    /// If true, dead-end upward cascade stops at this task.
    pub dead_end_barrier: bool,
    /// Scheduling priority. Higher values are processed first.
    pub priority: i32,
}

/// Targeted `SELECT` projection for building a [`BasicTaskDto`].
///
/// Listings and `get_dag_for_batch` produce `BasicTaskDto`s, which carry none of
/// the heavy JSONB columns (`metadata`, `start_condition` — up to 64 KiB each).
/// Selecting the full `task` row over-fetched those blobs on every listed/DAG
/// row for nothing. This struct selects only the columns `BasicTaskDto` actually
/// consumes, so the query never reads the JSONB columns off disk.
#[derive(diesel::prelude::Queryable, diesel::prelude::Selectable)]
#[diesel(table_name = crate::schema::task)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct BasicTaskRow {
    pub id: uuid::Uuid,
    pub name: String,
    pub kind: String,
    pub status: StatusKind,
    pub created_at: chrono::DateTime<Utc>,
    pub started_at: Option<chrono::DateTime<Utc>>,
    pub success: i32,
    pub failures: i32,
    pub expected_count: Option<i32>,
    pub ended_at: Option<chrono::DateTime<Utc>>,
    pub batch_id: Option<uuid::Uuid>,
    pub dead_end_barrier: bool,
    pub priority: i32,
}

impl From<BasicTaskRow> for BasicTaskDto {
    fn from(t: BasicTaskRow) -> Self {
        Self {
            id: t.id,
            name: t.name,
            kind: t.kind,
            status: t.status,
            created_at: t.created_at,
            started_at: t.started_at,
            success: t.success,
            failures: t.failures,
            expected_count: t.expected_count,
            ended_at: t.ended_at,
            batch_id: t.batch_id,
            dead_end_barrier: t.dead_end_barrier,
            priority: t.priority,
        }
    }
}

/// Payload for updating a task. Used by both `PATCH /task/{id}` (status update) and `PUT /task/{id}` (counter update).
///
/// ## For PATCH (status update):
/// Set `status` to `Success` or `Failure`. When `Failure`, `failure_reason` is required.
/// Other fields are optional.
///
/// ## For PUT (counter update):
/// Only `new_success` and `new_failures` are used. At least one must be non-zero.
/// The `status` field is ignored.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct UpdateTaskDto {
    /// Updated metadata. **Full replace, not a merge** — the value REPLACES the stored
    /// metadata entirely, so send the complete object to preserve existing fields.
    /// Only used by PATCH.
    pub metadata: Option<serde_json::Value>,
    /// New status. Only `Success` and `Failure` are accepted via the API. Only used by PATCH.
    pub status: Option<StatusKind>,
    /// Number of successful sub-items to add to the counter. Only used by PUT. Must be non-negative.
    pub new_success: Option<i32>,
    /// Number of failed sub-items to add to the counter. Only used by PUT. Must be non-negative.
    pub new_failures: Option<i32>,
    /// Reason for failure. Required when `status` is `Failure`, ignored otherwise.
    pub failure_reason: Option<String>,
    /// Updated expected count for progress tracking. Must be >= 0 if provided.
    pub expected_count: Option<i32>,
    /// Updated scheduling priority. Range: -1000 to 1000.
    pub priority: Option<i32>,
}

/// Input DTO for creating actions. The trigger (Start/End/Cancel) is determined by context:
/// `on_start` field -> Start trigger, `on_success` -> End+Success, `on_failure` -> End+Failure.
///
/// ## Example (Webhook action)
/// ```json
/// {
///   "kind": "Webhook",
///   "params": {
///     "url": "https://my-service.com/webhook",
///     "verb": "Post",
///     "body": {"key": "value"},
///     "headers": {"Authorization": "Bearer token123"}
///   }
/// }
/// ```
#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct NewActionDto {
    /// Action type. Currently only `Webhook` is supported.
    pub kind: ActionKindEnum,
    /// Action parameters. For `Webhook` kind, this must be a `WebhookParams` JSON object
    /// with fields: `url` (required, string), `verb` (required, one of Get/Post/Put/Patch/Delete),
    /// `body` (optional, JSON), `headers` (optional, key-value map).
    pub params: serde_json::Value,
}

/// Output DTO for a registered action on a task. Includes the trigger type (Start/End/Cancel)
/// which is determined by context during creation and stored in the database.
#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct ActionDto {
    /// Action type (currently only Webhook).
    pub kind: ActionKindEnum,
    /// When this action fires: `Start` (task begins), `End` (task completes), or `Cancel` (task canceled).
    pub trigger: TriggerKind,
    /// Action parameters (WebhookParams for Webhook kind).
    pub params: serde_json::Value,
}

/// Full task representation returned by `GET /task/{id}`. Includes all details: actions,
/// rules, metadata, counters, and timestamps.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct TaskDto {
    /// Server-assigned UUID.
    pub id: uuid::Uuid,
    /// Human-readable name.
    pub name: String,
    /// Task category.
    pub kind: String,
    /// Current lifecycle status.
    pub status: StatusKind,
    /// Timeout in seconds. Running tasks exceeding this are marked as Failure.
    pub timeout: i32,
    /// Concurrency rules (empty array means no concurrency constraints).
    pub rules: Rules,
    /// Arbitrary JSON metadata.
    pub metadata: serde_json::Value,
    /// All registered actions (on_start, on_success, on_failure, cancel).
    pub actions: Vec<ActionDto>,
    /// When the task was created.
    pub created_at: chrono::DateTime<Utc>,
    /// When the worker started the task. Null if not yet started.
    pub started_at: Option<chrono::DateTime<Utc>>,
    /// When the task reached a terminal state. Null if still active.
    pub ended_at: Option<chrono::DateTime<Utc>>,
    /// Last status or counter change timestamp.
    pub last_updated: chrono::DateTime<Utc>,
    /// Accumulated success counter.
    pub success: i32,
    /// Accumulated failure counter.
    pub failures: i32,
    /// Expected total count for progress tracking (`progress = (success + failures) / expected_count`).
    pub expected_count: Option<i32>,
    /// Reason for failure (set via API or by the system for timeouts/dependency failures).
    pub failure_reason: Option<String>,
    /// Batch UUID grouping tasks created in the same POST /task call.
    pub batch_id: Option<uuid::Uuid>,
    /// If true, dead-end upward cascade stops at this task.
    pub dead_end_barrier: bool,
    /// Scheduling priority. Higher values are processed first.
    pub priority: i32,
}

/// A webhook delivery (outbox) record, returned by `GET /webhook-deliveries`.
/// Use this for observability — e.g. listing `exhausted` deliveries that need
/// manual attention.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct WebhookDeliveryDto {
    /// Outbox row UUID.
    pub id: uuid::Uuid,
    /// The task this notification is for. Null for batch-level (`BatchComplete`) rows.
    pub task_id: Option<uuid::Uuid>,
    /// The batch this notification is for. Set only for batch-level (`BatchComplete`) rows.
    pub batch_id: Option<uuid::Uuid>,
    /// Lifecycle trigger (End, Cancel, or BatchComplete; Start rows are delivered synchronously).
    pub trigger: TriggerKind,
    /// For End triggers, whether this is the success or failure notification.
    pub condition: TriggerCondition,
    /// The `Idempotency-Key` header sent to the consumer.
    pub idempotency_key: String,
    /// Delivery status: pending, success, failure (failed start), or exhausted.
    /// Notifications awaiting retry remain pending.
    pub status: WebhookExecutionStatus,
    /// Number of delivery attempts made so far.
    pub attempts: i32,
    /// Last delivery error, if any.
    pub last_error: Option<String>,
    /// When this row becomes eligible for the next delivery attempt.
    pub next_attempt_at: chrono::DateTime<Utc>,
    /// When the outbox row was created (transition time).
    pub created_at: chrono::DateTime<Utc>,
    /// When the row was last updated (last attempt / terminal transition).
    pub updated_at: chrono::DateTime<Utc>,
}

impl From<WebhookExecution> for WebhookDeliveryDto {
    fn from(w: WebhookExecution) -> Self {
        Self {
            id: w.id,
            task_id: w.task_id,
            batch_id: w.batch_id,
            trigger: w.trigger,
            condition: w.condition,
            idempotency_key: w.idempotency_key,
            status: w.status,
            attempts: w.attempts,
            last_error: w.last_error,
            next_attempt_at: w.next_attempt_at,
            created_at: w.created_at,
            updated_at: w.updated_at,
        }
    }
}

// =============================================================================
// Conversions
// =============================================================================

impl From<Task> for BasicTaskDto {
    fn from(t: Task) -> Self {
        Self {
            id: t.id,
            name: t.name,
            kind: t.kind,
            status: t.status,
            created_at: t.created_at,
            started_at: t.started_at,
            success: t.success,
            failures: t.failures,
            expected_count: t.expected_count,
            ended_at: t.ended_at,
            batch_id: t.batch_id,
            dead_end_barrier: t.dead_end_barrier,
            priority: t.priority,
        }
    }
}

impl TaskDto {
    pub fn new(base_task: Task, actions: Vec<Action>) -> Self {
        Self {
            id: base_task.id,
            name: base_task.name,
            kind: base_task.kind,
            status: base_task.status,
            timeout: base_task.timeout,
            rules: base_task.start_condition,
            metadata: base_task.metadata,
            created_at: base_task.created_at,
            success: base_task.success,
            failures: base_task.failures,
            expected_count: base_task.expected_count,
            ended_at: base_task.ended_at,
            last_updated: base_task.last_updated,
            started_at: base_task.started_at,
            failure_reason: base_task.failure_reason,
            batch_id: base_task.batch_id,
            dead_end_barrier: base_task.dead_end_barrier,
            priority: base_task.priority,
            actions: actions
                .into_iter()
                .map(|a| ActionDto {
                    kind: a.kind,
                    params: a.params,
                    trigger: a.trigger,
                })
                .collect(),
        }
    }
}
