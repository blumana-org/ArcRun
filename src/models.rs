// Diesel QueryableByName emits separate impls with explicit field initializers;
// an allow on the struct does not cover those generated impls.
#![allow(clippy::redundant_field_names)]

use chrono::Utc;
// use crate::actions::ActionKindEnum;
use diesel::prelude::*;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::rule::Rules;

#[derive(Identifiable, Queryable, Selectable, Debug, Clone)]
#[diesel(table_name = crate::schema::task)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Task {
    pub id: uuid::Uuid,
    pub name: String,
    pub kind: String,
    pub status: StatusKind,
    pub timeout: i32,
    pub created_at: chrono::DateTime<Utc>,
    pub started_at: Option<chrono::DateTime<Utc>>,
    pub last_updated: chrono::DateTime<Utc>,
    pub metadata: serde_json::Value,
    pub ended_at: Option<chrono::DateTime<Utc>>,
    pub start_condition: Rules,
    pub wait_success: i32,
    pub wait_finished: i32,
    pub success: i32,
    pub failures: i32,
    pub failure_reason: Option<String>,
    pub batch_id: Option<uuid::Uuid>,
    pub expected_count: Option<i32>,
    pub dead_end_barrier: bool,
    pub priority: i32,
    /// Concurrency slot keys (Audit 2, D1) this task consumed when it was claimed,
    /// persisted so they can be released precisely on any exit from Claimed/Running.
    /// `None` = the task holds no concurrency slots. NEVER recomputed from `metadata`
    /// (which is mutable while Running) — always read back from this column.
    pub claimed_slot_keys: Option<Vec<String>>,
    /// Outstanding **capacity charge** (Audit 2, D1 / 7.3b) this task currently
    /// contributes to each of its `cap:` slot keys. Set at claim time to
    /// `GREATEST(expected_count - success - failures, 0)`, shrunk as progress is
    /// flushed (batch_updater), read back at release (NEVER recomputed — expected_count
    /// is mutable while Running), and NULLed on release. `None` = holds no capacity
    /// charge (never claimed through a Capacity rule, or already released).
    pub capacity_charge: Option<i32>,
}

/// A terminal task that the retention loop moved out of the hot `task` table into
/// the cold `task_archive` table (Audit 2, D6 / 7.5b). Mirrors every [`Task`] column
/// verbatim plus `archived_at`. Read-only: the archive is never written by any request
/// path (writes still target `task` only). Its sole reader is the `GET /task/{id}`
/// fallback, which converts it into a [`Task`] (see `From<TaskArchive> for Task`) and
/// reuses the existing task-to-DTO mapping — the archived task carries no actions
/// (they were deleted on archive), so the DTO's `actions` array is empty.
#[derive(Identifiable, Queryable, Selectable, Debug, Clone)]
#[diesel(table_name = crate::schema::task_archive)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct TaskArchive {
    pub id: uuid::Uuid,
    pub name: String,
    pub kind: String,
    pub status: StatusKind,
    pub timeout: i32,
    pub created_at: chrono::DateTime<Utc>,
    pub started_at: Option<chrono::DateTime<Utc>>,
    pub last_updated: chrono::DateTime<Utc>,
    pub metadata: serde_json::Value,
    pub ended_at: Option<chrono::DateTime<Utc>>,
    pub start_condition: Rules,
    pub wait_success: i32,
    pub wait_finished: i32,
    pub success: i32,
    pub failures: i32,
    pub failure_reason: Option<String>,
    pub batch_id: Option<uuid::Uuid>,
    pub expected_count: Option<i32>,
    pub dead_end_barrier: bool,
    pub priority: i32,
    pub claimed_slot_keys: Option<Vec<String>>,
    pub capacity_charge: Option<i32>,
    /// When the retention loop archived this task.
    pub archived_at: chrono::DateTime<Utc>,
}

impl From<TaskArchive> for Task {
    /// Project an archived row back to a [`Task`], dropping the archive-only
    /// `archived_at`. Lets the `GET /task/{id}` fallback reuse `TaskDto::new` instead
    /// of duplicating the task-to-DTO mapping.
    fn from(a: TaskArchive) -> Self {
        Task {
            id: a.id,
            name: a.name,
            kind: a.kind,
            status: a.status,
            timeout: a.timeout,
            created_at: a.created_at,
            started_at: a.started_at,
            last_updated: a.last_updated,
            metadata: a.metadata,
            ended_at: a.ended_at,
            start_condition: a.start_condition,
            wait_success: a.wait_success,
            wait_finished: a.wait_finished,
            success: a.success,
            failures: a.failures,
            failure_reason: a.failure_reason,
            batch_id: a.batch_id,
            expected_count: a.expected_count,
            dead_end_barrier: a.dead_end_barrier,
            priority: a.priority,
            claimed_slot_keys: a.claimed_slot_keys,
            capacity_charge: a.capacity_charge,
        }
    }
}

#[derive(Identifiable, Queryable, Associations, Selectable, PartialEq, Clone)]
#[diesel(
    table_name = crate::schema::action,
    belongs_to(Task),
    check_for_backend(diesel::pg::Pg))
]
pub struct Action {
    pub id: uuid::Uuid,
    pub task_id: uuid::Uuid,
    pub kind: ActionKindEnum,
    pub params: serde_json::Value,
    pub trigger: TriggerKind,
    pub condition: TriggerCondition,
    pub success: Option<bool>,
}

/// New task details.
#[derive(Debug, Deserialize, Insertable)]
#[diesel(table_name = crate::schema::task)]
pub struct NewTask {
    /// Application-generated UUID. Lot 3a generates task UUIDs app-side (instead of
    /// relying on the DB `DEFAULT gen_random_uuid()`) so the full `id_mapping` is
    /// known before any insert, enabling grouped multi-row inserts.
    pub id: uuid::Uuid,
    pub name: String,
    pub kind: String,
    pub status: StatusKind,
    pub timeout: i32,
    pub metadata: serde_json::Value,
    pub start_condition: Rules,
    pub wait_success: i32,
    pub wait_finished: i32,
    pub batch_id: Option<uuid::Uuid>,
    pub expected_count: Option<i32>,
    pub dead_end_barrier: bool,
    pub priority: i32,
}

/// Link represents a dependency between two tasks (parent -> child).
/// Used primarily for visualization/debugging. Actual execution uses
/// wait_success and wait_finished counters on the task.
#[derive(Queryable, Selectable, Insertable, Debug, Clone)]
#[diesel(table_name = crate::schema::link)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Link {
    pub parent_id: uuid::Uuid,
    pub child_id: uuid::Uuid,
    pub requires_success: bool,
}

/// A batch row. Created when the `POST /task` body provided any of
/// `on_batch_complete`, `scope`, or `metadata`. Batches with none of these never
/// get a row here (they are tracked only via `task.batch_id`).
#[derive(Identifiable, Queryable, Selectable, Debug, Clone)]
#[diesel(table_name = crate::schema::batch)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Batch {
    pub id: uuid::Uuid,
    /// The `on_batch_complete` actions, stored as a JSON array of `NewActionDto`
    /// (empty array `[]` when the batch carries only scope/metadata).
    pub on_complete: serde_json::Value,
    pub created_at: chrono::DateTime<Utc>,
    /// Optional business-level label for filtering/search.
    pub scope: Option<String>,
    /// Arbitrary structured metadata for filtering/search (defaults to `{}`).
    pub metadata: serde_json::Value,
    /// Number of the batch's tasks that are NOT yet terminal (Audit 2, D2). Every
    /// terminal transition decrements it in-tx; `remaining = 0` is the batch-complete
    /// signal. Initialized to the number of tasks actually inserted (dedupe-skips
    /// excluded).
    pub remaining: i32,
}

/// Insertable struct for a batch row (webhook payload and/or scope/metadata).
/// `remaining` is omitted on purpose: it defaults to 0 in the DB and is set to the
/// actual inserted-task count right after `insert_task_batch` (see `add_task`).
#[derive(Debug, Insertable)]
#[diesel(table_name = crate::schema::batch)]
pub struct NewBatch {
    pub id: uuid::Uuid,
    pub on_complete: serde_json::Value,
    pub scope: Option<String>,
    pub metadata: serde_json::Value,
}

#[derive(Associations, PartialEq, Debug, Serialize, Insertable)]
#[diesel(
    table_name = crate::schema::action,
    belongs_to(Task),
    check_for_backend(diesel::pg::Pg))
]
pub struct NewAction<'a> {
    pub task_id: uuid::Uuid,
    pub kind: &'a ActionKindEnum,
    pub params: serde_json::Value,
    pub trigger: &'a TriggerKind,
    pub condition: &'a TriggerCondition,
}

/// The type of action to execute. Currently only Webhook is supported.
#[derive(
    Debug, PartialEq, Serialize, diesel_derive_enum::DbEnum, Deserialize, Clone, Copy, ToSchema,
)]
#[db_enum(existing_type_path = "crate::schema::sql_types::ActionKind")]
pub enum ActionKindEnum {
    /// HTTP webhook call. Params must be a WebhookParams JSON object.
    Webhook,
}

/// When an action is triggered in the task lifecycle.
#[derive(
    Debug, PartialEq, Serialize, diesel_derive_enum::DbEnum, Deserialize, Clone, Copy, ToSchema,
)]
#[db_enum(existing_type_path = "crate::schema::sql_types::TriggerKind")]
pub enum TriggerKind {
    /// Fired when the worker starts the task (Pending -> Running transition).
    Start,
    /// Fired when the task reaches a terminal state (Success or Failure).
    End,
    /// Fired when the task is explicitly canceled via DELETE.
    Cancel,
    /// Fired once when the LAST task of a batch reaches a terminal state.
    /// Batch-level (not tied to a single task); see the `batch` table and the
    /// `on_batch_complete` field of `POST /task`.
    BatchComplete,
}

/// Condition that determines which End-trigger action fires.
#[derive(
    Debug, PartialEq, Serialize, diesel_derive_enum::DbEnum, Deserialize, Clone, Copy, ToSchema,
)]
#[db_enum(existing_type_path = "crate::schema::sql_types::TriggerCondition")]
pub enum TriggerCondition {
    /// Action fires when the task ends with Success status.
    Success,
    /// Action fires when the task ends with Failure status.
    Failure,
}

/// Status of a webhook execution record for idempotency tracking.
#[derive(
    Debug,
    PartialEq,
    Serialize,
    diesel_derive_enum::DbEnum,
    Deserialize,
    Clone,
    Copy,
    utoipa::ToSchema,
)]
#[db_enum(existing_type_path = "crate::schema::sql_types::WebhookExecutionStatus")]
pub enum WebhookExecutionStatus {
    /// Webhook execution claimed but not yet completed (also: outbox row awaiting delivery).
    Pending,
    /// Webhook executed successfully.
    Success,
    /// Webhook execution failed (transient — eligible for retry by the delivery loop).
    Failure,
    /// Webhook delivery permanently failed after exhausting the max retry attempts.
    Exhausted,
}

/// Tracks webhook executions for idempotency. Each trigger event for a task
/// gets at most one record, keyed by `idempotency_key`.
/// Note: for Start/Cancel triggers, `condition` is stored as `Success` sentinel.
#[derive(Identifiable, Queryable, QueryableByName, Selectable, Debug, Clone)]
#[diesel(table_name = crate::schema::webhook_execution)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct WebhookExecution {
    pub id: uuid::Uuid,
    /// Owning task, for task-level rows (end/cancel/start). NULL for batch-level
    /// (`batch_complete`) rows, which are keyed by `batch_id` instead.
    pub task_id: Option<uuid::Uuid>,
    pub trigger: TriggerKind,
    pub condition: TriggerCondition,
    pub idempotency_key: String,
    pub status: WebhookExecutionStatus,
    pub attempts: i32,
    pub created_at: chrono::DateTime<Utc>,
    pub updated_at: chrono::DateTime<Utc>,
    /// When this outbox row becomes eligible for (re)delivery. Defaults to now()
    /// on insert (mature immediately); bumped forward on each failed attempt.
    pub next_attempt_at: chrono::DateTime<Utc>,
    /// Last delivery error message (for diagnostics via GET /webhook-deliveries).
    pub last_error: Option<String>,
    /// Owning batch, for batch-level (`batch_complete`) rows. NULL for task-level rows.
    pub batch_id: Option<uuid::Uuid>,
}

/// Insertable struct for creating a new webhook execution record.
#[derive(Debug, Insertable)]
#[diesel(table_name = crate::schema::webhook_execution)]
pub struct NewWebhookExecution<'a> {
    pub task_id: uuid::Uuid,
    pub trigger: &'a TriggerKind,
    pub condition: &'a TriggerCondition,
    pub idempotency_key: &'a str,
}

/// A row of the dedicated webhook delivery QUEUE (`webhook_outbox`, Audit 2 D3).
///
/// The queue is pure: every row present is awaiting delivery, so there is no `status`
/// column (contrast with [`WebhookExecution`], which is the idempotency ledger + the
/// delivery history/log). A row is DELETED from this table — and historised into
/// `webhook_execution` as `success`/`exhausted` — the moment its delivery terminates.
/// Field order matches the `webhook_outbox` column order (positional `Queryable`).
#[derive(Identifiable, Queryable, QueryableByName, Selectable, Debug, Clone)]
#[diesel(table_name = crate::schema::webhook_outbox)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct WebhookOutbox {
    pub id: uuid::Uuid,
    /// Owning task, for task-level rows (end/cancel). NULL for batch-level
    /// (`batch_complete`) rows, which are keyed by `batch_id` instead.
    pub task_id: Option<uuid::Uuid>,
    /// Owning batch, for batch-level (`batch_complete`) rows. NULL for task-level rows.
    pub batch_id: Option<uuid::Uuid>,
    pub trigger: TriggerKind,
    pub condition: TriggerCondition,
    pub idempotency_key: String,
    pub attempts: i32,
    pub created_at: chrono::DateTime<Utc>,
    pub updated_at: chrono::DateTime<Utc>,
    /// When this queue row becomes eligible for (re)delivery. Defaults to now() on
    /// insert (mature immediately); pushed forward by the claim lease and on each
    /// failed attempt.
    pub next_attempt_at: chrono::DateTime<Utc>,
    /// Last delivery error message (for diagnostics; copied into the history row).
    pub last_error: Option<String>,
    /// Fresh ownership token assigned by each leased claim. Marks carrying an older token
    /// are ignored after the lease has been reclaimed by another worker.
    pub lease_token: Option<uuid::Uuid>,
}

/// Task lifecycle status. See the API description for the full state machine.
///
/// Valid transitions:
/// - Waiting -> Pending (automatic, when all dependencies are met)
/// - Pending -> Claimed (automatic, when worker claims the task)
/// - Claimed -> Running (automatic, after on_start webhook succeeds)
/// - Running -> Success (via PATCH API)
/// - Running -> Failure (via PATCH API, timeout, or parent failure propagation)
/// - Any active state -> Canceled (via DELETE API)
/// - Any active state -> Paused (via PATCH /task/pause/{id})
#[derive(
    Debug,
    PartialEq,
    Serialize,
    diesel_derive_enum::DbEnum,
    Deserialize,
    Clone,
    Copy,
    Hash,
    Eq,
    ToSchema,
)]
#[db_enum(existing_type_path = "crate::schema::sql_types::StatusKind")]
pub enum StatusKind {
    /// Has unmet dependencies. Transitions to Pending automatically when all parents complete.
    Waiting,
    /// Ready to run. The worker loop will pick it up and call its on_start webhook.
    Pending,
    /// Claimed by a worker but not yet started (on_start not completed).
    Claimed,
    /// Currently executing. The on_start webhook has been called. Waiting for external completion.
    Running,
    /// Failed. Set via API, by timeout, or by parent failure propagation.
    Failure,
    /// Completed successfully. Set via PATCH API.
    Success,
    /// Manually paused. Worker ignores paused tasks.
    Paused,
    /// Manually canceled. Treated like Failure for dependency propagation.
    Canceled,
}
