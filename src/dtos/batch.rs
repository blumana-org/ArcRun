use chrono::Utc;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::models::StatusKind;
use crate::rule::Rules;

/// Summary of a batch with aggregated task statistics.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct BatchSummaryDto {
    /// The batch UUID grouping tasks created in the same POST /task call.
    pub batch_id: uuid::Uuid,
    /// Total number of tasks in this batch.
    pub total_tasks: i64,
    /// When the first task in this batch was created.
    pub first_created_at: chrono::DateTime<Utc>,
    /// When the most recent task in this batch was last updated.
    pub latest_updated_at: chrono::DateTime<Utc>,
    /// Breakdown of task counts by status.
    pub status_counts: BatchStatusCounts,
    /// Distinct task kinds in this batch.
    pub kinds: Vec<String>,
    /// Optional business-level label for the batch (null when none was set).
    pub scope: Option<String>,
    /// Structured metadata attached to the batch (`{}` when none was set).
    pub metadata: serde_json::Value,
    /// Number of tasks not yet terminal, from the denormalized `batch.remaining`
    /// counter. `0` means the batch is complete. `null` for batches
    /// that have no `batch` row (no on_batch_complete / scope / metadata was set —
    /// such batches are tracked only via `task.batch_id`, so no counter exists).
    pub remaining: Option<i32>,
}

/// Per-status task counts within a batch.
#[derive(Debug, Serialize, Deserialize, ToSchema, Default)]
pub struct BatchStatusCounts {
    pub waiting: i64,
    pub pending: i64,
    pub claimed: i64,
    pub running: i64,
    pub success: i64,
    pub failure: i64,
    pub paused: i64,
    pub canceled: i64,
}

/// Aggregated counters for a single batch.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct BatchStatsDto {
    /// The batch UUID.
    pub batch_id: uuid::Uuid,
    /// Total number of tasks in this batch.
    pub total_tasks: i64,
    /// Sum of all tasks' success counters.
    pub total_success: i64,
    /// Sum of all tasks' failure counters.
    pub total_failures: i64,
    /// Sum of all tasks' expected_count values. Null when at least one task has no expected_count set,
    /// meaning total progress cannot be meaningfully computed.
    pub total_expected: Option<i64>,
    /// Breakdown of task counts by status.
    pub status_counts: BatchStatusCounts,
    /// Optional business-level label for the batch (null when none was set or no batch row exists).
    pub scope: Option<String>,
    /// Structured metadata attached to the batch (`{}` when none was set or no batch row exists).
    pub metadata: serde_json::Value,
}

/// Filter parameters for batch listing. All filters are optional and combined with AND logic.
/// Filters apply to tasks within each batch — a batch is included if it contains at least one
/// matching task.
#[derive(Debug, Serialize, Deserialize, Default, IntoParams)]
pub struct BatchFilterDto {
    /// Filter batches containing tasks with this kind (substring match).
    pub kind: Option<String>,
    /// Filter batches containing tasks with this status.
    pub status: Option<StatusKind>,
    /// Filter batches containing tasks with this name (substring match).
    pub name: Option<String>,
    /// Only include batches with tasks created on or after this timestamp.
    pub created_after: Option<chrono::DateTime<Utc>>,
    /// Only include batches with tasks created on or before this timestamp.
    pub created_before: Option<chrono::DateTime<Utc>>,
    /// Filter by batch scope (exact match).
    pub scope: Option<String>,
    /// Filter by batch metadata using JSONB containment (`@>`). Pass a JSON object,
    /// e.g. `?metadata={"env":"prod"}`; matches batches whose metadata contains it.
    pub metadata: Option<String>,
    /// Free-text substring search across the batch scope and its metadata (as text).
    pub search: Option<String>,
}

/// Payload for updating concurrency/capacity rules on Waiting/Pending/Paused tasks in a batch,
/// filtered by task kind.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct UpdateBatchRulesDto {
    /// The task kind to target. Only non-terminal tasks with this kind will be updated.
    pub kind: String,
    /// The new concurrency/capacity rules to apply.
    /// Pass an empty array to remove all rules.
    pub rules: Rules,
}

/// Response from updating batch rules. Reports how many tasks were affected.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct UpdateBatchRulesResponseDto {
    /// The batch UUID that was updated.
    pub batch_id: uuid::Uuid,
    /// The task kind that was targeted.
    pub kind: String,
    /// Number of not-yet-active tasks whose rules were updated.
    pub updated_count: i64,
}

/// Response from stopping an entire batch. Reports how many tasks were affected per status.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct StopBatchResponseDto {
    /// The batch UUID that was stopped.
    pub batch_id: uuid::Uuid,
    /// Number of Waiting tasks that were canceled.
    pub canceled_waiting: i64,
    /// Number of Pending tasks that were canceled.
    pub canceled_pending: i64,
    /// Number of Claimed tasks that were canceled (on_start may be in flight).
    pub canceled_claimed: i64,
    /// Number of Running tasks that were canceled (cancel webhooks fired if registered).
    pub canceled_running: i64,
    /// Number of Paused tasks that were canceled.
    pub canceled_paused: i64,
    /// Number of tasks already in a terminal state (Success, Failure, Canceled) — not modified.
    pub already_terminal: i64,
}
