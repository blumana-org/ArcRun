use std::collections::HashSet;

use crate::dtos::{NewTaskDto, UpdateTaskDto};
use crate::models::StatusKind;
use crate::rule::{Rules, Strategy};

use super::action::validate_action_params;
use super::constants::*;
use super::{ValidationError, ValidationResult};

/// Validates the strategies (concurrency/capacity rules) within a Rules struct.
/// `prefix` is prepended to field names in errors (e.g. "rules" or "tasks[0].rules").
fn validate_rule_strategies(
    rules: &Rules,
    prefix: &str,
    errors: &mut Vec<ValidationError>,
) -> bool {
    let mut has_capacity_rule = false;
    for (i, rule) in rules.0.iter().enumerate() {
        match rule {
            Strategy::Concurency(concurrency_rule) => {
                if concurrency_rule.max_concurency <= 0 {
                    errors.push(ValidationError {
                        field: format!("{}[{}].max_concurency", prefix, i),
                        message: "max_concurency must be a positive integer".to_string(),
                    });
                }
                if concurrency_rule.matcher.kind.trim().is_empty() {
                    errors.push(ValidationError {
                        field: format!("{}[{}].matcher.kind", prefix, i),
                        message: "Matcher kind cannot be empty".to_string(),
                    });
                }
            }
            Strategy::Capacity(capacity_rule) => {
                has_capacity_rule = true;
                if capacity_rule.max_capacity <= 0 {
                    errors.push(ValidationError {
                        field: format!("{}[{}].max_capacity", prefix, i),
                        message: "max_capacity must be a positive integer".to_string(),
                    });
                }
                if capacity_rule.matcher.kind.trim().is_empty() {
                    errors.push(ValidationError {
                        field: format!("{}[{}].matcher.kind", prefix, i),
                        message: "Matcher kind cannot be empty".to_string(),
                    });
                }
                if capacity_rule.matcher.status != StatusKind::Running {
                    errors.push(ValidationError {
                        field: format!("{}[{}].matcher.status", prefix, i),
                        message: "Capacity rules only support matcher.status = Running".to_string(),
                    });
                }
            }
        }
    }
    has_capacity_rule
}

/// Validates a new task DTO before creation.
pub fn validate_new_task(dto: &NewTaskDto) -> ValidationResult {
    let mut errors = Vec::new();
    let limits = super::get_limits_config();

    // A10: bound the number of dependencies a single task may declare (an unbounded
    // fan-in multiplies out into the grouped `link` INSERT's bind-parameter count).
    if let Some(ref deps) = dto.dependencies
        && deps.len() > limits.max_deps_per_task
    {
        errors.push(ValidationError {
            field: "dependencies".to_string(),
            message: format!(
                "task cannot exceed {} dependencies (received {})",
                limits.max_deps_per_task,
                deps.len()
            ),
        });
    }

    // A10: bound the number of actions per task. `on_start` is always present (1);
    // `on_failure` and `on_success` are optional lists. Together they drive the
    // grouped `action` INSERT's bind-parameter count.
    let action_count = 1
        + dto.on_failure.as_ref().map(|a| a.len()).unwrap_or(0)
        + dto.on_success.as_ref().map(|a| a.len()).unwrap_or(0);
    if action_count > limits.max_actions_per_task {
        errors.push(ValidationError {
            field: "actions".to_string(),
            message: format!(
                "task cannot exceed {} actions (on_start + on_failure + on_success); received {}",
                limits.max_actions_per_task, action_count
            ),
        });
    }

    // Validate id (local reference)
    if dto.id.trim().is_empty() {
        errors.push(ValidationError {
            field: "id".to_string(),
            message: "Task local ID cannot be empty".to_string(),
        });
    }

    // Validate name
    if dto.name.trim().is_empty() {
        errors.push(ValidationError {
            field: "name".to_string(),
            message: "Task name cannot be empty".to_string(),
        });
    } else if dto.name.len() > MAX_TASK_NAME_LEN {
        errors.push(ValidationError {
            field: "name".to_string(),
            message: format!("Task name cannot exceed {} characters", MAX_TASK_NAME_LEN),
        });
    }

    // Validate kind
    if dto.kind.trim().is_empty() {
        errors.push(ValidationError {
            field: "kind".to_string(),
            message: "Task kind cannot be empty".to_string(),
        });
    } else if dto.kind.len() > MAX_TASK_KIND_LEN {
        errors.push(ValidationError {
            field: "kind".to_string(),
            message: format!("Task kind cannot exceed {} characters", MAX_TASK_KIND_LEN),
        });
    }

    // Validate timeout
    if let Some(timeout) = dto.timeout {
        if timeout <= 0 {
            errors.push(ValidationError {
                field: "timeout".to_string(),
                message: "Timeout must be a positive integer".to_string(),
            });
        } else if timeout > MAX_TIMEOUT_SECS {
            errors.push(ValidationError {
                field: "timeout".to_string(),
                message: format!(
                    "Timeout cannot exceed {} seconds (24 hours)",
                    MAX_TIMEOUT_SECS
                ),
            });
        }
    }

    // Validate expected_count
    if let Some(expected_count) = dto.expected_count
        && expected_count < 0
    {
        errors.push(ValidationError {
            field: "expected_count".to_string(),
            message: "expected_count cannot be negative".to_string(),
        });
    }

    // Validate priority range
    if let Some(p) = dto.priority
        && !(-1000..=1000).contains(&p)
    {
        errors.push(ValidationError {
            field: "priority".into(),
            message: "priority must be between -1000 and 1000".into(),
        });
    }

    // Validate metadata size (prevent extremely large payloads)
    if let Some(ref metadata) = dto.metadata {
        let metadata_str = serde_json::to_string(metadata).unwrap_or_default();
        if metadata_str.len() > MAX_METADATA_BYTES {
            errors.push(ValidationError {
                field: "metadata".to_string(),
                message: "Metadata cannot exceed 64KB".to_string(),
            });
        }
    }

    // Validate on_start action
    if let Err(e) = validate_action_params(&dto.on_start.kind, &dto.on_start.params) {
        errors.push(ValidationError {
            field: "on_start.params".to_string(),
            message: e,
        });
    }

    // Validate on_failure actions
    if let Some(ref actions) = dto.on_failure {
        for (i, action) in actions.iter().enumerate() {
            if let Err(e) = validate_action_params(&action.kind, &action.params) {
                errors.push(ValidationError {
                    field: format!("on_failure[{}].params", i),
                    message: e,
                });
            }
        }
    }

    // Validate on_success actions
    if let Some(ref actions) = dto.on_success {
        for (i, action) in actions.iter().enumerate() {
            if let Err(e) = validate_action_params(&action.kind, &action.params) {
                errors.push(ValidationError {
                    field: format!("on_success[{}].params", i),
                    message: e,
                });
            }
        }
    }

    // Validate dependencies
    if let Some(ref deps) = dto.dependencies {
        for (i, dep) in deps.iter().enumerate() {
            if dep.id.trim().is_empty() {
                errors.push(ValidationError {
                    field: format!("dependencies[{}].id", i),
                    message: "Dependency ID cannot be empty".to_string(),
                });
            }
        }

        // Check for self-referencing dependency
        if deps.iter().any(|d| d.id == dto.id) {
            errors.push(ValidationError {
                field: "dependencies".to_string(),
                message: "Task cannot depend on itself".to_string(),
            });
        }

        // Check for duplicate dependencies using HashSet
        let mut seen_ids: HashSet<&str> = HashSet::new();
        for dep in deps {
            if !seen_ids.insert(dep.id.as_str()) {
                errors.push(ValidationError {
                    field: "dependencies".to_string(),
                    message: format!("Duplicate dependency: {}", dep.id),
                });
            }
        }
    }

    // Validate rules (concurrency conditions)
    if let Some(ref rules) = dto.rules {
        let has_capacity_rule = validate_rule_strategies(rules, "rules", &mut errors);
        if has_capacity_rule && dto.expected_count.is_none() {
            errors.push(ValidationError {
                field: "expected_count".to_string(),
                message: "expected_count is required when a Capacity rule is used".to_string(),
            });
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

/// Validates an update task DTO.
pub fn validate_update_task(dto: &UpdateTaskDto) -> ValidationResult {
    let mut errors = Vec::new();

    // Validate status transition (only Success or Failure allowed via API)
    if let Some(ref status) = dto.status {
        match status {
            StatusKind::Success | StatusKind::Failure => {}
            _ => {
                errors.push(ValidationError {
                    field: "status".to_string(),
                    message: "Status can only be updated to Success or Failure".to_string(),
                });
            }
        }
    }

    // Validate counters are non-negative
    if let Some(success) = dto.new_success
        && success < 0
    {
        errors.push(ValidationError {
            field: "new_success".to_string(),
            message: "new_success cannot be negative".to_string(),
        });
    }

    if let Some(failures) = dto.new_failures
        && failures < 0
    {
        errors.push(ValidationError {
            field: "new_failures".to_string(),
            message: "new_failures cannot be negative".to_string(),
        });
    }

    // Validate expected_count
    if let Some(expected_count) = dto.expected_count
        && expected_count < 0
    {
        errors.push(ValidationError {
            field: "expected_count".to_string(),
            message: "expected_count cannot be negative".to_string(),
        });
    }

    // Validate priority range
    if let Some(p) = dto.priority
        && !(-1000..=1000).contains(&p)
    {
        errors.push(ValidationError {
            field: "priority".into(),
            message: "priority must be between -1000 and 1000".into(),
        });
    }

    // Validate failure_reason is provided when status is Failure
    if let Some(ref status) = dto.status
        && *status == StatusKind::Failure
        && dto.failure_reason.is_none()
    {
        errors.push(ValidationError {
            field: "failure_reason".to_string(),
            message: "failure_reason is required when status is Failure".to_string(),
        });
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

/// Validates an update task DTO for the PUT (batch counter) endpoint.
/// Only validates counter fields — does NOT check status or failure_reason.
pub fn validate_update_task_counters(dto: &UpdateTaskDto) -> ValidationResult {
    let mut errors = Vec::new();

    if let Some(success) = dto.new_success
        && success < 0
    {
        errors.push(ValidationError {
            field: "new_success".to_string(),
            message: "new_success cannot be negative".to_string(),
        });
    }

    if let Some(failures) = dto.new_failures
        && failures < 0
    {
        errors.push(ValidationError {
            field: "new_failures".to_string(),
            message: "new_failures cannot be negative".to_string(),
        });
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

/// Validates rules for a batch rules update.
pub fn validate_rules(rules: &Rules) -> ValidationResult {
    let mut errors = Vec::new();
    validate_rule_strategies(rules, "rules", &mut errors);

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

/// Validates a batch of new tasks, checking for duplicate IDs, unknown/forward
/// dependency references, and circular dependencies.
/// Validate the batch-level `scope` and `metadata` carried by a `POST /task` object body.
/// `scope` must be non-empty (after trim) and within the length cap; `metadata` is size-capped
/// like task metadata. Both are optional — `None` is always valid.
pub fn validate_batch_meta(
    scope: Option<&str>,
    metadata: Option<&serde_json::Value>,
) -> ValidationResult {
    let mut errors = Vec::new();

    if let Some(scope) = scope {
        if scope.trim().is_empty() {
            errors.push(ValidationError {
                field: "scope".to_string(),
                message: "Batch scope cannot be empty".to_string(),
            });
        } else if scope.len() > MAX_BATCH_SCOPE_LEN {
            errors.push(ValidationError {
                field: "scope".to_string(),
                message: format!(
                    "Batch scope cannot exceed {} characters",
                    MAX_BATCH_SCOPE_LEN
                ),
            });
        }
    }

    if let Some(metadata) = metadata {
        let metadata_str = serde_json::to_string(metadata).unwrap_or_default();
        if metadata_str.len() > MAX_METADATA_BYTES {
            errors.push(ValidationError {
                field: "metadata".to_string(),
                message: "Batch metadata cannot exceed 64KB".to_string(),
            });
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

pub fn validate_task_batch(tasks: &[NewTaskDto]) -> ValidationResult {
    use std::collections::HashMap;

    let mut errors = Vec::new();

    // A10: bound the batch size FIRST and bail out immediately. A single
    // unauthenticated request could otherwise submit an unbounded number of tasks,
    // both blowing past PostgreSQL's 65535 bind-parameter ceiling (→ 500) and forcing
    // O(n) validation work (dependency resolution) on an arbitrarily large payload.
    let limits = super::get_limits_config();
    if tasks.len() > limits.max_tasks_per_batch {
        return Err(vec![ValidationError {
            field: "tasks".to_string(),
            message: format!(
                "batch cannot exceed {} tasks (received {})",
                limits.max_tasks_per_batch,
                tasks.len()
            ),
        }]);
    }

    // First validate each task individually
    for (i, task) in tasks.iter().enumerate() {
        if let Err(task_errors) = validate_new_task(task) {
            for e in task_errors {
                errors.push(ValidationError {
                    field: format!("tasks[{}].{}", i, e.field),
                    message: e.message,
                });
            }
        }
    }

    // Build task_id_to_idx incrementally, checking for duplicate IDs
    let mut task_id_to_idx: HashMap<&str, usize> = HashMap::new();
    for (i, task) in tasks.iter().enumerate() {
        if let Some(prev_idx) = task_id_to_idx.insert(task.id.as_str(), i) {
            errors.push(ValidationError {
                field: format!("tasks[{}].id", i),
                message: format!(
                    "duplicate task id: '{}' (first defined at index {})",
                    task.id, prev_idx
                ),
            });
        }
    }

    // Check for unknown and forward dependency references
    let mut seen: HashSet<&str> = HashSet::new();
    for (i, task) in tasks.iter().enumerate() {
        if let Some(ref deps) = task.dependencies {
            for dep in deps {
                if !task_id_to_idx.contains_key(dep.id.as_str()) {
                    errors.push(ValidationError {
                        field: format!("tasks[{}].dependencies", i),
                        message: format!("unknown dependency: '{}'", dep.id),
                    });
                } else if !seen.contains(dep.id.as_str()) {
                    errors.push(ValidationError {
                        field: format!("tasks[{}].dependencies", i),
                        message: format!(
                            "dependency '{}' must appear before task '{}' in the batch",
                            dep.id, task.id
                        ),
                    });
                }
            }
        }
        seen.insert(task.id.as_str());
    }

    // Cycle detection is UNNECESSARY here (Audit 2, A10): the forward-reference rule
    // enforced just above ("a dependency must appear before the task in the batch",
    // via the `seen` set) makes cycles impossible by construction. Every edge points
    // from a later task to a strictly earlier one, so the dependency graph is a DAG in
    // topological (input) order and can contain no cycle. The old recursive DFS was
    // therefore redundant — and, being recursive, a stack-overflow DoS vector on a
    // long chain of tasks. It has been removed; the forward-reference check is the
    // single guarantee that rejects both unknown-id and would-be-cyclic references.

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dtos::{Dependency, NewActionDto};
    use crate::models::ActionKindEnum;

    fn make_valid_action() -> NewActionDto {
        NewActionDto {
            kind: ActionKindEnum::Webhook,
            params: serde_json::json!({
                "url": "https://example.com/webhook",
                "verb": "Post"
            }),
        }
    }

    fn make_valid_task(id: &str) -> NewTaskDto {
        NewTaskDto {
            id: id.to_string(),
            name: "Test Task".to_string(),
            kind: "test".to_string(),
            timeout: Some(60),
            metadata: None,
            dedupe_strategy: None,
            rules: None,
            on_start: make_valid_action(),
            dependencies: None,
            expected_count: None,
            on_failure: None,
            on_success: None,
            dead_end_barrier: None,
            priority: None,
        }
    }

    #[test]
    fn test_valid_task() {
        let task = make_valid_task("task1");
        assert!(validate_new_task(&task).is_ok());
    }

    #[test]
    fn test_empty_name() {
        let mut task = make_valid_task("task1");
        task.name = "".to_string();
        let result = validate_new_task(&task);
        assert!(result.is_err());
        assert!(result.unwrap_err().iter().any(|e| e.field == "name"));
    }

    #[test]
    fn test_empty_kind() {
        let mut task = make_valid_task("task1");
        task.kind = "".to_string();
        let result = validate_new_task(&task);
        assert!(result.is_err());
        assert!(result.unwrap_err().iter().any(|e| e.field == "kind"));
    }

    #[test]
    fn test_negative_timeout() {
        let mut task = make_valid_task("task1");
        task.timeout = Some(-1);
        let result = validate_new_task(&task);
        assert!(result.is_err());
        assert!(result.unwrap_err().iter().any(|e| e.field == "timeout"));
    }

    #[test]
    fn test_self_dependency() {
        let mut task = make_valid_task("task1");
        task.dependencies = Some(vec![Dependency {
            id: "task1".to_string(),
            requires_success: true,
        }]);
        let result = validate_new_task(&task);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .iter()
                .any(|e| e.message.contains("itself"))
        );
    }

    #[test]
    fn test_circular_dependency() {
        let mut task1 = make_valid_task("task1");
        task1.dependencies = Some(vec![Dependency {
            id: "task2".to_string(),
            requires_success: true,
        }]);

        let mut task2 = make_valid_task("task2");
        task2.dependencies = Some(vec![Dependency {
            id: "task1".to_string(),
            requires_success: true,
        }]);

        let result = validate_task_batch(&[task1, task2]);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .iter()
                .any(|e| e.message.contains("must appear before"))
        );
    }

    #[test]
    fn test_longer_circular_dependency() {
        let mut task_a = make_valid_task("A");
        task_a.dependencies = Some(vec![Dependency {
            id: "B".to_string(),
            requires_success: true,
        }]);

        let mut task_b = make_valid_task("B");
        task_b.dependencies = Some(vec![Dependency {
            id: "C".to_string(),
            requires_success: true,
        }]);

        let mut task_c = make_valid_task("C");
        task_c.dependencies = Some(vec![Dependency {
            id: "A".to_string(),
            requires_success: true,
        }]);

        let result = validate_task_batch(&[task_a, task_b, task_c]);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(
            errors
                .iter()
                .any(|e| e.message.contains("must appear before")),
            "Expected forward reference error, got: {:?}",
            errors
        );
    }

    #[test]
    fn test_invalid_webhook_url() {
        let mut task = make_valid_task("task1");
        task.on_start.params = serde_json::json!({
            "url": "not-a-url",
            "verb": "Post"
        });
        let result = validate_new_task(&task);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .iter()
                .any(|e| e.field.contains("on_start"))
        );
    }

    #[test]
    fn test_priority_valid_range() {
        let mut task = make_valid_task("task1");
        task.priority = Some(500);
        assert!(validate_new_task(&task).is_ok());

        task.priority = Some(-1000);
        assert!(validate_new_task(&task).is_ok());

        task.priority = Some(1000);
        assert!(validate_new_task(&task).is_ok());
    }

    #[test]
    fn test_priority_out_of_range() {
        let mut task = make_valid_task("task1");
        task.priority = Some(1001);
        let result = validate_new_task(&task);
        assert!(result.is_err());
        assert!(result.unwrap_err().iter().any(|e| e.field == "priority"));

        task.priority = Some(-1001);
        let result = validate_new_task(&task);
        assert!(result.is_err());
        assert!(result.unwrap_err().iter().any(|e| e.field == "priority"));
    }

    #[test]
    fn test_priority_default_none_is_valid() {
        let task = make_valid_task("task1");
        assert!(task.priority.is_none());
        assert!(validate_new_task(&task).is_ok());
    }
}
