use crate::common::*;

use arcrun::db_operation::{ClaimResult, claim_task_with_rules};
use arcrun::models::{StatusKind, Task};
use arcrun::rule::{CapacityRule, ConcurencyRule, Matcher, Rules, Strategy};
use serde_json::json;

/// Helper: build a minimal Task struct from a TaskDto and known metadata/rules.
/// Only the fields used by `claim_task_with_rules` matter (id, kind, metadata, start_condition).
fn make_task_for_claim(
    id: uuid::Uuid,
    kind: &str,
    metadata: serde_json::Value,
    rules: Rules,
) -> Task {
    Task {
        id,
        name: String::new(),
        kind: kind.to_string(),
        status: StatusKind::Pending,
        timeout: 60,
        created_at: chrono::Utc::now(),
        started_at: None,
        last_updated: chrono::Utc::now(),
        metadata,
        ended_at: None,
        start_condition: rules,
        wait_success: 0,
        wait_finished: 0,
        success: 0,
        failures: 0,
        failure_reason: None,
        batch_id: None,
        expected_count: None,
        dead_end_barrier: false,
        priority: 0,
        claimed_slot_keys: None,
        capacity_charge: None,
    }
}

/// Helper: build the Rules for a concurrency rule with max_concurency and optional metadata fields.
fn concurrency_rules(kind: &str, max: i32, fields: Vec<&str>) -> Rules {
    Rules(vec![Strategy::Concurency(ConcurencyRule {
        max_concurency: max,
        matcher: Matcher {
            kind: kind.to_string(),
            status: StatusKind::Running,
            fields: fields.into_iter().map(String::from).collect(),
        },
    })])
}

// =============================================================================
// Storage tests
// =============================================================================

#[tokio::test]
async fn test_concurrency_rules_stored_on_task() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let task = json!({
        "id": "concurrent",
        "name": "Concurrent Task",
        "kind": "concurrent-kind",
        "timeout": 60,
        "metadata": {"test": true},
        "on_start": webhook_action(),
        "rules": [{
            "type": "Concurency",
            "matcher": {
                "kind": "concurrent-kind",
                "status": "Running",
                "fields": []
            },
            "max_concurency": 2
        }]
    });

    let body = create_tasks_ok(&app, &[task]).await;
    assert_eq!(body[0].status, StatusKind::Pending);
    let full = get_task_ok(&app, body[0].id).await;
    assert!(!full.rules.0.is_empty());
}

/// Merged test: rules are stored per-project AND different projects get independent rules.
#[tokio::test]
async fn test_concurrency_rules_stored_per_project() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let tasks = vec![
        json!({
            "id": "proj-task-1",
            "name": "Project 1 Task",
            "kind": "project-concurrent",
            "timeout": 60,
            "metadata": {"projectId": 111},
            "on_start": webhook_action(),
            "rules": [{
                "type": "Concurency",
                "matcher": {
                    "kind": "project-concurrent",
                    "status": "Running",
                    "fields": ["projectId"]
                },
                "max_concurency": 1
            }]
        }),
        json!({
            "id": "proj-task-2",
            "name": "Project 1 Task 2 (same project)",
            "kind": "project-concurrent",
            "timeout": 60,
            "metadata": {"projectId": 111},
            "on_start": webhook_action(),
            "rules": [{
                "type": "Concurency",
                "matcher": {
                    "kind": "project-concurrent",
                    "status": "Running",
                    "fields": ["projectId"]
                },
                "max_concurency": 1
            }]
        }),
        json!({
            "id": "proj-task-3",
            "name": "Project 2 Task (different project)",
            "kind": "project-concurrent",
            "timeout": 60,
            "metadata": {"projectId": 222},
            "on_start": webhook_action(),
            "rules": [{
                "type": "Concurency",
                "matcher": {
                    "kind": "project-concurrent",
                    "status": "Running",
                    "fields": ["projectId"]
                },
                "max_concurency": 1
            }]
        }),
    ];

    let created = create_tasks_ok(&app, &tasks).await;
    assert_eq!(created.len(), 3);
    for t in &created {
        assert_eq!(t.status, StatusKind::Pending);
        let full = get_task_ok(&app, t.id).await;
        assert!(
            !full.rules.0.is_empty(),
            "task {} should have rules",
            t.name
        );
    }
}

// =============================================================================
// Enforcement tests
// =============================================================================

/// Concurrency enforcement: with max_concurency=1 and same kind,
/// the second task should be blocked when the first is Running.
#[tokio::test]
async fn test_concurrency_blocks_when_limit_reached() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let rules_json = json!([{
        "type": "Concurency",
        "matcher": {
            "kind": "enforce-kind",
            "status": "Running",
            "fields": []
        },
        "max_concurency": 1
    }]);

    let tasks = vec![
        json!({
            "id": "enforce-1",
            "name": "Enforce Task 1",
            "kind": "enforce-kind",
            "timeout": 60,
            "metadata": {"test": true},
            "on_start": webhook_action(),
            "rules": rules_json.clone()
        }),
        json!({
            "id": "enforce-2",
            "name": "Enforce Task 2",
            "kind": "enforce-kind",
            "timeout": 60,
            "metadata": {"test": true},
            "on_start": webhook_action(),
            "rules": rules_json.clone()
        }),
    ];

    let created = create_tasks_ok(&app, &tasks).await;
    let id1 = created[0].id;
    let id2 = created[1].id;

    let rules = concurrency_rules("enforce-kind", 1, vec![]);
    let metadata = json!({"test": true});

    // Claim first task — should succeed
    let mut conn = state.pool.get().await.unwrap();
    let t1 = make_task_for_claim(id1, "enforce-kind", metadata.clone(), rules.clone());
    let result1 = claim_task_with_rules(&mut conn, &t1).await.unwrap();
    assert_eq!(
        result1,
        ClaimResult::Claimed,
        "First task should be claimed"
    );

    // Try to claim second task — should be blocked (max_concurency=1, one already Claimed)
    let t2 = make_task_for_claim(id2, "enforce-kind", metadata.clone(), rules.clone());
    let result2 = claim_task_with_rules(&mut conn, &t2).await.unwrap();
    assert_eq!(
        result2,
        ClaimResult::RuleBlocked,
        "Second task should be blocked by concurrency rule"
    );

    // Verify statuses
    assert_task_status(&app, id1, StatusKind::Claimed, "first task Claimed").await;
    assert_task_status(&app, id2, StatusKind::Pending, "second task still Pending").await;

    // Mark first task as Running then complete it
    arcrun::db_operation::mark_task_running(&mut conn, &id1)
        .await
        .unwrap();
    let update_result =
        arcrun::db_operation::update_running_task(&mut conn, id1, success_dto(), true)
            .await
            .unwrap();
    assert_eq!(
        update_result,
        arcrun::db_operation::UpdateTaskResult::Updated,
        "First task should be updated to Success"
    );

    // Now claim second task — should succeed
    let result3 = claim_task_with_rules(&mut conn, &t2).await.unwrap();
    assert_eq!(
        result3,
        ClaimResult::Claimed,
        "Second task should be claimable after first completes"
    );

    assert_task_status(&app, id2, StatusKind::Claimed, "second task now Claimed").await;
}

/// Concurrency enforcement with metadata fields: tasks with different metadata
/// should NOT block each other even with max_concurency=1.
#[tokio::test]
async fn test_concurrency_allows_different_metadata() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let make_task = |id: &str, project_id: i32| {
        json!({
            "id": id,
            "name": format!("Project {} Task", project_id),
            "kind": "meta-enforce",
            "timeout": 60,
            "metadata": {"projectId": project_id},
            "on_start": webhook_action(),
            "rules": [{
                "type": "Concurency",
                "matcher": {
                    "kind": "meta-enforce",
                    "status": "Running",
                    "fields": ["projectId"]
                },
                "max_concurency": 1
            }]
        })
    };

    let tasks = vec![make_task("meta-1", 111), make_task("meta-2", 222)];
    let created = create_tasks_ok(&app, &tasks).await;
    let id1 = created[0].id;
    let id2 = created[1].id;

    let rules = concurrency_rules("meta-enforce", 1, vec!["projectId"]);

    // Claim first task (project 111)
    let mut conn = state.pool.get().await.unwrap();
    let t1 = make_task_for_claim(
        id1,
        "meta-enforce",
        json!({"projectId": 111}),
        rules.clone(),
    );
    let result1 = claim_task_with_rules(&mut conn, &t1).await.unwrap();
    assert_eq!(result1, ClaimResult::Claimed);

    // Claim second task (project 222) — different metadata, should NOT be blocked
    let t2 = make_task_for_claim(
        id2,
        "meta-enforce",
        json!({"projectId": 222}),
        rules.clone(),
    );
    let result2 = claim_task_with_rules(&mut conn, &t2).await.unwrap();
    assert_eq!(
        result2,
        ClaimResult::Claimed,
        "Tasks with different projectId should not block each other"
    );

    assert_task_status(&app, id1, StatusKind::Claimed, "task 1 Claimed").await;
    assert_task_status(&app, id2, StatusKind::Claimed, "task 2 Claimed").await;
}

/// Concurrency enforcement with metadata fields: tasks with the SAME metadata
/// should block each other with max_concurency=1.
#[tokio::test]
async fn test_concurrency_blocks_same_metadata() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let make_task = |id: &str| {
        json!({
            "id": id,
            "name": format!("Same Project Task {}", id),
            "kind": "same-meta",
            "timeout": 60,
            "metadata": {"projectId": 999},
            "on_start": webhook_action(),
            "rules": [{
                "type": "Concurency",
                "matcher": {
                    "kind": "same-meta",
                    "status": "Running",
                    "fields": ["projectId"]
                },
                "max_concurency": 1
            }]
        })
    };

    let tasks = vec![make_task("same-1"), make_task("same-2")];
    let created = create_tasks_ok(&app, &tasks).await;
    let id1 = created[0].id;
    let id2 = created[1].id;

    let rules = concurrency_rules("same-meta", 1, vec!["projectId"]);
    let metadata = json!({"projectId": 999});

    let mut conn = state.pool.get().await.unwrap();
    let t1 = make_task_for_claim(id1, "same-meta", metadata.clone(), rules.clone());
    let result1 = claim_task_with_rules(&mut conn, &t1).await.unwrap();
    assert_eq!(result1, ClaimResult::Claimed);

    let t2 = make_task_for_claim(id2, "same-meta", metadata.clone(), rules.clone());
    let result2 = claim_task_with_rules(&mut conn, &t2).await.unwrap();
    assert_eq!(
        result2,
        ClaimResult::RuleBlocked,
        "Tasks with same projectId should block each other"
    );
}

// =============================================================================
// Capacity rule helpers
// =============================================================================

/// Helper: build the Rules for a capacity rule with max_capacity and optional metadata fields.
fn capacity_rules(kind: &str, max_capacity: i32, fields: Vec<&str>) -> Rules {
    Rules(vec![Strategy::Capacity(CapacityRule {
        max_capacity,
        matcher: Matcher {
            kind: kind.to_string(),
            status: StatusKind::Running,
            fields: fields.into_iter().map(String::from).collect(),
        },
    })])
}

/// Helper: build a Task struct for claim with expected_count set.
fn make_task_for_claim_with_expected(
    id: uuid::Uuid,
    kind: &str,
    metadata: serde_json::Value,
    rules: Rules,
    expected_count: Option<i32>,
) -> Task {
    Task {
        id,
        name: String::new(),
        kind: kind.to_string(),
        status: StatusKind::Pending,
        timeout: 60,
        created_at: chrono::Utc::now(),
        started_at: None,
        last_updated: chrono::Utc::now(),
        metadata,
        ended_at: None,
        start_condition: rules,
        wait_success: 0,
        wait_finished: 0,
        success: 0,
        failures: 0,
        failure_reason: None,
        batch_id: None,
        expected_count,
        dead_end_barrier: false,
        priority: 0,
        claimed_slot_keys: None,
        capacity_charge: None,
    }
}

// =============================================================================
// Capacity storage tests
// =============================================================================

/// Roundtrip: create a task with a Capacity rule, GET it, verify the rule is stored.
#[tokio::test]
async fn test_capacity_rule_stored_on_task() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let task = json!({
        "id": "cap-store",
        "name": "Capacity Storage Test",
        "kind": "cap-kind",
        "timeout": 60,
        "expected_count": 1000,
        "metadata": {"test": true},
        "on_start": webhook_action(),
        "rules": [{
            "type": "Capacity",
            "max_capacity": 500,
            "matcher": {
                "kind": "cap-kind",
                "status": "Running",
                "fields": []
            }
        }]
    });

    let body = create_tasks_ok(&app, &[task]).await;
    assert_eq!(body[0].status, StatusKind::Pending);
    let full = get_task_ok(&app, body[0].id).await;
    assert_eq!(full.rules.0.len(), 1);
    match &full.rules.0[0] {
        Strategy::Capacity(rule) => {
            assert_eq!(rule.max_capacity, 500);
            assert_eq!(rule.matcher.kind, "cap-kind");
        }
        other => panic!("Expected Capacity rule, got {:?}", other),
    }
}

// =============================================================================
// Capacity enforcement tests
// =============================================================================

/// sum=200 < max_capacity=500 → second task is allowed.
#[tokio::test]
async fn test_capacity_allows_when_under_limit() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let cap_rules_json = json!([{
        "type": "Capacity",
        "max_capacity": 500,
        "matcher": { "kind": "cap-enforce", "status": "Running", "fields": [] }
    }]);

    // Task 1: expected_count=200 (will be Running with remaining=200)
    // Task 2: candidate expected_count=300
    let tasks = vec![
        json!({
            "id": "cap-t1",
            "name": "Cap Task 1",
            "kind": "cap-enforce",
            "timeout": 60,
            "expected_count": 200,
            "metadata": {},
            "on_start": webhook_action(),
            "rules": cap_rules_json.clone()
        }),
        json!({
            "id": "cap-t2",
            "name": "Cap Task 2",
            "kind": "cap-enforce",
            "timeout": 60,
            "expected_count": 300,
            "metadata": {},
            "on_start": webhook_action(),
            "rules": cap_rules_json.clone()
        }),
    ];

    let created = create_tasks_ok(&app, &tasks).await;
    let id1 = created[0].id;
    let id2 = created[1].id;

    let rules = capacity_rules("cap-enforce", 500, vec![]);

    // Claim + mark running task 1 (remaining = 200 - 0 - 0 = 200)
    let mut conn = state.pool.get().await.unwrap();
    let t1 =
        make_task_for_claim_with_expected(id1, "cap-enforce", json!({}), rules.clone(), Some(200));
    let result1 = claim_task_with_rules(&mut conn, &t1).await.unwrap();
    assert_eq!(result1, ClaimResult::Claimed);
    arcrun::db_operation::mark_task_running(&mut conn, &id1)
        .await
        .unwrap();

    // Now try claiming task 2: sum_running=200 < 500 → should be allowed
    let t2 =
        make_task_for_claim_with_expected(id2, "cap-enforce", json!({}), rules.clone(), Some(300));
    let result2 = claim_task_with_rules(&mut conn, &t2).await.unwrap();
    assert_eq!(
        result2,
        ClaimResult::Claimed,
        "sum=200 < 500, candidate should be allowed"
    );
}

/// sum=500 >= max_capacity=500 → candidate is blocked.
#[tokio::test]
async fn test_capacity_blocks_when_at_limit() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let cap_rules_json = json!([{
        "type": "Capacity",
        "max_capacity": 500,
        "matcher": { "kind": "cap-block", "status": "Running", "fields": [] }
    }]);

    let tasks = vec![
        json!({
            "id": "cap-b1",
            "name": "Cap Block 1",
            "kind": "cap-block",
            "timeout": 60,
            "expected_count": 500,
            "metadata": {},
            "on_start": webhook_action(),
            "rules": cap_rules_json.clone()
        }),
        json!({
            "id": "cap-b2",
            "name": "Cap Block 2",
            "kind": "cap-block",
            "timeout": 60,
            "expected_count": 100,
            "metadata": {},
            "on_start": webhook_action(),
            "rules": cap_rules_json.clone()
        }),
    ];

    let created = create_tasks_ok(&app, &tasks).await;
    let id1 = created[0].id;
    let id2 = created[1].id;

    let rules = capacity_rules("cap-block", 500, vec![]);

    // Claim + mark running task 1 (remaining = 500)
    let mut conn = state.pool.get().await.unwrap();
    let t1 =
        make_task_for_claim_with_expected(id1, "cap-block", json!({}), rules.clone(), Some(500));
    let result1 = claim_task_with_rules(&mut conn, &t1).await.unwrap();
    assert_eq!(result1, ClaimResult::Claimed);
    arcrun::db_operation::mark_task_running(&mut conn, &id1)
        .await
        .unwrap();

    // Try claiming task 2: sum_running=500 >= 500 → blocked
    let t2 =
        make_task_for_claim_with_expected(id2, "cap-block", json!({}), rules.clone(), Some(100));
    let result2 = claim_task_with_rules(&mut conn, &t2).await.unwrap();
    assert_eq!(
        result2,
        ClaimResult::RuleBlocked,
        "sum=500 >= 500, candidate should be blocked"
    );

    assert_task_status(&app, id2, StatusKind::Pending, "blocked task stays Pending").await;
}

/// Candidate with expected_count=2000 starts when sum=200 < 500 (candidate size irrelevant).
#[tokio::test]
async fn test_capacity_allows_large_candidate_when_room() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let cap_rules_json = json!([{
        "type": "Capacity",
        "max_capacity": 500,
        "matcher": { "kind": "cap-large", "status": "Running", "fields": [] }
    }]);

    let tasks = vec![
        json!({
            "id": "cap-l1",
            "name": "Small Running",
            "kind": "cap-large",
            "timeout": 60,
            "expected_count": 200,
            "metadata": {},
            "on_start": webhook_action(),
            "rules": cap_rules_json.clone()
        }),
        json!({
            "id": "cap-l2",
            "name": "Large Candidate",
            "kind": "cap-large",
            "timeout": 60,
            "expected_count": 2000,
            "metadata": {},
            "on_start": webhook_action(),
            "rules": cap_rules_json.clone()
        }),
    ];

    let created = create_tasks_ok(&app, &tasks).await;
    let id1 = created[0].id;
    let id2 = created[1].id;

    let rules = capacity_rules("cap-large", 500, vec![]);

    // Claim + mark running task 1 (remaining = 200)
    let mut conn = state.pool.get().await.unwrap();
    let t1 =
        make_task_for_claim_with_expected(id1, "cap-large", json!({}), rules.clone(), Some(200));
    let result1 = claim_task_with_rules(&mut conn, &t1).await.unwrap();
    assert_eq!(result1, ClaimResult::Claimed);
    arcrun::db_operation::mark_task_running(&mut conn, &id1)
        .await
        .unwrap();

    // Candidate with expected_count=2000: sum=200 < 500 → allowed (candidate size doesn't matter)
    let t2 =
        make_task_for_claim_with_expected(id2, "cap-large", json!({}), rules.clone(), Some(2000));
    let result2 = claim_task_with_rules(&mut conn, &t2).await.unwrap();
    assert_eq!(
        result2,
        ClaimResult::Claimed,
        "Candidate size is irrelevant, sum=200 < 500"
    );
}

/// Progress updates (success counter) reduce remaining, unblocking candidates.
#[tokio::test]
async fn test_capacity_accounts_for_progress() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let cap_rules_json = json!([{
        "type": "Capacity",
        "max_capacity": 500,
        "matcher": { "kind": "cap-progress", "status": "Running", "fields": [] }
    }]);

    let tasks = vec![
        json!({
            "id": "cap-p1",
            "name": "Running Progress",
            "kind": "cap-progress",
            "timeout": 60,
            "expected_count": 600,
            "metadata": {},
            "on_start": webhook_action(),
            "rules": cap_rules_json.clone()
        }),
        json!({
            "id": "cap-p2",
            "name": "Waiting Candidate",
            "kind": "cap-progress",
            "timeout": 60,
            "expected_count": 100,
            "metadata": {},
            "on_start": webhook_action(),
            "rules": cap_rules_json.clone()
        }),
    ];

    let created = create_tasks_ok(&app, &tasks).await;
    let id1 = created[0].id;
    let id2 = created[1].id;

    let rules = capacity_rules("cap-progress", 500, vec![]);

    // Claim + mark running task 1 (remaining = 600 - 0 - 0 = 600)
    let mut conn = state.pool.get().await.unwrap();
    let t1 =
        make_task_for_claim_with_expected(id1, "cap-progress", json!({}), rules.clone(), Some(600));
    let result1 = claim_task_with_rules(&mut conn, &t1).await.unwrap();
    assert_eq!(result1, ClaimResult::Claimed);
    arcrun::db_operation::mark_task_running(&mut conn, &id1)
        .await
        .unwrap();

    // sum=600 >= 500 → blocked
    let t2 =
        make_task_for_claim_with_expected(id2, "cap-progress", json!({}), rules.clone(), Some(100));
    let result2 = claim_task_with_rules(&mut conn, &t2).await.unwrap();
    assert_eq!(
        result2,
        ClaimResult::RuleBlocked,
        "sum=600 >= 500, should be blocked"
    );

    // Simulate progress THROUGH THE REAL FLUSH PATH: task 1 completed 200 items. Under
    // D1/7.3b the capacity slot is a claim-time charge counter — raw-SQL counter writes no
    // longer influence it; only the batch_updater flush pushes progress onto the slot,
    // shrinking task 1's capacity_charge 600 -> 400 and rule_slot.used 600 -> 400.
    arcrun::workers::run_counter_flush_once(&mut conn, &[(id1, 200, 0)])
        .await
        .unwrap();

    // Now the slot reflects remaining 400 < 500 → candidate should be allowed
    let result3 = claim_task_with_rules(&mut conn, &t2).await.unwrap();
    assert_eq!(
        result3,
        ClaimResult::Claimed,
        "After flushed progress, sum=400 < 500, should be allowed"
    );
}

/// Candidate without expected_count is blocked: validation rejects creation,
/// and the runtime claim also blocks as defense-in-depth.
#[tokio::test]
async fn test_capacity_requires_expected_count() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // Validation rejects: Capacity rule without expected_count → 400
    let cap_rules_json = json!([{
        "type": "Capacity",
        "max_capacity": 500,
        "matcher": { "kind": "cap-nocount", "status": "Running", "fields": [] }
    }]);

    let req = actix_web::test::TestRequest::post()
        .uri("/task")
        .insert_header(("requester", "test"))
        .set_json(&[json!({
            "id": "cap-nc",
            "name": "No Expected Count",
            "kind": "cap-nocount",
            "timeout": 60,
            "metadata": {},
            "on_start": webhook_action(),
            "rules": cap_rules_json
        })])
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(
        resp.status(),
        actix_web::http::StatusCode::BAD_REQUEST,
        "Task with Capacity rule but no expected_count should be rejected at creation"
    );

    // Runtime defense-in-depth: create a plain task (no capacity rule) then
    // try to claim it with a capacity rule and no expected_count.
    let tasks = vec![json!({
        "id": "cap-nc-plain",
        "name": "Plain Task",
        "kind": "cap-nocount",
        "timeout": 60,
        "metadata": {},
        "on_start": webhook_action()
    })];
    let created = create_tasks_ok(&app, &tasks).await;
    let id = created[0].id;

    let rules = capacity_rules("cap-nocount", 500, vec![]);

    // claim_task_with_rules checks expected_count at runtime → RuleBlocked
    let mut conn = state.pool.get().await.unwrap();
    let t = make_task_for_claim_with_expected(id, "cap-nocount", json!({}), rules.clone(), None);
    let result = claim_task_with_rules(&mut conn, &t).await.unwrap();
    assert_eq!(
        result,
        ClaimResult::RuleBlocked,
        "Candidate without expected_count should be blocked at runtime"
    );
}

/// Capacity rules only support matcher.status = Running.
#[tokio::test]
async fn test_capacity_requires_running_status() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let cap_rules_json = json!([{
        "type": "Capacity",
        "max_capacity": 500,
        "matcher": { "kind": "cap-status", "status": "Pending", "fields": [] }
    }]);

    let req = actix_web::test::TestRequest::post()
        .uri("/task")
        .insert_header(("requester", "test"))
        .set_json(&[json!({
            "id": "cap-bad-status",
            "name": "Bad Capacity Status",
            "kind": "cap-status",
            "timeout": 60,
            "expected_count": 10,
            "metadata": {},
            "on_start": webhook_action(),
            "rules": cap_rules_json
        })])
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(
        resp.status(),
        actix_web::http::StatusCode::BAD_REQUEST,
        "Capacity rule with matcher.status != Running should be rejected at creation"
    );
}

/// Running tasks with expected_count = 0 or NULL contribute 0 remaining capacity.
///
/// Under D1/7.3b semantics this holds for two reasons that both point the same way: the
/// two holders are claimed rule-less (`rules_empty`), so they do not occupy the `cap:`
/// slot at all (only tasks that claim THROUGH the rule count — the assumed D1 semantic
/// change), and a 0/NULL-expected task would in any case carry a charge of 0. Either way
/// the candidate is admitted.
#[tokio::test]
async fn test_capacity_zero_or_null_expected_count_contributes_zero() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let cap_rules_json = json!([{
        "type": "Capacity",
        "max_capacity": 1,
        "matcher": { "kind": "cap-zero", "status": "Running", "fields": [] }
    }]);

    let tasks = vec![
        json!({
            "id": "cap-null",
            "name": "Running Null Expected",
            "kind": "cap-zero",
            "timeout": 60,
            "metadata": {},
            "on_start": webhook_action()
        }),
        json!({
            "id": "cap-zero",
            "name": "Running Zero Expected",
            "kind": "cap-zero",
            "timeout": 60,
            "expected_count": 0,
            "metadata": {},
            "on_start": webhook_action()
        }),
        json!({
            "id": "cap-candidate",
            "name": "Capacity Candidate",
            "kind": "cap-zero",
            "timeout": 60,
            "expected_count": 10,
            "metadata": {},
            "on_start": webhook_action(),
            "rules": cap_rules_json.clone()
        }),
    ];

    let created = create_tasks_ok(&app, &tasks).await;
    let id_null = created[0].id;
    let id_zero = created[1].id;
    let id_candidate = created[2].id;

    let rules_empty = Rules(vec![]);
    let rules_capacity = capacity_rules("cap-zero", 1, vec![]);

    let mut conn = state.pool.get().await.unwrap();

    // Claim + mark running task with expected_count = NULL
    let t_null = make_task_for_claim_with_expected(
        id_null,
        "cap-zero",
        json!({}),
        rules_empty.clone(),
        None,
    );
    let result_null = claim_task_with_rules(&mut conn, &t_null).await.unwrap();
    assert_eq!(result_null, ClaimResult::Claimed);
    arcrun::db_operation::mark_task_running(&mut conn, &id_null)
        .await
        .unwrap();

    // Claim + mark running task with expected_count = 0
    let t_zero = make_task_for_claim_with_expected(
        id_zero,
        "cap-zero",
        json!({}),
        rules_empty.clone(),
        Some(0),
    );
    let result_zero = claim_task_with_rules(&mut conn, &t_zero).await.unwrap();
    assert_eq!(result_zero, ClaimResult::Claimed);
    arcrun::db_operation::mark_task_running(&mut conn, &id_zero)
        .await
        .unwrap();

    // Candidate should be allowed since both running tasks contribute 0 remaining.
    let t_candidate = make_task_for_claim_with_expected(
        id_candidate,
        "cap-zero",
        json!({}),
        rules_capacity.clone(),
        Some(10),
    );
    let result_candidate = claim_task_with_rules(&mut conn, &t_candidate)
        .await
        .unwrap();
    assert_eq!(
        result_candidate,
        ClaimResult::Claimed,
        "expected_count NULL/0 should contribute 0 remaining capacity"
    );
}

/// Different metadata fields → independent capacity pools.
#[tokio::test]
async fn test_capacity_respects_metadata_fields() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let cap_rules_json = json!([{
        "type": "Capacity",
        "max_capacity": 500,
        "matcher": { "kind": "cap-meta", "status": "Running", "fields": ["projectId"] }
    }]);

    let tasks = vec![
        json!({
            "id": "cap-m1",
            "name": "Project A Running",
            "kind": "cap-meta",
            "timeout": 60,
            "expected_count": 500,
            "metadata": {"projectId": "A"},
            "on_start": webhook_action(),
            "rules": cap_rules_json.clone()
        }),
        json!({
            "id": "cap-m2",
            "name": "Project B Candidate",
            "kind": "cap-meta",
            "timeout": 60,
            "expected_count": 100,
            "metadata": {"projectId": "B"},
            "on_start": webhook_action(),
            "rules": cap_rules_json.clone()
        }),
        json!({
            "id": "cap-m3",
            "name": "Project A Candidate",
            "kind": "cap-meta",
            "timeout": 60,
            "expected_count": 100,
            "metadata": {"projectId": "A"},
            "on_start": webhook_action(),
            "rules": cap_rules_json.clone()
        }),
    ];

    let created = create_tasks_ok(&app, &tasks).await;
    let id1 = created[0].id;
    let id2 = created[1].id;
    let id3 = created[2].id;

    let rules_a = capacity_rules("cap-meta", 500, vec!["projectId"]);

    // Claim + mark running task 1 (project A, remaining = 500)
    let mut conn = state.pool.get().await.unwrap();
    let t1 = make_task_for_claim_with_expected(
        id1,
        "cap-meta",
        json!({"projectId": "A"}),
        rules_a.clone(),
        Some(500),
    );
    let result1 = claim_task_with_rules(&mut conn, &t1).await.unwrap();
    assert_eq!(result1, ClaimResult::Claimed);
    arcrun::db_operation::mark_task_running(&mut conn, &id1)
        .await
        .unwrap();

    // Project B candidate: different pool, sum_B=0 < 500 → allowed
    let t2 = make_task_for_claim_with_expected(
        id2,
        "cap-meta",
        json!({"projectId": "B"}),
        rules_a.clone(),
        Some(100),
    );
    let result2 = claim_task_with_rules(&mut conn, &t2).await.unwrap();
    assert_eq!(
        result2,
        ClaimResult::Claimed,
        "Different project should not be blocked"
    );

    // Project A candidate: sum_A=500 >= 500 → blocked
    let t3 = make_task_for_claim_with_expected(
        id3,
        "cap-meta",
        json!({"projectId": "A"}),
        rules_a.clone(),
        Some(100),
    );
    let result3 = claim_task_with_rules(&mut conn, &t3).await.unwrap();
    assert_eq!(
        result3,
        ClaimResult::RuleBlocked,
        "Same project with sum=500 should be blocked"
    );
}

/// Combined Concurency + Capacity: both rules must pass (AND semantics).
#[tokio::test]
async fn test_capacity_combined_with_concurrency() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let combined_rules_json = json!([
        {
            "type": "Concurency",
            "max_concurency": 3,
            "matcher": { "kind": "cap-combo", "status": "Running", "fields": [] }
        },
        {
            "type": "Capacity",
            "max_capacity": 500,
            "matcher": { "kind": "cap-combo", "status": "Running", "fields": [] }
        }
    ]);

    // Create 4 tasks: 3 will run, 4th tests the combined rules
    let tasks: Vec<serde_json::Value> = (1..=4)
        .map(|i| {
            json!({
                "id": format!("combo-{}", i),
                "name": format!("Combo Task {}", i),
                "kind": "cap-combo",
                "timeout": 60,
                "expected_count": 100,
                "metadata": {},
                "on_start": webhook_action(),
                "rules": combined_rules_json.clone()
            })
        })
        .collect();

    let created = create_tasks_ok(&app, &tasks).await;
    let ids: Vec<uuid::Uuid> = created.iter().map(|t| t.id).collect();

    let combined_rules = Rules(vec![
        Strategy::Concurency(ConcurencyRule {
            max_concurency: 3,
            matcher: Matcher {
                kind: "cap-combo".to_string(),
                status: StatusKind::Running,
                fields: vec![],
            },
        }),
        Strategy::Capacity(CapacityRule {
            max_capacity: 500,
            matcher: Matcher {
                kind: "cap-combo".to_string(),
                status: StatusKind::Running,
                fields: vec![],
            },
        }),
    ]);

    let mut conn = state.pool.get().await.unwrap();

    // Claim + run first 3 tasks (each remaining=100, sum=300, count=3)
    for (i, id) in ids.iter().take(3).enumerate() {
        let t = make_task_for_claim_with_expected(
            *id,
            "cap-combo",
            json!({}),
            combined_rules.clone(),
            Some(100),
        );
        let result = claim_task_with_rules(&mut conn, &t).await.unwrap();
        assert_eq!(
            result,
            ClaimResult::Claimed,
            "Task {} should be claimed",
            i + 1
        );
        arcrun::db_operation::mark_task_running(&mut conn, id)
            .await
            .unwrap();
    }

    // Task 4: sum=300 < 500 OK, but count=3 >= max_concurency=3 → blocked by concurrency
    let t4 = make_task_for_claim_with_expected(
        ids[3],
        "cap-combo",
        json!({}),
        combined_rules.clone(),
        Some(100),
    );
    let result4 = claim_task_with_rules(&mut conn, &t4).await.unwrap();
    assert_eq!(
        result4,
        ClaimResult::RuleBlocked,
        "Should be blocked: capacity OK (300 < 500) but concurrency limit reached (3 >= 3)"
    );

    // Complete task 1 so concurrency has room (count=2)
    let update_result =
        arcrun::db_operation::update_running_task(&mut conn, ids[0], success_dto(), true)
            .await
            .unwrap();
    assert_eq!(
        update_result,
        arcrun::db_operation::UpdateTaskResult::Updated
    );

    // Now count=2 < 3 and sum=200 < 500 → both pass
    let result5 = claim_task_with_rules(&mut conn, &t4).await.unwrap();
    assert_eq!(
        result5,
        ClaimResult::Claimed,
        "After completing task 1: concurrency OK (2 < 3) and capacity OK (200 < 500)"
    );
}
