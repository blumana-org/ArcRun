use crate::common::*;
use arcrun::db_operation::{ClaimResult, claim_task_with_rules};
use arcrun::dtos::UpdateBatchRulesResponseDto;
use arcrun::models::{StatusKind, Task};
use serde_json::json;

/// PATCH /batch/{batch_id}/rules updates only tasks of the specified kind.
#[tokio::test]
async fn test_update_batch_rules_by_kind() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // Create a batch with tasks of two different kinds
    let tasks = vec![
        json!({
            "id": "build",
            "name": "Build Task",
            "kind": "ci",
            "timeout": 60,
            "expected_count": 100,
            "metadata": {"project": "test"},
            "on_start": webhook_action()
        }),
        json!({
            "id": "deploy",
            "name": "Deploy Task",
            "kind": "deploy",
            "timeout": 60,
            "expected_count": 200,
            "metadata": {"project": "test"},
            "on_start": webhook_action(),
            "dependencies": [{"id": "build", "requires_success": true}]
        }),
        json!({
            "id": "build2",
            "name": "Build Task 2",
            "kind": "ci",
            "timeout": 60,
            "expected_count": 300,
            "metadata": {"project": "test"},
            "on_start": webhook_action(),
            "dependencies": [{"id": "build", "requires_success": true}]
        }),
    ];

    let created = create_tasks_ok(&app, &tasks).await;
    let batch_id = created[0].batch_id.unwrap();

    // Update rules only for kind "ci"
    let new_rules = json!({
        "kind": "ci",
        "rules": [{
            "type": "Capacity",
            "max_capacity": 500,
            "matcher": {
                "kind": "ci",
                "status": "Running",
                "fields": ["project"]
            }
        }]
    });

    let req = actix_web::test::TestRequest::patch()
        .uri(&format!("/batch/{}/rules", batch_id))
        .set_json(&new_rules)
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::OK);

    let body: UpdateBatchRulesResponseDto = actix_web::test::read_body_json(resp).await;
    assert_eq!(body.batch_id, batch_id);
    assert_eq!(body.kind, "ci");
    assert_eq!(
        body.updated_count, 2,
        "Only the 2 'ci' tasks should be updated"
    );

    // Verify "ci" tasks have the new rule
    let build = get_task_ok(&app, created[0].id).await;
    assert_eq!(build.rules.0.len(), 1, "ci task should have 1 rule");
    match &build.rules.0[0] {
        arcrun::rule::Strategy::Capacity(rule) => {
            assert_eq!(rule.max_capacity, 500);
        }
        other => panic!("Expected Capacity rule, got {:?}", other),
    }

    let build2 = get_task_ok(&app, created[2].id).await;
    assert_eq!(build2.rules.0.len(), 1, "second ci task should have 1 rule");

    // Verify "deploy" task was NOT updated
    let deploy = get_task_ok(&app, created[1].id).await;
    assert!(
        deploy.rules.0.is_empty(),
        "deploy task should have no rules"
    );
}

/// Terminal tasks are skipped even when their kind matches.
#[tokio::test]
async fn test_update_batch_rules_skips_terminal_tasks() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let tasks = vec![
        json!({
            "id": "will-succeed",
            "name": "Will Succeed",
            "kind": "rule-terminal",
            "timeout": 60,
            "metadata": {},
            "on_start": webhook_action()
        }),
        json!({
            "id": "will-wait",
            "name": "Will Wait",
            "kind": "rule-terminal",
            "timeout": 60,
            "metadata": {},
            "on_start": webhook_action(),
            "dependencies": [{"id": "will-succeed", "requires_success": true}]
        }),
    ];

    let created = create_tasks_ok(&app, &tasks).await;
    let batch_id = created[0].batch_id.unwrap();

    // Complete the first task -> it becomes Success (terminal)
    succeed_task(&state, created[0].id).await;

    // Now update batch rules for kind "rule-terminal"
    let new_rules = json!({
        "kind": "rule-terminal",
        "rules": [{
            "type": "Concurency",
            "max_concurency": 2,
            "matcher": {
                "kind": "rule-terminal",
                "status": "Running",
                "fields": []
            }
        }]
    });

    let req = actix_web::test::TestRequest::patch()
        .uri(&format!("/batch/{}/rules", batch_id))
        .set_json(&new_rules)
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::OK);

    let body: UpdateBatchRulesResponseDto = actix_web::test::read_body_json(resp).await;
    assert_eq!(
        body.updated_count, 1,
        "Only 1 non-terminal task should be updated"
    );

    // Verify the succeeded task still has no rules
    let succeeded = get_task_ok(&app, created[0].id).await;
    assert!(
        succeeded.rules.0.is_empty(),
        "Terminal task should keep its original rules"
    );

    // Verify the waiting child got the new rule
    let child = get_task_ok(&app, created[1].id).await;
    assert_eq!(
        child.rules.0.len(),
        1,
        "Waiting child should have the new rule"
    );
}

/// Claimed/Running tasks may already hold slots derived from their original rules.
/// Updating their rule JSON in place would make the visible policy disagree with the
/// slots that will later be released, so only pre-claim states are mutable.
#[tokio::test]
async fn test_update_batch_rules_skips_claimed_tasks() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let created = create_tasks_ok(
        &app,
        &[
            json!({
                "id": "claimed",
                "name": "Claimed",
                "kind": "rule-active",
                "timeout": 60,
                "metadata": {},
                "on_start": webhook_action()
            }),
            json!({
                "id": "pending",
                "name": "Pending",
                "kind": "rule-active",
                "timeout": 60,
                "metadata": {},
                "on_start": webhook_action()
            }),
        ],
    )
    .await;
    let batch_id = created[0].batch_id.unwrap();

    let mut conn = state.pool.get().await.unwrap();
    assert!(
        arcrun::db_operation::claim_task(&mut conn, &created[0].id)
            .await
            .unwrap()
    );
    drop(conn);

    let req = actix_web::test::TestRequest::patch()
        .uri(&format!("/batch/{}/rules", batch_id))
        .set_json(json!({
            "kind": "rule-active",
            "rules": [{
                "type": "Concurency",
                "max_concurency": 2,
                "matcher": { "kind": "rule-active", "status": "Running", "fields": [] }
            }]
        }))
        .to_request();
    let response = actix_web::test::call_service(&app, req).await;
    assert_eq!(response.status(), actix_web::http::StatusCode::OK);
    let body: UpdateBatchRulesResponseDto = actix_web::test::read_body_json(response).await;
    assert_eq!(body.updated_count, 1, "only the Pending task is mutable");

    assert!(
        get_task_ok(&app, created[0].id).await.rules.0.is_empty(),
        "Claimed task keeps its original rules"
    );
    assert_eq!(
        get_task_ok(&app, created[1].id).await.rules.0.len(),
        1,
        "Pending task receives the new rules"
    );
}

/// Empty rules array removes all rules for the specified kind.
#[tokio::test]
async fn test_update_batch_rules_with_empty_removes_rules() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let tasks = vec![json!({
        "id": "has-rules",
        "name": "Task With Rules",
        "kind": "clear-rules",
        "timeout": 60,
        "expected_count": 100,
        "metadata": {},
        "on_start": webhook_action(),
        "rules": [{
            "type": "Capacity",
            "max_capacity": 500,
            "matcher": { "kind": "clear-rules", "status": "Running", "fields": [] }
        }]
    })];

    let created = create_tasks_ok(&app, &tasks).await;
    let batch_id = created[0].batch_id.unwrap();

    // Verify task has rule
    let task = get_task_ok(&app, created[0].id).await;
    assert_eq!(task.rules.0.len(), 1);

    // Clear rules for kind "clear-rules"
    let req = actix_web::test::TestRequest::patch()
        .uri(&format!("/batch/{}/rules", batch_id))
        .set_json(json!({ "kind": "clear-rules", "rules": [] }))
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::OK);

    let task = get_task_ok(&app, created[0].id).await;
    assert!(
        task.rules.0.is_empty(),
        "Rules should be empty after clearing"
    );
}

/// Returns 404 for non-existent batch.
#[tokio::test]
async fn test_update_batch_rules_not_found() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let fake_batch_id = uuid::Uuid::new_v4();
    let req = actix_web::test::TestRequest::patch()
        .uri(&format!("/batch/{}/rules", fake_batch_id))
        .set_json(json!({ "kind": "anything", "rules": [] }))
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::NOT_FOUND);
}

/// #29: Capacity rule applied to tasks without expected_count.
/// The candidate task MUST have expected_count set to be claimable with a capacity rule.
/// When expected_count is None, claim_task_with_rules blocks the task.
/// This test verifies that behavior when batch rules are updated after creation.
#[tokio::test]
async fn test_capacity_rule_with_no_expected_count() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // Create tasks WITHOUT expected_count
    let tasks = vec![
        json!({
            "id": "cap-no-ec-1",
            "name": "No Expected Count 1",
            "kind": "cap-no-ec",
            "timeout": 60,
            "metadata": {"project": "test"},
            "on_start": webhook_action()
        }),
        // Second task WITH expected_count for comparison
        json!({
            "id": "cap-with-ec",
            "name": "With Expected Count",
            "kind": "cap-no-ec",
            "timeout": 60,
            "metadata": {"project": "test"},
            "expected_count": 50,
            "on_start": webhook_action()
        }),
    ];

    let created = create_tasks_ok(&app, &tasks).await;
    let batch_id = created[0].batch_id.unwrap();
    let task_no_ec = created[0].id;
    let task_with_ec = created[1].id;

    // Apply a capacity rule via batch rules update
    let new_rules = json!({
        "kind": "cap-no-ec",
        "rules": [{
            "type": "Capacity",
            "max_capacity": 100,
            "matcher": {
                "kind": "cap-no-ec",
                "status": "Running",
                "fields": ["project"]
            }
        }]
    });

    let req = actix_web::test::TestRequest::patch()
        .uri(&format!("/batch/{}/rules", batch_id))
        .set_json(&new_rules)
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::OK);

    let body: UpdateBatchRulesResponseDto = actix_web::test::read_body_json(resp).await;
    assert_eq!(body.updated_count, 2, "both tasks should have been updated");

    // Verify the rules were applied
    let t_no_ec = get_task_ok(&app, task_no_ec).await;
    assert_eq!(t_no_ec.rules.0.len(), 1);
    assert!(
        t_no_ec.expected_count.is_none(),
        "task should have no expected_count"
    );

    let t_with_ec = get_task_ok(&app, task_with_ec).await;
    assert_eq!(t_with_ec.rules.0.len(), 1);
    assert_eq!(t_with_ec.expected_count, Some(50));

    let rules_for_claim = t_no_ec.rules.clone();

    // Task WITHOUT expected_count should be blocked by the capacity rule
    let task_model_no_ec = Task {
        id: task_no_ec,
        name: String::new(),
        kind: "cap-no-ec".to_string(),
        status: StatusKind::Pending,
        timeout: 60,
        created_at: chrono::Utc::now(),
        started_at: None,
        last_updated: chrono::Utc::now(),
        metadata: json!({"project": "test"}),
        ended_at: None,
        start_condition: rules_for_claim.clone(),
        wait_success: 0,
        wait_finished: 0,
        success: 0,
        failures: 0,
        failure_reason: None,
        batch_id: Some(batch_id),
        expected_count: None,
        dead_end_barrier: false,
        priority: 0,
        claimed_slot_keys: None,
        capacity_charge: None,
    };

    let mut conn = state.pool.get().await.unwrap();
    let claim_result = claim_task_with_rules(&mut conn, &task_model_no_ec)
        .await
        .unwrap();
    assert_eq!(
        claim_result,
        ClaimResult::RuleBlocked,
        "task without expected_count should be BLOCKED by capacity rule"
    );

    // Task WITH expected_count should be claimable
    let task_model_with_ec = Task {
        id: task_with_ec,
        expected_count: Some(50),
        ..task_model_no_ec
    };

    let claim_result2 = claim_task_with_rules(&mut conn, &task_model_with_ec)
        .await
        .unwrap();
    assert_eq!(
        claim_result2,
        ClaimResult::Claimed,
        "task with expected_count should be claimable with capacity rule"
    );
}

/// Returns 400 for invalid rules or empty kind.
#[tokio::test]
async fn test_update_batch_rules_validation_error() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let tasks = vec![json!({
        "id": "valid-task",
        "name": "Valid Task",
        "kind": "validate-rules",
        "timeout": 60,
        "metadata": {},
        "on_start": webhook_action()
    })];

    let created = create_tasks_ok(&app, &tasks).await;
    let batch_id = created[0].batch_id.unwrap();

    // Invalid: empty kind
    let req = actix_web::test::TestRequest::patch()
        .uri(&format!("/batch/{}/rules", batch_id))
        .set_json(json!({ "kind": "", "rules": [] }))
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::BAD_REQUEST);

    // Invalid: Capacity rule with matcher.status = Pending
    let req = actix_web::test::TestRequest::patch()
        .uri(&format!("/batch/{}/rules", batch_id))
        .set_json(json!({
            "kind": "validate-rules",
            "rules": [{
                "type": "Capacity",
                "max_capacity": 500,
                "matcher": { "kind": "validate-rules", "status": "Pending", "fields": [] }
            }]
        }))
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::BAD_REQUEST);

    // Invalid: Concurency rule with max_concurency = 0
    let req = actix_web::test::TestRequest::patch()
        .uri(&format!("/batch/{}/rules", batch_id))
        .set_json(json!({
            "kind": "validate-rules",
            "rules": [{
                "type": "Concurency",
                "max_concurency": 0,
                "matcher": { "kind": "validate-rules", "status": "Running", "fields": [] }
            }]
        }))
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::BAD_REQUEST);
}
