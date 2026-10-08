use crate::common::*;

use serde_json::json;

#[tokio::test]
async fn test_dedupe_skip_existing() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // Create first task with metadata
    let task1 = json!({
        "id": "dedupe-1",
        "name": "Dedupe Task 1",
        "kind": "dedupe-kind",
        "timeout": 60,
        "metadata": {"unique_key": "abc123"},
        "on_start": webhook_action()
    });

    create_tasks_ok(&app, &[task1]).await;

    // Create second task with same metadata but dedupe strategy
    let task2 = json!({
        "id": "dedupe-2",
        "name": "Dedupe Task 2",
        "kind": "dedupe-kind",
        "timeout": 60,
        "metadata": {"unique_key": "abc123"},
        "on_start": webhook_action(),
        "dedupe_strategy": [{
            "kind": "dedupe-kind",
            "status": "Pending",
            "fields": ["unique_key"]
        }]
    });

    let create_req2 = actix_web::test::TestRequest::post()
        .uri("/task")
        .insert_header(("requester", "test"))
        .set_json(vec![task2])
        .to_request();
    let create_resp2 = actix_web::test::call_service(&app, create_req2).await;
    // Should return NoContent because task was deduplicated
    assert_eq!(
        create_resp2.status(),
        actix_web::http::StatusCode::NO_CONTENT
    );
}

/// Bug #7: Over-aggressive dedupe when metadata is None.
#[tokio::test]
async fn test_bug7_dedupe_not_over_aggressive_when_metadata_is_none() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // Step 1: Create first task WITH metadata
    let task1 = json!({
        "id": "dedupe-existing",
        "name": "Existing Task With Metadata",
        "kind": "dedupe-none-kind",
        "timeout": 60,
        "metadata": {"unique_key": "abc123", "project": "test"},
        "on_start": webhook_action()
    });

    create_tasks_ok(&app, &[task1]).await;

    // Step 2: Create second task WITHOUT metadata, WITH dedupe_strategy referencing fields.
    let task2 = json!({
        "id": "dedupe-no-meta",
        "name": "Task Without Metadata",
        "kind": "dedupe-none-kind",
        "timeout": 60,
        "on_start": webhook_action(),
        "dedupe_strategy": [{
            "kind": "dedupe-none-kind",
            "status": "Pending",
            "fields": ["unique_key"]
        }]
    });

    let create_req2 = actix_web::test::TestRequest::post()
        .uri("/task")
        .insert_header(("requester", "test"))
        .set_json(vec![task2])
        .to_request();
    let create_resp2 = actix_web::test::call_service(&app, create_req2).await;
    assert_eq!(
        create_resp2.status(),
        actix_web::http::StatusCode::CREATED,
        "Task without metadata should NOT be deduplicated against task with metadata"
    );
}

/// Dedup against non-Pending tasks.
/// When a matcher specifies status=Running, the dedup check should match against Running tasks.
#[tokio::test]
async fn test_dedupe_against_running_task() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // Create a task and move it to Running
    let task1 = json!({
        "id": "dedupe-running-1",
        "name": "Running Dedupe Task",
        "kind": "dedupe-run-kind",
        "timeout": 60,
        "metadata": {"project": "alpha"},
        "on_start": webhook_action()
    });
    let created = create_tasks_ok(&app, &[task1]).await;
    let task_id = created[0].id;

    // Move to Running
    let mut conn = state.pool.get().await.unwrap();
    arcrun::db_operation::claim_task(&mut conn, &task_id)
        .await
        .unwrap();
    arcrun::db_operation::mark_task_running(&mut conn, &task_id)
        .await
        .unwrap();
    drop(conn);

    // Try to create a duplicate with dedupe_strategy matching Running status
    let task2 = json!({
        "id": "dedupe-running-2",
        "name": "Duplicate of Running",
        "kind": "dedupe-run-kind",
        "timeout": 60,
        "metadata": {"project": "alpha"},
        "on_start": webhook_action(),
        "dedupe_strategy": [{
            "kind": "dedupe-run-kind",
            "status": "Running",
            "fields": ["project"]
        }]
    });

    let req = actix_web::test::TestRequest::post()
        .uri("/task")
        .insert_header(("requester", "test"))
        .set_json(vec![task2])
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(
        resp.status(),
        actix_web::http::StatusCode::NO_CONTENT,
        "Task should be deduplicated against the Running task"
    );
}

/// Dedup with status=Pending should NOT match a Running task.
#[tokio::test]
async fn test_dedupe_pending_does_not_match_running() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // Create a task and move it to Running
    let task1 = json!({
        "id": "dedupe-nomatch-1",
        "name": "Running No Match",
        "kind": "dedupe-nm-kind",
        "timeout": 60,
        "metadata": {"project": "beta"},
        "on_start": webhook_action()
    });
    let created = create_tasks_ok(&app, &[task1]).await;
    let task_id = created[0].id;

    let mut conn = state.pool.get().await.unwrap();
    arcrun::db_operation::claim_task(&mut conn, &task_id)
        .await
        .unwrap();
    arcrun::db_operation::mark_task_running(&mut conn, &task_id)
        .await
        .unwrap();
    drop(conn);

    // Try to create with dedupe matching Pending — should NOT match the Running task
    let task2 = json!({
        "id": "dedupe-nomatch-2",
        "name": "No Match Pending",
        "kind": "dedupe-nm-kind",
        "timeout": 60,
        "metadata": {"project": "beta"},
        "on_start": webhook_action(),
        "dedupe_strategy": [{
            "kind": "dedupe-nm-kind",
            "status": "Pending",
            "fields": ["project"]
        }]
    });

    let req = actix_web::test::TestRequest::post()
        .uri("/task")
        .insert_header(("requester", "test"))
        .set_json(vec![task2])
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(
        resp.status(),
        actix_web::http::StatusCode::CREATED,
        "Pending matcher should NOT match Running task — new task should be created"
    );
}

/// Bug #7 (variant): Two tasks with None metadata should not dedupe each other.
#[tokio::test]
async fn test_bug7_dedupe_two_tasks_both_no_metadata() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // Create first task without metadata
    let task1 = json!({
        "id": "no-meta-1",
        "name": "No Metadata Task 1",
        "kind": "no-meta-dedupe-kind",
        "timeout": 60,
        "on_start": webhook_action()
    });

    create_tasks_ok(&app, &[task1]).await;

    // Create second task without metadata, with dedupe strategy referencing fields
    let task2 = json!({
        "id": "no-meta-2",
        "name": "No Metadata Task 2",
        "kind": "no-meta-dedupe-kind",
        "timeout": 60,
        "on_start": webhook_action(),
        "dedupe_strategy": [{
            "kind": "no-meta-dedupe-kind",
            "status": "Pending",
            "fields": ["some_field"]
        }]
    });

    let create_req2 = actix_web::test::TestRequest::post()
        .uri("/task")
        .insert_header(("requester", "test"))
        .set_json(vec![task2])
        .to_request();
    let create_resp2 = actix_web::test::call_service(&app, create_req2).await;
    assert_eq!(
        create_resp2.status(),
        actix_web::http::StatusCode::CREATED,
        "Second task (no metadata) should be created"
    );
}
