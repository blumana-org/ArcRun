use crate::common::*;

use arcrun::dtos::{BatchStatsDto, BatchSummaryDto};
use serde_json::json;

/// Priority 2 — GET /batch/{batch_id} (batch stats) and GET /batches (batch listing).
///
/// Test correct aggregation of success/failure counters across tasks.
#[tokio::test]
async fn test_batch_stats_aggregation() {
    let (_g, test_state) = setup_test_app_with_batch_updater().await;
    let state = test_state.state;
    let app = test_service!(state);

    let tasks = vec![
        task_json("bs-t1", "Batch Stats Task 1", "bs-kind"),
        task_json("bs-t2", "Batch Stats Task 2", "bs-kind"),
    ];
    let created = create_tasks_ok(&app, &tasks).await;
    let batch_id = created[0].batch_id.unwrap();

    // Complete task 1 as Success, task 2 as Failure
    succeed_task(&state, created[0].id).await;
    fail_task(&state, created[1].id, "test fail").await;

    let req = actix_web::test::TestRequest::get()
        .uri(&format!("/batch/{}", batch_id))
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::OK);

    let stats: BatchStatsDto = actix_web::test::read_body_json(resp).await;
    assert_eq!(stats.batch_id, batch_id);
    assert_eq!(stats.total_tasks, 2);
    assert_eq!(stats.status_counts.success, 1);
    assert_eq!(stats.status_counts.failure, 1);
}

/// Test per-status counts correctness.
#[tokio::test]
async fn test_batch_stats_per_status_counts() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let tasks = vec![
        task_json("bs-p1", "Pending 1", "bs-kind"),
        task_json("bs-p2", "Pending 2", "bs-kind"),
        task_with_deps("bs-w1", "Waiting 1", "bs-kind", vec![("bs-p1", true)]),
    ];
    let created = create_tasks_ok(&app, &tasks).await;
    let batch_id = created[0].batch_id.unwrap();

    let req = actix_web::test::TestRequest::get()
        .uri(&format!("/batch/{}", batch_id))
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::OK);

    let stats: BatchStatsDto = actix_web::test::read_body_json(resp).await;
    assert_eq!(stats.total_tasks, 3);
    assert_eq!(stats.status_counts.pending, 2);
    assert_eq!(stats.status_counts.waiting, 1);
}

/// Test 404 for non-existent batch_id.
#[tokio::test]
async fn test_batch_stats_not_found() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let fake_id = uuid::Uuid::new_v4();
    let req = actix_web::test::TestRequest::get()
        .uri(&format!("/batch/{}", fake_id))
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::NOT_FOUND);
}

/// Test GET /batches lists batches with correct stats.
#[tokio::test]
async fn test_list_batches() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // Create two separate batches
    let tasks1 = vec![task_json("lb-1a", "Batch 1 Task A", "lb-kind1")];
    let tasks2 = vec![task_json("lb-2a", "Batch 2 Task A", "lb-kind2")];
    let created1 = create_tasks_ok(&app, &tasks1).await;
    let created2 = create_tasks_ok(&app, &tasks2).await;
    let batch1 = created1[0].batch_id.unwrap();
    let batch2 = created2[0].batch_id.unwrap();

    let req = actix_web::test::TestRequest::get()
        .uri("/batches")
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::OK);

    let batches: Vec<BatchSummaryDto> = actix_web::test::read_body_json(resp).await;
    assert!(batches.len() >= 2, "should list at least 2 batches");

    let batch_ids: Vec<uuid::Uuid> = batches.iter().map(|b| b.batch_id).collect();
    assert!(batch_ids.contains(&batch1), "should contain batch 1");
    assert!(batch_ids.contains(&batch2), "should contain batch 2");
}

/// Test GET /batches pagination.
#[tokio::test]
async fn test_list_batches_pagination() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // Create 3 batches
    for i in 0..3 {
        let tasks = vec![task_json(
            &format!("lbp-{}", i),
            &format!("Batch Pag {}", i),
            "lbp-kind",
        )];
        create_tasks_ok(&app, &tasks).await;
    }

    let req = actix_web::test::TestRequest::get()
        .uri("/batches?page=0&page_size=2")
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::OK);

    let batches: Vec<BatchSummaryDto> = actix_web::test::read_body_json(resp).await;
    assert_eq!(batches.len(), 2, "page_size=2 should return 2 batches");
}

/// Test GET /batches filtering by kind.
#[tokio::test]
async fn test_list_batches_filter_by_kind() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let tasks_a = vec![task_json("lbf-a", "Filter A", "filter-alpha")];
    let tasks_b = vec![task_json("lbf-b", "Filter B", "filter-beta")];
    let created_a = create_tasks_ok(&app, &tasks_a).await;
    create_tasks_ok(&app, &tasks_b).await;
    let batch_a = created_a[0].batch_id.unwrap();

    let req = actix_web::test::TestRequest::get()
        .uri("/batches?kind=filter-alpha")
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::OK);

    let batches: Vec<BatchSummaryDto> = actix_web::test::read_body_json(resp).await;
    assert!(
        batches.iter().any(|b| b.batch_id == batch_a),
        "should include batch with filter-alpha kind"
    );
    // All returned batches should contain filter-alpha
    for b in &batches {
        assert!(
            b.kinds.iter().any(|k| k.contains("filter-alpha")),
            "batch {} should contain filter-alpha kind, got kinds: {:?}",
            b.batch_id,
            b.kinds,
        );
    }
}

/// Test GET /batches filtering by status.
#[tokio::test]
async fn test_list_batches_filter_by_status() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // Create two batches: one with a Success task, one all Pending
    let tasks_done = vec![task_json("lbs-done", "Done Task", "lbs-kind")];
    let tasks_pending = vec![task_json("lbs-pend", "Pending Task", "lbs-kind2")];
    let created_done = create_tasks_ok(&app, &tasks_done).await;
    let created_pend = create_tasks_ok(&app, &tasks_pending).await;
    let batch_done = created_done[0].batch_id.unwrap();
    let batch_pend = created_pend[0].batch_id.unwrap();

    succeed_task(&state, created_done[0].id).await;

    // Filter by status=Success — only batch_done should appear
    let req = actix_web::test::TestRequest::get()
        .uri("/batches?status=Success")
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::OK);

    let batches: Vec<BatchSummaryDto> = actix_web::test::read_body_json(resp).await;
    let batch_ids: Vec<uuid::Uuid> = batches.iter().map(|b| b.batch_id).collect();
    assert!(
        batch_ids.contains(&batch_done),
        "should contain batch with Success task"
    );
    assert!(
        !batch_ids.contains(&batch_pend),
        "should NOT contain batch that has no Success tasks"
    );
}

/// Test GET /batches filtering by name substring.
#[tokio::test]
async fn test_list_batches_filter_by_name() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let tasks = vec![json!({
        "id": "lbn-unique",
        "name": "UniqueXYZ123Name",
        "kind": "lbn-kind",
        "timeout": 60,
        "metadata": {"test": true},
        "on_start": webhook_action()
    })];
    let created = create_tasks_ok(&app, &tasks).await;
    let batch_id = created[0].batch_id.unwrap();

    let req = actix_web::test::TestRequest::get()
        .uri("/batches?name=UniqueXYZ123")
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::OK);

    let batches: Vec<BatchSummaryDto> = actix_web::test::read_body_json(resp).await;
    assert!(
        batches.iter().any(|b| b.batch_id == batch_id),
        "should find batch by name substring"
    );
}
