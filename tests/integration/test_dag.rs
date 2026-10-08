use crate::common::*;

use arcrun::dtos::DagDto;
use arcrun::models::StatusKind;

#[tokio::test]
async fn test_task_with_single_dependency() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let tasks = vec![
        task_json("parent", "Parent Task", "parent-kind"),
        task_with_deps("child", "Child Task", "child-kind", vec![("parent", true)]),
    ];

    let body = create_tasks_ok(&app, &tasks).await;
    assert_eq!(body.len(), 2);
    assert_eq!(body[0].status, StatusKind::Pending);
    assert_eq!(body[1].status, StatusKind::Waiting);
}

#[tokio::test]
async fn test_task_with_multiple_dependencies() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let tasks = vec![
        task_json("parent-1", "Parent 1", "ingest"),
        task_json("parent-2", "Parent 2", "ingest"),
        task_json("parent-3", "Parent 3", "ingest"),
        task_with_deps(
            "aggregator",
            "Aggregator Task",
            "aggregate",
            vec![("parent-1", true), ("parent-2", true), ("parent-3", false)],
        ),
    ];

    let body = create_tasks_ok(&app, &tasks).await;
    assert_eq!(body.len(), 4);
    assert_eq!(body[0].status, StatusKind::Pending);
    assert_eq!(body[1].status, StatusKind::Pending);
    assert_eq!(body[2].status, StatusKind::Pending);
    assert_eq!(body[3].status, StatusKind::Waiting);
}

#[tokio::test]
async fn test_diamond_dag_pattern() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // Diamond pattern: A -> B, A -> C, B -> D, C -> D
    let tasks = vec![
        task_json("A", "Task A", "diamond"),
        task_with_deps("B", "Task B", "diamond", vec![("A", true)]),
        task_with_deps("C", "Task C", "diamond", vec![("A", true)]),
        task_with_deps("D", "Task D", "diamond", vec![("B", true), ("C", true)]),
    ];

    let body = create_tasks_ok(&app, &tasks).await;
    assert_eq!(body.len(), 4);
    assert_eq!(body[0].status, StatusKind::Pending);
    assert_eq!(body[1].status, StatusKind::Waiting);
    assert_eq!(body[2].status, StatusKind::Waiting);
    assert_eq!(body[3].status, StatusKind::Waiting);
}

#[tokio::test]
async fn test_linear_chain_dag() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // Linear chain: A -> B -> C -> D
    let tasks = vec![
        task_json("chain-A", "Chain A", "chain"),
        task_with_deps("chain-B", "Chain B", "chain", vec![("chain-A", true)]),
        task_with_deps("chain-C", "Chain C", "chain", vec![("chain-B", true)]),
        task_with_deps("chain-D", "Chain D", "chain", vec![("chain-C", true)]),
    ];

    let body = create_tasks_ok(&app, &tasks).await;
    assert_eq!(body.len(), 4);
    assert_eq!(body[0].status, StatusKind::Pending);
    assert_eq!(body[1].status, StatusKind::Waiting);
    assert_eq!(body[2].status, StatusKind::Waiting);
    assert_eq!(body[3].status, StatusKind::Waiting);
}

#[tokio::test]
async fn test_fan_out_dag() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // Fan-out: A -> B, A -> C, A -> D, A -> E
    let tasks = vec![
        task_json("fanout-A", "Fanout A", "fanout"),
        task_with_deps("fanout-B", "Fanout B", "fanout", vec![("fanout-A", true)]),
        task_with_deps("fanout-C", "Fanout C", "fanout", vec![("fanout-A", true)]),
        task_with_deps("fanout-D", "Fanout D", "fanout", vec![("fanout-A", true)]),
        task_with_deps("fanout-E", "Fanout E", "fanout", vec![("fanout-A", true)]),
    ];

    let body = create_tasks_ok(&app, &tasks).await;
    assert_eq!(body.len(), 5);
    assert_eq!(body[0].status, StatusKind::Pending);
    for task in &body[1..5] {
        assert_eq!(task.status, StatusKind::Waiting);
    }
}

#[tokio::test]
async fn test_fan_in_dag() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // Fan-in: A, B, C, D -> E
    let tasks = vec![
        task_json("fanin-A", "Fanin A", "fanin"),
        task_json("fanin-B", "Fanin B", "fanin"),
        task_json("fanin-C", "Fanin C", "fanin"),
        task_json("fanin-D", "Fanin D", "fanin"),
        task_with_deps(
            "fanin-E",
            "Fanin E",
            "fanin",
            vec![
                ("fanin-A", true),
                ("fanin-B", true),
                ("fanin-C", true),
                ("fanin-D", true),
            ],
        ),
    ];

    let body = create_tasks_ok(&app, &tasks).await;
    assert_eq!(body.len(), 5);
    for task in &body[..4] {
        assert_eq!(task.status, StatusKind::Pending);
    }
    assert_eq!(body[4].status, StatusKind::Waiting);
}

#[tokio::test]
async fn test_mixed_dependency_requirements() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let tasks = vec![
        task_json("mixed-A", "Mixed A", "mixed"),
        task_json("mixed-B", "Mixed B", "mixed"),
        task_with_deps(
            "mixed-C",
            "Mixed C",
            "mixed",
            vec![
                ("mixed-A", true),  // requires success
                ("mixed-B", false), // just needs to finish
            ],
        ),
    ];

    let body = create_tasks_ok(&app, &tasks).await;
    assert_eq!(body.len(), 3);
    assert_eq!(body[2].status, StatusKind::Waiting);
}

/// Verify that GET /dag/{batch_id} returns correct tasks and links.
#[tokio::test]
async fn test_dag_endpoint_returns_tasks_and_links() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // Diamond: A -> B, A -> C, B -> D, C -> D
    let tasks = vec![
        task_json("dag-A", "Task A", "dag-endpoint"),
        task_with_deps("dag-B", "Task B", "dag-endpoint", vec![("dag-A", true)]),
        task_with_deps("dag-C", "Task C", "dag-endpoint", vec![("dag-A", false)]),
        task_with_deps(
            "dag-D",
            "Task D",
            "dag-endpoint",
            vec![("dag-B", true), ("dag-C", true)],
        ),
    ];

    // Use raw POST to capture X-Batch-ID header
    let req = actix_web::test::TestRequest::post()
        .uri("/task")
        .insert_header(("requester", "test"))
        .set_json(&tasks)
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::CREATED);

    let batch_id = resp
        .headers()
        .get("X-Batch-ID")
        .expect("Response should have X-Batch-ID header")
        .to_str()
        .unwrap()
        .to_string();

    let created: Vec<arcrun::dtos::BasicTaskDto> = actix_web::test::read_body_json(resp).await;
    assert_eq!(created.len(), 4);

    let id_a = created[0].id;
    let id_b = created[1].id;
    let id_c = created[2].id;
    let id_d = created[3].id;

    // GET /dag/{batch_id}
    let dag_req = actix_web::test::TestRequest::get()
        .uri(&format!("/dag/{}", batch_id))
        .to_request();
    let dag_resp = actix_web::test::call_service(&app, dag_req).await;
    assert!(dag_resp.status().is_success());

    let dag: DagDto = actix_web::test::read_body_json(dag_resp).await;

    // Verify all 4 tasks are returned
    assert_eq!(dag.tasks.len(), 4, "DAG should have 4 tasks");
    let dag_ids: std::collections::HashSet<uuid::Uuid> = dag.tasks.iter().map(|t| t.id).collect();
    assert!(dag_ids.contains(&id_a));
    assert!(dag_ids.contains(&id_b));
    assert!(dag_ids.contains(&id_c));
    assert!(dag_ids.contains(&id_d));

    // Verify 4 links: A->B, A->C, B->D, C->D
    assert_eq!(dag.links.len(), 4, "Diamond DAG should have 4 links");

    let has_link = |parent: uuid::Uuid, child: uuid::Uuid, requires_success: bool| {
        dag.links.iter().any(|l| {
            l.parent_id == parent && l.child_id == child && l.requires_success == requires_success
        })
    };

    assert!(has_link(id_a, id_b, true), "A -> B (requires_success=true)");
    assert!(
        has_link(id_a, id_c, false),
        "A -> C (requires_success=false)"
    );
    assert!(has_link(id_b, id_d, true), "B -> D (requires_success=true)");
    assert!(has_link(id_c, id_d, true), "C -> D (requires_success=true)");
}
