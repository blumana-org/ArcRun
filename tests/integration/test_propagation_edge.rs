use crate::common::*;

use arcrun::models::StatusKind;

/// Priority 4 — Propagation edge cases.
///
/// Test: Canceled parent with requires_success=false child.
/// Child should transition to Pending (not fail) because it doesn't require success.
#[tokio::test]
async fn test_canceled_parent_optional_child_becomes_pending() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let tasks = vec![
        task_json("opt-parent", "Optional Parent", "opt-kind"),
        task_with_deps(
            "opt-child",
            "Optional Child",
            "opt-kind",
            vec![("opt-parent", false)], // requires_success = false
        ),
    ];

    let created = create_tasks_ok(&app, &tasks).await;
    let parent_id = created[0].id;
    let child_id = created[1].id;

    assert_eq!(created[1].status, StatusKind::Waiting);

    // Cancel the parent
    let cancel_req = actix_web::test::TestRequest::delete()
        .uri(&format!("/task/{}", parent_id))
        .to_request();
    let resp = actix_web::test::call_service(&app, cancel_req).await;
    assert!(resp.status().is_success());

    // Child should become Pending (not Failure) since requires_success=false
    assert_task_status(
        &app,
        child_id,
        StatusKind::Pending,
        "optional child should become Pending when parent is canceled",
    )
    .await;
}
