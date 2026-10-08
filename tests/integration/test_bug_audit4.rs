// Diesel QueryableByName emits separate impls with explicit field initializers;
// an allow on the struct does not cover those generated impls.
#![allow(clippy::redundant_field_names)]

//! Regressions for stale timeout/claim candidates and delivery pool occupancy.

use crate::common::*;
use arcrun::db_operation::{ClaimResult, batch_claim_tasks, claim_task_with_rules};
use arcrun::models::{StatusKind, Task};
use diesel::associations::HasTable;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use serde_json::json;
use std::time::Duration;

#[derive(diesel::QueryableByName)]
struct BackendPid {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    pid: i32,
}

#[derive(diesel::QueryableByName)]
struct Blocked {
    #[diesel(sql_type = diesel::sql_types::Bool)]
    blocked: bool,
}

#[derive(diesel::QueryableByName)]
struct Count {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    value: i64,
}

async fn wait_for_blocked(pool: &arcrun::DbPool, blocker_pid: i32) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let mut conn = pool.get().await.unwrap();
            let row: Blocked = diesel::sql_query(
                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity \
                 WHERE $1 = ANY(pg_blocking_pids(pid))) AS blocked",
            )
            .bind::<diesel::sql_types::Integer, _>(blocker_pid)
            .get_result(&mut conn)
            .await
            .unwrap();
            if row.blocked {
                break;
            }
            drop(conn);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("worker must reach the locked row");
}

fn concurrency_rules(limit: i32) -> serde_json::Value {
    json!([{
        "type": "Concurency", "max_concurency": limit,
        "matcher": {"kind": "audit", "status": "Running", "fields": []}
    }])
}

/// Bug 1: the timeout scan became stale while an earlier candidate propagated.
/// Rechecking inactivity in the locking query must preserve fresh progress and
/// its dependent task, while still timing out the genuinely inactive parent.
#[tokio::test]
async fn test_bug1_timeout_rechecks_activity_after_scan() {
    let (_guard, state) = setup_test_app().await;
    let app = test_service!(state);
    let created = create_tasks_ok(
        &app,
        &[
            task_json("parent", "parent", "audit"),
            task_with_deps("child", "child", "audit", vec![("parent", true)]),
            task_json("active", "active", "audit"),
            task_with_deps(
                "active-child",
                "active-child",
                "audit",
                vec![("active", true)],
            ),
        ],
    )
    .await;
    let id = |name: &str| created.iter().find(|t| t.name == name).unwrap().id;
    let parent = id("parent");
    let child = id("child");
    let active = id("active");
    {
        let mut conn = state.pool.get().await.unwrap();
        diesel::sql_query(
            "UPDATE task SET status='running', started_at=now()-interval '2 hours', \
             last_updated=now()-interval '2 hours' WHERE id=$1",
        )
        .bind::<diesel::sql_types::Uuid, _>(parent)
        .execute(&mut conn)
        .await
        .unwrap();
        diesel::sql_query(
            "UPDATE task SET status='running', started_at=now()-interval '1 hour', \
             last_updated=now()-interval '1 hour' WHERE id=$1",
        )
        .bind::<diesel::sql_types::Uuid, _>(active)
        .execute(&mut conn)
        .await
        .unwrap();
    }

    let (locked_tx, locked_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let pool = state.pool.clone();
    let blocker = tokio::spawn(async move {
        let mut conn = pool.get().await.unwrap();
        arcrun::db_operation::run_in_transaction(&mut conn, |conn| {
            Box::pin(async move {
                diesel::sql_query("SELECT id FROM task WHERE id=$1 FOR UPDATE")
                    .bind::<diesel::sql_types::Uuid, _>(child)
                    .execute(conn)
                    .await?;
                let pid: BackendPid = diesel::sql_query("SELECT pg_backend_pid() AS pid")
                    .get_result(conn)
                    .await?;
                locked_tx.send(pid.pid).unwrap();
                release_rx.await.unwrap();
                Ok::<(), arcrun::error::ArcRunError>(())
            })
        })
        .await
        .unwrap();
    });
    let pid = locked_rx.await.unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let pool = state.pool.clone();
    let worker = tokio::spawn(arcrun::workers::timeout_loop(
        pool,
        Duration::from_secs(60),
        Duration::from_secs(30),
        false,
        100,
        shutdown_rx,
        arcrun::workers::WorkerNudges::new(),
    ));

    // The worker scanned both candidates, then blocked propagating the first.
    wait_for_blocked(&state.pool, pid).await;
    let request = actix_web::test::TestRequest::patch()
        .uri(&format!("/task/{active}"))
        .set_json(json!({"new_success": 1}))
        .to_request();
    let response = actix_web::test::call_service(&app, request).await;
    assert!(response.status().is_success());
    let refreshed = get_task_ok(&app, active).await;

    shutdown_tx.send(true).unwrap();
    release_tx.send(()).unwrap();
    blocker.await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .unwrap()
        .unwrap();

    let actual = get_task_ok(&app, active).await;
    assert_eq!(actual.status, StatusKind::Running);
    assert_eq!(actual.success, 1);
    assert_eq!(actual.last_updated, refreshed.last_updated);
    assert_task_status(
        &app,
        id("active-child"),
        StatusKind::Waiting,
        "fresh parent must not cascade",
    )
    .await;
    assert_task_status(
        &app,
        parent,
        StatusKind::Failure,
        "inactive parent must time out",
    )
    .await;
    assert_task_status(
        &app,
        child,
        StatusKind::Failure,
        "inactive parent must still propagate",
    )
    .await;
}

/// Bug 2: a batch-rules PATCH after the scheduler scan bypassed slot admission.
/// Both rule-free entry points must reject changed candidates, keep the batch
/// fast path for unchanged candidates, and enforce the quota on the next scan.
#[tokio::test]
async fn test_bug2_rule_free_claim_rechecks_current_rules() {
    let (_guard, state) = setup_test_app().await;
    let app = test_service!(state);
    let created = create_tasks_ok(
        &app,
        &[
            task_json("a", "a", "audit"),
            task_json("b", "b", "audit"),
            task_json("free", "free", "other"),
        ],
    )
    .await;
    let ids: Vec<_> = created.iter().map(|t| t.id).collect();
    let mut conn = state.pool.get().await.unwrap();
    let stale: Task = Task::table().find(ids[0]).first(&mut conn).await.unwrap();
    let request = actix_web::test::TestRequest::patch()
        .uri(&format!("/batch/{}/rules", stale.batch_id.unwrap()))
        .set_json(json!({"kind": "audit", "rules": concurrency_rules(1)}))
        .to_request();
    let response = actix_web::test::call_service(&app, request).await;
    assert!(response.status().is_success());

    assert_eq!(
        claim_task_with_rules(&mut conn, &stale).await.unwrap(),
        ClaimResult::AlreadyClaimed,
        "a stale rule-free snapshot must not bypass newly added rules"
    );
    assert_eq!(
        batch_claim_tasks(&mut conn, &ids).await.unwrap(),
        vec![ids[2]]
    );

    let claimed = arcrun::workers::run_claim_loop(&mut conn, 10, 10).await;
    assert_eq!(
        claimed.len(),
        1,
        "fresh scan must enforce max_concurrency=1"
    );
    let used: Count = diesel::sql_query("SELECT SUM(used)::bigint AS value FROM rule_slot")
        .get_result(&mut conn)
        .await
        .unwrap();
    assert_eq!(used.value, 1, "only the admitted task reserves a slot");
}

/// Bug 2: claims computed from old non-empty rules reserved the wrong quota.
/// Rejecting the final UPDATE must roll back every slot, including newly inserted
/// capacity rows. Replacement and removal must both become claimable on a rescan.
#[tokio::test]
async fn test_bug2_changed_rules_roll_back_all_reservations() {
    let (_guard, state) = setup_test_app().await;
    let app = test_service!(state);
    let mut old_rules = concurrency_rules(2);
    old_rules.as_array_mut().unwrap().push(json!({
        "type": "Capacity", "max_capacity": 100,
        "matcher": {"kind": "audit", "status": "Running", "fields": []}
    }));

    for new_rules in [concurrency_rules(1), json!([])] {
        let mut payload = task_with_rules("candidate", "candidate", "audit", old_rules.clone());
        payload["expected_count"] = json!(20);
        let created = create_tasks_ok(&app, &[payload]).await;
        let mut conn = state.pool.get().await.unwrap();
        let stale: Task = Task::table()
            .find(created[0].id)
            .first(&mut conn)
            .await
            .unwrap();
        let request = actix_web::test::TestRequest::patch()
            .uri(&format!("/batch/{}/rules", stale.batch_id.unwrap()))
            .set_json(json!({"kind": "audit", "rules": new_rules}))
            .to_request();
        let response = actix_web::test::call_service(&app, request).await;
        assert!(response.status().is_success());

        assert_eq!(
            claim_task_with_rules(&mut conn, &stale).await.unwrap(),
            ClaimResult::AlreadyClaimed
        );
        let slots: Count = diesel::sql_query("SELECT COUNT(*) AS value FROM rule_slot")
            .get_result(&mut conn)
            .await
            .unwrap();
        assert_eq!(
            slots.value, 0,
            "stale admission must roll back both slot inserts"
        );
        let fresh: Task = Task::table().find(stale.id).first(&mut conn).await.unwrap();
        assert_eq!(fresh.status, StatusKind::Pending);
        assert!(fresh.claimed_slot_keys.is_none());
        assert!(fresh.capacity_charge.is_none());
        assert_eq!(
            claim_task_with_rules(&mut conn, &fresh).await.unwrap(),
            ClaimResult::Claimed
        );
        drop(conn);
        // Finish this task so the next case starts with no outstanding quota.
        let request = actix_web::test::TestRequest::patch()
            .uri(&format!("/task/{}", stale.id))
            .set_json(json!({"status": "Success"}))
            .to_request();
        let response = actix_web::test::call_service(&app, request).await;
        assert!(response.status().is_success());
        let mut conn = state.pool.get().await.unwrap();
        diesel::sql_query("DELETE FROM rule_slot")
            .execute(&mut conn)
            .await
            .unwrap();
    }
}

/// Bug 2: a rules edit can still be uncommitted when admission starts. The final
/// UPDATE must recheck its rules predicate after waiting for that edit's row lock,
/// and roll back the slot it reserved before waiting.
#[tokio::test]
async fn test_bug2_claim_rechecks_rules_after_lock_wait() {
    let (_guard, state) = setup_test_app().await;
    let app = test_service!(state);
    let created = create_tasks_ok(
        &app,
        &[task_with_rules(
            "candidate",
            "candidate",
            "audit",
            concurrency_rules(2),
        )],
    )
    .await;
    let task_id = created[0].id;
    let stale: Task = {
        let mut conn = state.pool.get().await.unwrap();
        Task::table().find(task_id).first(&mut conn).await.unwrap()
    };

    let (locked_tx, locked_rx) = tokio::sync::oneshot::channel();
    let (commit_tx, commit_rx) = tokio::sync::oneshot::channel();
    let pool = state.pool.clone();
    let editor = tokio::spawn(async move {
        let mut conn = pool.get().await.unwrap();
        arcrun::db_operation::run_in_transaction(&mut conn, |conn| {
            Box::pin(async move {
                diesel::sql_query(
                    "UPDATE task SET start_condition=$2, last_updated=now() \
                     WHERE id=$1 AND status='pending'",
                )
                .bind::<diesel::sql_types::Uuid, _>(task_id)
                .bind::<diesel::sql_types::Jsonb, _>(concurrency_rules(1))
                .execute(conn)
                .await?;
                let pid: BackendPid = diesel::sql_query("SELECT pg_backend_pid() AS pid")
                    .get_result(conn)
                    .await?;
                locked_tx.send(pid.pid).unwrap();
                commit_rx.await.unwrap();
                Ok::<(), arcrun::error::ArcRunError>(())
            })
        })
        .await
        .unwrap();
    });
    let pid = locked_rx.await.unwrap();
    let pool = state.pool.clone();
    let claim = tokio::spawn(async move {
        let mut conn = pool.get().await.unwrap();
        claim_task_with_rules(&mut conn, &stale).await.unwrap()
    });
    wait_for_blocked(&state.pool, pid).await;
    commit_tx.send(()).unwrap();
    editor.await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), claim)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result, ClaimResult::AlreadyClaimed);
    let mut conn = state.pool.get().await.unwrap();
    let slots: Count = diesel::sql_query("SELECT COUNT(*) AS value FROM rule_slot")
        .get_result(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        slots.value, 0,
        "the rejected claim must not retain a reservation"
    );
    let fresh: Task = Task::table().find(task_id).first(&mut conn).await.unwrap();
    assert_eq!(fresh.status, StatusKind::Pending);
    assert_eq!(
        claim_task_with_rules(&mut conn, &fresh).await.unwrap(),
        ClaimResult::Claimed
    );
}

/// Bug 6: the delivery worker retained a pooled connection throughout HTTP.
/// With a pool of size one and an endpoint blocked on a channel, an API read must
/// still complete before the endpoint responds; the outcome must then be saved.
#[tokio::test]
async fn test_bug6_delivery_returns_connection_during_http() {
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let database = setup_test_db_with_pool_size(1).await;
    let state = create_test_state(database.pool.clone());
    let app = test_service!(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/notify", listener.local_addr().unwrap());
    let (received_tx, received_rx) = tokio::sync::oneshot::channel();
    let (respond_tx, respond_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buffer = [0; 4096];
        assert!(socket.read(&mut buffer).await.unwrap() > 0);
        received_tx.send(()).unwrap();
        respond_rx.await.unwrap();
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
    });
    let mut payload = task_json("delivery", "delivery", "audit");
    payload["on_success"] = json!([{
        "kind": "Webhook", "params": {"url": url, "verb": "Post"}
    }]);
    let created = create_tasks_ok(&app, &[payload]).await;
    let task_id = created[0].id;
    succeed_task(&state, task_id).await;

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let worker = tokio::spawn(arcrun::workers::delivery_loop(
        Arc::new(state.action_executor.clone()),
        state.pool.clone(),
        Duration::from_secs(60),
        default_delivery_cfg(),
        shutdown_rx,
        arcrun::workers::WorkerNudges::new(),
    ));
    tokio::time::timeout(Duration::from_secs(5), received_rx)
        .await
        .unwrap()
        .unwrap();
    // Keep HTTP blocked while an API request borrows the sole connection.
    let read = tokio::time::timeout(Duration::from_secs(2), get_task_ok(&app, task_id)).await;
    shutdown_tx.send(true).unwrap();
    respond_tx.send(()).unwrap();
    server.await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        read.expect("delivery must release its connection during HTTP")
            .status,
        StatusKind::Success
    );
    let mut conn = state.pool.get().await.unwrap();
    let recorded: Count = diesel::sql_query(
        "SELECT COUNT(*) AS value FROM webhook_execution \
         WHERE task_id=$1 AND trigger='end' AND status='success'",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .get_result(&mut conn)
    .await
    .unwrap();
    assert_eq!(
        recorded.value, 1,
        "delivery result must survive connection reacquisition"
    );
}
