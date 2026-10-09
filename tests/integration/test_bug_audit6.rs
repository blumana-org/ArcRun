// Diesel's generated QueryableByName impls need this at module scope.
#![allow(clippy::redundant_field_names)]

//! Audit: task locks must precede reservation locks on every admission/exit path.

use crate::common::*;
use arcrun::{
    DbPool,
    db_operation::{self, ClaimResult},
    models::{StatusKind, Task},
};
use diesel_async::RunQueryDsl;
use serde_json::json;
use std::time::Duration;

#[derive(diesel::QueryableByName)]
struct Pid {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    pid: i32,
}

#[derive(diesel::QueryableByName)]
struct Blocked {
    #[diesel(sql_type = diesel::sql_types::Bool)]
    blocked: bool,
}

#[derive(diesel::QueryableByName)]
struct Used {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    used: i32,
}

#[derive(diesel::QueryableByName)]
struct SlotKeys {
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Array<diesel::sql_types::Text>>)]
    keys: Option<Vec<String>>,
}

fn snapshot(id: uuid::Uuid, rules: serde_json::Value) -> Task {
    Task {
        id,
        name: String::new(),
        kind: "ordered".into(),
        status: StatusKind::Pending,
        timeout: 60,
        created_at: chrono::Utc::now(),
        started_at: None,
        last_updated: chrono::Utc::now(),
        metadata: serde_json::Value::Null,
        ended_at: None,
        start_condition: serde_json::from_value(rules).unwrap(),
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

// Observe an actual wait edge, rather than assuming that a sleep let the other
// transaction reach its conflicting lock. The observer uses a third connection.
async fn wait_for_blocker(pool: &DbPool, blocker_pid: i32) {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut observer = pool.get().await.unwrap();
        loop {
            let blocked: Blocked = diesel::sql_query(
                "SELECT EXISTS (
                    SELECT 1 FROM pg_stat_activity
                    WHERE $1 = ANY(pg_blocking_pids(pid))
                 ) AS blocked",
            )
            .bind::<diesel::sql_types::Integer, _>(blocker_pid)
            .get_result(&mut observer)
            .await
            .unwrap();
            if blocked.blocked {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("competing transaction did not reach the task lock");
}

async fn used(pool: &DbPool, key: &str) -> i32 {
    diesel::sql_query("SELECT used FROM rule_slot WHERE lock_key = $1")
        .bind::<diesel::sql_types::Text, _>(key)
        .get_result::<Used>(&mut pool.get().await.unwrap())
        .await
        .unwrap()
        .used
}

fn complete() -> arcrun::dtos::UpdateTaskDto {
    arcrun::dtos::UpdateTaskDto {
        status: Some(StatusKind::Success),
        metadata: None,
        new_success: None,
        new_failures: None,
        failure_reason: None,
        expected_count: None,
        priority: None,
    }
}

/// Bug 3: a capacity flush held the slot while waiting on the task, whereas
/// completion held the task and waited on the slot. The same cycle affected the
/// single-task fallback. Wait until the real flush is blocked, then finish the
/// task: both transactions must commit, without changing terminal counters.
#[tokio::test]
async fn test_bug3_completion_and_capacity_flush_do_not_deadlock() {
    let (_guard, state) = setup_test_app().await;
    let app = test_service!(state);
    let created = create_tasks_ok(&app, &[task_json("capacity", "capacity", "audit")]).await;
    let id = created[0].id;
    let key = "cap:audit-lock-order";
    let mut setup = state.pool.get().await.unwrap();
    diesel::sql_query("INSERT INTO rule_slot(lock_key, used) VALUES ($1, 100)")
        .bind::<diesel::sql_types::Text, _>(key)
        .execute(&mut setup)
        .await
        .unwrap();
    diesel::sql_query(
        "UPDATE task SET status='running', started_at=now(), expected_count=100,
         capacity_charge=100, claimed_slot_keys=ARRAY[$2] WHERE id=$1",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .bind::<diesel::sql_types::Text, _>(key)
    .execute(&mut setup)
    .await
    .unwrap();
    drop(setup);

    let (locked_tx, locked_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
    let pool = state.pool.clone();
    let completion = tokio::spawn(async move {
        let mut conn = pool.get().await.unwrap();
        db_operation::run_in_transaction(&mut conn, |conn| {
            Box::pin(async move {
                diesel::sql_query("SELECT id FROM task WHERE id=$1 FOR UPDATE")
                    .bind::<diesel::sql_types::Uuid, _>(id)
                    .execute(conn)
                    .await?;
                let pid: Pid = diesel::sql_query("SELECT pg_backend_pid() AS pid")
                    .get_result(conn)
                    .await?;
                locked_tx.send(pid.pid).unwrap();
                resume_rx.await.unwrap();
                db_operation::update_running_task(conn, id, complete(), false).await
            })
        })
        .await
    });
    let pid = locked_rx.await.unwrap();
    let pool = state.pool.clone();
    let flush = tokio::spawn(async move {
        arcrun::workers::run_counter_flush_once(&mut pool.get().await.unwrap(), &[(id, 1, 0)]).await
    });
    wait_for_blocker(&state.pool, pid).await;
    resume_tx.send(()).unwrap();
    let (completion, flush) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(completion, flush)
    })
    .await
    .unwrap();
    completion.unwrap().expect("completion must not deadlock");
    flush.unwrap().expect("flush must not deadlock");
    let task = get_task_ok(&app, id).await;
    assert_eq!(task.status, StatusKind::Success);
    assert_eq!(task.success, 0, "terminal progress stays frozen");
    assert_eq!(used(&state.pool, key).await, 0);
}

/// Bug 3: a claim could hold the parent's shared slot while waiting on a child
/// row locked by propagation. Releasing the parent then deadlocked. Even when
/// the claim's snapshot is Pending, it must lock the task before any slot.
#[tokio::test]
async fn test_bug3_claim_waiting_on_task_does_not_block_slot_release() {
    let (_guard, state) = setup_test_app().await;
    let app = test_service!(state);
    let rule = json!({
        "type": "Concurency", "max_concurency": 2,
        "matcher": {"kind": "ordered", "status": "Running", "fields": []}
    });
    let mut parent = task_json("parent", "parent", "ordered");
    parent["rules"] = json!([rule.clone()]);
    let mut child = task_json("child", "child", "ordered");
    child["rules"] = json!([rule.clone()]);
    let created = create_tasks_ok(&app, &[parent, child]).await;
    let parent_id = created[0].id;
    let child_id = created[1].id;
    let mut setup = state.pool.get().await.unwrap();
    let parent = snapshot(parent_id, json!([rule.clone()]));
    let child = snapshot(child_id, json!([rule]));
    assert_eq!(
        db_operation::claim_task_with_rules(&mut setup, &parent)
            .await
            .unwrap(),
        ClaimResult::Claimed
    );
    drop(setup);

    let (locked_tx, locked_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
    let pool = state.pool.clone();
    let completion = tokio::spawn(async move {
        let mut conn = pool.get().await.unwrap();
        db_operation::run_in_transaction(&mut conn, |conn| {
            Box::pin(async move {
                diesel::sql_query("SELECT id FROM task WHERE id=$1 FOR UPDATE")
                    .bind::<diesel::sql_types::Uuid, _>(child_id)
                    .execute(conn)
                    .await?;
                let pid: Pid = diesel::sql_query("SELECT pg_backend_pid() AS pid")
                    .get_result(conn)
                    .await?;
                locked_tx.send(pid.pid).unwrap();
                resume_rx.await.unwrap();
                db_operation::update_running_task(conn, parent_id, complete(), false).await
            })
        })
        .await
    });
    let pid = locked_rx.await.unwrap();
    let pool = state.pool.clone();
    let claim = tokio::spawn(async move {
        db_operation::claim_task_with_rules(&mut pool.get().await.unwrap(), &child).await
    });
    wait_for_blocker(&state.pool, pid).await;
    resume_tx.send(()).unwrap();
    let (completion, claim) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(completion, claim)
    })
    .await
    .unwrap();
    completion
        .unwrap()
        .expect("parent release must not deadlock");
    assert_eq!(claim.unwrap().unwrap(), ClaimResult::Claimed);
    let child: SlotKeys =
        diesel::sql_query("SELECT claimed_slot_keys AS keys FROM task WHERE id=$1")
            .bind::<diesel::sql_types::Uuid, _>(child_id)
            .get_result(&mut state.pool.get().await.unwrap())
            .await
            .unwrap();
    let keys = child.keys.unwrap();
    assert_eq!(keys.len(), 1);
    assert_eq!(used(&state.pool, &keys[0]).await, 1);
}

/// Bug 3: stopping a batch locked Waiting rows before Running rows, opposite to
/// an id-ordered progress flush. Hold the lower-id Running task, let stop reach
/// it, then flush both tasks. A sorted stop must not already own the Waiting row.
#[tokio::test]
async fn test_bug3_stop_batch_and_counter_flush_share_task_order() {
    let (_guard, state) = setup_test_app().await;
    let app = test_service!(state);
    let created = create_tasks_ok(
        &app,
        &[
            task_json("first", "first", "ordered"),
            task_json("second", "second", "ordered"),
        ],
    )
    .await;
    let mut ids = [created[0].id, created[1].id];
    ids.sort();
    let bid = created[0].batch_id.unwrap();
    let mut setup = state.pool.get().await.unwrap();
    diesel::sql_query("UPDATE task SET status='running', started_at=now() WHERE id=$1")
        .bind::<diesel::sql_types::Uuid, _>(ids[0])
        .execute(&mut setup)
        .await
        .unwrap();
    diesel::sql_query("UPDATE task SET status='waiting' WHERE id=$1")
        .bind::<diesel::sql_types::Uuid, _>(ids[1])
        .execute(&mut setup)
        .await
        .unwrap();
    drop(setup);

    let (locked_tx, locked_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
    let pool = state.pool.clone();
    let flush = tokio::spawn(async move {
        let mut conn = pool.get().await.unwrap();
        db_operation::run_in_transaction(&mut conn, |conn| {
            Box::pin(async move {
                diesel::sql_query("SELECT id FROM task WHERE id=$1 FOR UPDATE")
                    .bind::<diesel::sql_types::Uuid, _>(ids[0])
                    .execute(conn)
                    .await?;
                let pid: Pid = diesel::sql_query("SELECT pg_backend_pid() AS pid")
                    .get_result(conn)
                    .await?;
                locked_tx.send(pid.pid).unwrap();
                resume_rx.await.unwrap();
                arcrun::workers::run_counter_flush_once(conn, &[(ids[0], 1, 0), (ids[1], 1, 0)])
                    .await
            })
        })
        .await
    });
    let pid = locked_rx.await.unwrap();
    let req = actix_web::test::TestRequest::delete()
        .uri(&format!("/batch/{bid}"))
        .to_request();
    let (response, flush) = tokio::time::timeout(Duration::from_secs(8), async {
        tokio::join!(actix_web::test::call_service(&app, req), async {
            wait_for_blocker(&state.pool, pid).await;
            resume_tx.send(()).unwrap();
            flush.await.unwrap()
        })
    })
    .await
    .unwrap();
    assert!(response.status().is_success(), "stop must not deadlock");
    flush.expect("flush must not deadlock");
    for id in ids {
        let task = get_task_ok(&app, id).await;
        assert_eq!(task.status, StatusKind::Canceled);
        assert_eq!(task.success, 1);
    }
}

/// Bug 3: GC used a planner-ordered DELETE that could wait on locks held by
/// admission/release, then form a multi-slot cycle. GC must skip a locked empty
/// slot, remove other empty slots, and collect the skipped slot on a later pass.
#[tokio::test]
async fn test_bug3_slot_gc_skips_busy_rows() {
    let db = setup_test_db().await;
    let mut setup = db.pool.get().await.unwrap();
    diesel::sql_query(
        "INSERT INTO rule_slot(lock_key,used) VALUES
         ('cap:a-busy',0), ('cap:b-empty',0), ('cap:c-active',10)",
    )
    .execute(&mut setup)
    .await
    .unwrap();
    drop(setup);
    let (locked_tx, locked_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
    let pool = db.pool.clone();
    let owner = tokio::spawn(async move {
        let mut conn = pool.get().await.unwrap();
        db_operation::run_in_transaction(&mut conn, |conn| {
            Box::pin(async move {
                diesel::sql_query(
                    "SELECT lock_key FROM rule_slot WHERE lock_key='cap:a-busy' FOR UPDATE",
                )
                .execute(conn)
                .await?;
                locked_tx.send(()).unwrap();
                resume_rx.await.unwrap();
                Ok(())
            })
        })
        .await
    });
    locked_rx.await.unwrap();
    let pool = db.pool.clone();
    let mut gc = tokio::spawn(async move {
        db_operation::gc_empty_rule_slots(&mut pool.get().await.unwrap()).await
    });
    let result = tokio::time::timeout(Duration::from_secs(5), &mut gc).await;
    // Release even on failure, so a failed regression does not strand a backend.
    resume_tx.send(()).unwrap();
    owner.await.unwrap().unwrap();
    let removed = match result {
        Ok(result) => result.unwrap().unwrap(),
        Err(_) => {
            gc.await.unwrap().unwrap();
            panic!("GC must skip the busy row without waiting for its owner");
        }
    };
    assert_eq!(removed, 1);
    assert_eq!(used(&db.pool, "cap:a-busy").await, 0);
    assert_eq!(used(&db.pool, "cap:c-active").await, 10);
    assert_eq!(
        db_operation::gc_empty_rule_slots(&mut db.pool.get().await.unwrap())
            .await
            .unwrap(),
        1
    );
    assert_eq!(used(&db.pool, "cap:c-active").await, 10);
}

/// Bug 3 claim rewrite: a later saturated rule must roll back the first slot
/// even though its upsert is now combined with the task lock. No reservation or
/// claim state may survive, and the same task must remain claimable afterwards.
#[tokio::test]
async fn test_bug3_later_rule_rejection_rolls_back_combined_first_slot() {
    let (_guard, state) = setup_test_app().await;
    let app = test_service!(state);
    let conc = arcrun::rule::ConcurencyRule {
        max_concurency: 1,
        matcher: arcrun::rule::Matcher {
            kind: "ordered".into(),
            status: StatusKind::Running,
            fields: vec![],
        },
    };
    let cap = arcrun::rule::CapacityRule {
        max_capacity: 100,
        matcher: conc.matcher.clone(),
    };
    let rules = arcrun::rule::Rules(vec![
        arcrun::rule::Strategy::Concurency(conc.clone()),
        arcrun::rule::Strategy::Capacity(cap.clone()),
    ]);
    let mut payload = task_json("candidate", "candidate", "ordered");
    payload["expected_count"] = json!(50);
    payload["rules"] = serde_json::to_value(&rules).unwrap();
    let id = create_tasks_ok(&app, &[payload]).await[0].id;
    let mut candidate = snapshot(id, serde_json::to_value(&rules).unwrap());
    candidate.expected_count = Some(50);
    let conc_key = arcrun::rule::concurrency_slot_key(&conc, &candidate.metadata).unwrap();
    let cap_key = arcrun::rule::capacity_slot_key(&cap, &candidate.metadata).unwrap();
    let mut conn = state.pool.get().await.unwrap();
    diesel::sql_query("INSERT INTO rule_slot(lock_key, used) VALUES ($1, 1)")
        .bind::<diesel::sql_types::Text, _>(&conc_key)
        .execute(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        db_operation::claim_task_with_rules(&mut conn, &candidate)
            .await
            .unwrap(),
        ClaimResult::RuleBlocked
    );
    let keys: SlotKeys =
        diesel::sql_query("SELECT claimed_slot_keys AS keys FROM task WHERE id=$1")
            .bind::<diesel::sql_types::Uuid, _>(id)
            .get_result(&mut conn)
            .await
            .unwrap();
    assert!(keys.keys.is_none());
    let cap_rows = diesel::sql_query("SELECT used FROM rule_slot WHERE lock_key=$1")
        .bind::<diesel::sql_types::Text, _>(&cap_key)
        .get_results::<Used>(&mut conn)
        .await
        .unwrap();
    assert!(cap_rows.is_empty(), "first slot INSERT must roll back");
    assert_eq!(get_task_ok(&app, id).await.status, StatusKind::Pending);
    diesel::sql_query("UPDATE rule_slot SET used=0 WHERE lock_key=$1")
        .bind::<diesel::sql_types::Text, _>(&conc_key)
        .execute(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        db_operation::claim_task_with_rules(&mut conn, &candidate)
            .await
            .unwrap(),
        ClaimResult::Claimed
    );
    drop(conn);
    assert_eq!(used(&state.pool, &conc_key).await, 1);
    assert_eq!(used(&state.pool, &cap_key).await, 50);
}

/// Bug 3: SQL's default collation can disagree with the claim's Rust BTreeMap
/// order. Under ICU, "á" sorts before "b"; Rust orders their UTF-8 bytes the other
/// way. Both release and flush must use C collation, even on a localized column.
#[tokio::test]
async fn test_bug3_slot_order_matches_rust_with_non_c_collation() {
    for terminal in [true, false] {
        let (_guard, state) = setup_test_app().await;
        let app = test_service!(state);
        let id = create_tasks_ok(&app, &[task_json("locale", "locale", "ordered")]).await[0].id;
        let mut setup = state.pool.get().await.unwrap();
        diesel::sql_query("CREATE COLLATION audit_natural (provider=icu, locale='und')")
            .execute(&mut setup)
            .await
            .unwrap();
        diesel::sql_query(
            "ALTER TABLE rule_slot ALTER COLUMN lock_key TYPE text COLLATE audit_natural",
        )
        .execute(&mut setup)
        .await
        .unwrap();
        diesel::sql_query("INSERT INTO rule_slot(lock_key,used) VALUES ('cap:b',2), ('cap:á',2)")
            .execute(&mut setup)
            .await
            .unwrap();
        let natural: SlotKeys = diesel::sql_query(
            "SELECT array_agg(lock_key ORDER BY lock_key) AS keys FROM rule_slot",
        )
        .get_result(&mut setup)
        .await
        .unwrap();
        assert_eq!(natural.keys.unwrap(), ["cap:á", "cap:b"]);
        diesel::sql_query(
            "UPDATE task SET status='running', started_at=now(), expected_count=2,
             capacity_charge=2, claimed_slot_keys=ARRAY['cap:b','cap:á'] WHERE id=$1",
        )
        .bind::<diesel::sql_types::Uuid, _>(id)
        .execute(&mut setup)
        .await
        .unwrap();
        drop(setup);

        let (locked_tx, locked_rx) = tokio::sync::oneshot::channel();
        let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
        let pool = state.pool.clone();
        let ordered_owner = tokio::spawn(async move {
            let mut conn = pool.get().await.unwrap();
            db_operation::run_in_transaction(&mut conn, |conn| {
                Box::pin(async move {
                    // This is the byte order in which the claim acquires its slots.
                    diesel::sql_query(
                        "SELECT lock_key FROM rule_slot WHERE lock_key='cap:b' FOR UPDATE",
                    )
                    .execute(conn)
                    .await?;
                    let pid: Pid = diesel::sql_query("SELECT pg_backend_pid() AS pid")
                        .get_result(conn)
                        .await?;
                    locked_tx.send(pid.pid).unwrap();
                    resume_rx.await.unwrap();
                    diesel::sql_query(
                        "SELECT lock_key FROM rule_slot WHERE lock_key='cap:á' FOR UPDATE",
                    )
                    .execute(conn)
                    .await?;
                    Ok(())
                })
            })
            .await
        });
        let pid = locked_rx.await.unwrap();
        let pool = state.pool.clone();
        let competing = tokio::spawn(async move {
            let mut conn = pool.get().await.unwrap();
            if terminal {
                db_operation::update_running_task(&mut conn, id, complete(), false)
                    .await
                    .map(|_| ())
            } else {
                arcrun::workers::run_counter_flush_once(&mut conn, &[(id, 1, 0)]).await
            }
        });
        wait_for_blocker(&state.pool, pid).await;
        resume_tx.send(()).unwrap();
        let (owner, competing) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(ordered_owner, competing)
        })
        .await
        .unwrap();
        owner
            .unwrap()
            .expect("Rust slot ordering must not deadlock");
        competing
            .unwrap()
            .expect("SQL must use the same slot ordering");
        for key in ["cap:b", "cap:á"] {
            assert_eq!(used(&state.pool, key).await, if terminal { 0 } else { 1 });
        }
    }
}
