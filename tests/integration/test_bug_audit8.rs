#![allow(clippy::redundant_field_names)]

//! Audit 3: acquire every blocking task lock before propagating across DAG levels.

use crate::common::*;
use arcrun::{DbPool, db_operation, models::StatusKind};
use diesel_async::RunQueryDsl;
use serde_json::json;
use std::time::Duration;
use uuid::Uuid;

#[derive(diesel::QueryableByName)]
struct Pid {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    pid: i32,
}

#[derive(diesel::QueryableByName)]
struct Flag {
    #[diesel(sql_type = diesel::sql_types::Bool)]
    value: bool,
}

#[derive(diesel::QueryableByName)]
struct Count {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    n: i64,
}

async fn wait_edge(pool: &DbPool, waiter: i32, blocker: i32) {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut observer = pool.get().await.unwrap();
        loop {
            let blocked: Flag = diesel::sql_query("SELECT $2 = ANY(pg_blocking_pids($1)) AS value")
                .bind::<diesel::sql_types::Integer, _>(waiter)
                .bind::<diesel::sql_types::Integer, _>(blocker)
                .get_result(&mut observer)
                .await
                .unwrap();
            if blocked.value {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("expected wait edge did not appear");
}

async fn pid(conn: &mut arcrun::Conn<'_>) -> i32 {
    diesel::sql_query("SELECT pg_backend_pid() AS pid")
        .get_result::<Pid>(conn)
        .await
        .unwrap()
        .pid
}

async fn graph() -> (TestApp, arcrun::handlers::AppState, Vec<Uuid>) {
    let (guard, state) = setup_test_app().await;
    let app = test_service!(state);
    // Creation order is topological AND UUID order. Z is a real extra child
    // used as a SQL lock barrier, without a production hook or a FIFO assumption.
    // A -> P -> X; A -> Y,Z; B -> X,Y.
    let mut tasks = vec![
        task_json("a", "a", "audit8"),
        task_json("b", "b", "audit8"),
        task_with_deps("p", "p", "audit8", vec![("a", true)]),
        task_with_deps("x", "x", "audit8", vec![("p", true), ("b", true)]),
        task_with_deps("y", "y", "audit8", vec![("a", true), ("b", true)]),
        task_with_deps("z", "z", "audit8", vec![("a", true)]),
    ];
    for task in &mut tasks {
        task["on_failure"] = json!([webhook_action()]);
        task["on_success"] = json!([webhook_action()]);
    }
    let created = create_tasks_ok(&app, &tasks).await;
    let ids: Vec<_> = created.into_iter().map(|t| t.id).collect();
    assert!(ids.windows(2).all(|w| w[0] < w[1]));
    let mut conn = state.pool.get().await.unwrap();
    diesel::sql_query("UPDATE task SET status='running', started_at=now() WHERE id=ANY($1)")
        .bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(&ids[..2])
        .execute(&mut conn)
        .await
        .unwrap();
    diesel::sql_query(
        "INSERT INTO batch(id, on_complete, remaining)
         SELECT batch_id, '[]'::jsonb, 6 FROM task WHERE id=$1",
    )
    .bind::<diesel::sql_types::Uuid, _>(ids[0])
    .execute(&mut conn)
    .await
    .unwrap();
    drop(conn);
    (guard, state, ids)
}

async fn cross_frontiers(other: &'static str) {
    let (_guard, state, ids) = graph().await;
    let mut holder = state.pool.get().await.unwrap();
    diesel::sql_query("BEGIN")
        .execute(&mut holder)
        .await
        .unwrap();
    diesel::sql_query("SELECT id FROM task WHERE id=$1 FOR UPDATE")
        .bind::<diesel::sql_types::Uuid, _>(ids[5])
        .execute(&mut holder)
        .await
        .unwrap();
    let holder_pid = pid(&mut holder).await;

    let (sent, received) = tokio::sync::oneshot::channel();
    let pool = state.pool.clone();
    let a = ids[0];
    let first = tokio::spawn(async move {
        let mut conn = pool.get().await.unwrap();
        sent.send(pid(&mut conn).await).unwrap();
        db_operation::update_running_task(&mut conn, a, failure_dto("cascade"), false).await
    });
    let first_pid = received.await.unwrap();
    wait_edge(&state.pool, first_pid, holder_pid).await;

    let (sent, received) = tokio::sync::oneshot::channel();
    let pool = state.pool.clone();
    let b = ids[1];
    let x = ids[3];
    let y = ids[4];
    let second = tokio::spawn(async move {
        let mut conn = pool.get().await.unwrap();
        sent.send(pid(&mut conn).await).unwrap();
        if other == "flush" {
            arcrun::workers::run_counter_flush_once(&mut conn, &[(x, 1, 0), (y, 1, 0)]).await?;
            Ok(db_operation::UpdateTaskResult::Updated)
        } else {
            let dto = if other == "success" {
                success_dto()
            } else {
                failure_dto("other root")
            };
            db_operation::update_running_task(&mut conn, b, dto, false).await
        }
    });
    let second_pid = received.await.unwrap();
    wait_edge(&state.pool, second_pid, first_pid).await;
    diesel::sql_query("ROLLBACK")
        .execute(&mut holder)
        .await
        .unwrap();
    drop(holder);

    let (first, second) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(first, second)
    })
    .await
    .unwrap();
    assert!(matches!(
        first.unwrap().expect("cascade deadlocked"),
        db_operation::UpdateTaskResult::Updated
    ));
    assert!(matches!(
        second.unwrap().expect("other transition deadlocked"),
        db_operation::UpdateTaskResult::Updated
    ));
    let mut conn = state.pool.get().await.unwrap();
    let expected_failures = if other == "failure" { 6 } else { 5 };
    let failures: Count =
        diesel::sql_query("SELECT count(*) AS n FROM task WHERE status='failure'")
            .get_result(&mut conn)
            .await
            .unwrap();
    assert_eq!(failures.n, expected_failures);
    let events: Count =
        diesel::sql_query("SELECT count(*) AS n FROM webhook_outbox WHERE trigger='end'")
            .get_result(&mut conn)
            .await
            .unwrap();
    assert_eq!(
        events.n,
        if other == "flush" { 5 } else { 6 },
        "one terminal event per task"
    );
    let remaining: Count = diesel::sql_query("SELECT remaining::bigint AS n FROM batch")
        .get_result(&mut conn)
        .await
        .unwrap();
    assert_eq!(remaining.n, i64::from(other == "flush"));
}

/// Bug 3: independently sorted frontiers acquired Y then X versus X then Y.
/// Both complete transitions must commit, including counters and unique outbox rows.
#[tokio::test]
async fn test_bug3_failure_frontiers_acquire_one_global_task_order() {
    cross_frontiers("failure").await;
}

/// Bug 3: even a success propagation can cross a multi-level failure cascade.
#[tokio::test]
async fn test_bug3_success_and_failure_share_one_global_task_order() {
    cross_frontiers("success").await;
}

/// Bug 3: a counter flush can hold X while waiting on a higher frontier row Y.
#[tokio::test]
async fn test_bug3_counter_flush_and_cascade_share_one_global_task_order() {
    cross_frontiers("flush").await;
}

/// Bug 3: a plain multi-row UPDATE can visit the heap in the reverse of the
/// flush's UUID order. Force that plan and gate its middle row, then observe the
/// flush waiting on the claim before releasing the gate. No scheduling sleeps.
#[tokio::test]
async fn test_bug3_rule_free_claim_orders_locks_against_counter_flush() {
    let (_guard, state) = setup_test_app().await;
    let app = test_service!(state);
    let tasks = create_tasks_ok(
        &app,
        &[
            task_json("low", "low", "audit8"),
            task_json("gate", "gate", "audit8"),
            task_json("high", "high", "audit8"),
        ],
    )
    .await;
    let ids: Vec<_> = tasks.into_iter().map(|t| t.id).collect();
    let mut holder = state.pool.get().await.unwrap();
    diesel::sql_query("CREATE INDEX audit8_reverse ON task(id DESC)")
        .execute(&mut holder)
        .await
        .unwrap();
    diesel::sql_query("CLUSTER task USING audit8_reverse")
        .execute(&mut holder)
        .await
        .unwrap();
    diesel::sql_query("BEGIN")
        .execute(&mut holder)
        .await
        .unwrap();
    diesel::sql_query("SELECT id FROM task WHERE id=$1 FOR UPDATE")
        .bind::<diesel::sql_types::Uuid, _>(ids[1])
        .execute(&mut holder)
        .await
        .unwrap();
    let holder_pid = pid(&mut holder).await;
    let pool = state.pool.clone();
    let claim_ids = ids.clone();
    let (sent, received) = tokio::sync::oneshot::channel();
    let claim = tokio::spawn(async move {
        let mut conn = pool.get().await.unwrap();
        diesel::sql_query("SET enable_indexscan=off")
            .execute(&mut conn)
            .await
            .unwrap();
        diesel::sql_query("SET enable_bitmapscan=off")
            .execute(&mut conn)
            .await
            .unwrap();
        sent.send(pid(&mut conn).await).unwrap();
        db_operation::batch_claim_tasks(&mut conn, &claim_ids).await
    });
    let claim_pid = received.await.unwrap();
    wait_edge(&state.pool, claim_pid, holder_pid).await;
    let pool = state.pool.clone();
    let low = ids[0];
    let high = ids[2];
    let (sent, received) = tokio::sync::oneshot::channel();
    let flush = tokio::spawn(async move {
        let mut conn = pool.get().await.unwrap();
        sent.send(pid(&mut conn).await).unwrap();
        arcrun::workers::run_counter_flush_once(&mut conn, &[(low, 1, 0), (high, 1, 0)]).await
    });
    wait_edge(&state.pool, received.await.unwrap(), claim_pid).await;
    diesel::sql_query("ROLLBACK")
        .execute(&mut holder)
        .await
        .unwrap();
    drop(holder);
    let (claim, flush) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(claim, flush)
    })
    .await
    .unwrap();
    assert_eq!(claim.unwrap().expect("claim deadlocked").len(), 3);
    flush.unwrap().expect("flush deadlocked");
    for id in [low, high] {
        let task = get_task_ok(&app, id).await;
        assert_eq!(task.status, StatusKind::Claimed);
        assert_eq!(task.success, 1);
    }
}

/// A busy deep child must defer the ENTIRE timeout, never silently omit the child.
/// A later progress update invalidates the candidate; a later genuine timeout
/// must commit its complete propagation and batch decrement.
#[tokio::test]
async fn test_bug3_timeout_requires_all_locks_and_fresh_inactivity() {
    let (_guard, state, ids) = graph().await;
    let mut conn = state.pool.get().await.unwrap();
    diesel::sql_query("UPDATE task SET last_updated=now()-interval '2 minutes' WHERE id=$1")
        .bind::<diesel::sql_types::Uuid, _>(ids[0])
        .execute(&mut conn)
        .await
        .unwrap();
    let mut holder = state.pool.get().await.unwrap();
    diesel::sql_query("BEGIN")
        .execute(&mut holder)
        .await
        .unwrap();
    diesel::sql_query("SELECT id FROM task WHERE id=$1 FOR UPDATE")
        .bind::<diesel::sql_types::Uuid, _>(ids[3])
        .execute(&mut holder)
        .await
        .unwrap();
    let applied = tokio::time::timeout(
        Duration::from_secs(1),
        db_operation::timeout_task_and_propagate(&mut conn, ids[0], false),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(!applied);
    let changed: Count = diesel::sql_query("SELECT count(*) AS n FROM task WHERE status='failure'")
        .get_result(&mut conn)
        .await
        .unwrap();
    assert_eq!(changed.n, 0);
    let events: Count = diesel::sql_query("SELECT count(*) AS n FROM webhook_outbox")
        .get_result(&mut conn)
        .await
        .unwrap();
    assert_eq!(events.n, 0);
    diesel::sql_query("ROLLBACK")
        .execute(&mut holder)
        .await
        .unwrap();
    // The aborted read-only attempt must have released its root lock too.
    diesel::sql_query("UPDATE task SET last_updated=now() WHERE id=$1")
        .bind::<diesel::sql_types::Uuid, _>(ids[0])
        .execute(&mut holder)
        .await
        .unwrap();
    assert!(
        !db_operation::timeout_task_and_propagate(&mut conn, ids[0], false)
            .await
            .unwrap()
    );
    diesel::sql_query("UPDATE task SET last_updated=now()-interval '2 minutes' WHERE id=$1")
        .bind::<diesel::sql_types::Uuid, _>(ids[0])
        .execute(&mut holder)
        .await
        .unwrap();
    assert!(
        db_operation::timeout_task_and_propagate(&mut conn, ids[0], false)
            .await
            .unwrap()
    );
    let failures: Count =
        diesel::sql_query("SELECT count(*) AS n FROM task WHERE status='failure'")
            .get_result(&mut conn)
            .await
            .unwrap();
    assert_eq!(failures.n, 5);
    let remaining: Count = diesel::sql_query("SELECT remaining::bigint AS n FROM batch")
        .get_result(&mut conn)
        .await
        .unwrap();
    assert_eq!(remaining.n, 1);
}

/// An idempotent terminal PATCH must not discover/lock an already-finished DAG.
#[tokio::test]
async fn test_bug3_idempotent_terminal_patch_does_not_lock_descendants() {
    let (_guard, state, ids) = graph().await;
    let mut conn = state.pool.get().await.unwrap();
    db_operation::update_running_task(&mut conn, ids[0], failure_dto("first"), false)
        .await
        .unwrap();
    let mut holder = state.pool.get().await.unwrap();
    diesel::sql_query("BEGIN")
        .execute(&mut holder)
        .await
        .unwrap();
    diesel::sql_query("SELECT id FROM task WHERE id=ANY($1) FOR UPDATE")
        .bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(&ids[2..])
        .execute(&mut holder)
        .await
        .unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        db_operation::update_running_task(&mut conn, ids[0], failure_dto("repeat"), false),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(
        result,
        db_operation::UpdateTaskResult::AlreadyInRequestedState
    ));
    diesel::sql_query("ROLLBACK")
        .execute(&mut holder)
        .await
        .unwrap();
    let remaining: Count = diesel::sql_query("SELECT remaining::bigint AS n FROM batch")
        .get_result(&mut conn)
        .await
        .unwrap();
    assert_eq!(remaining.n, 1);
}

/// Retention must skip busy terminal rows before touching their dependent records.
/// Only the actually locked IDs may be archived; the rest are retried next pass.
#[tokio::test]
async fn test_bug3_retention_skips_locked_tasks_and_their_actions() {
    let (_guard, state) = setup_test_app().await;
    let app = test_service!(state);
    let tasks = create_tasks_ok(
        &app,
        &[
            task_json("busy", "busy", "audit8"),
            task_json("free", "free", "audit8"),
        ],
    )
    .await;
    let mut conn = state.pool.get().await.unwrap();
    diesel::sql_query("UPDATE task SET status='success', ended_at=now()-interval '60 days'")
        .execute(&mut conn)
        .await
        .unwrap();
    let mut holder = state.pool.get().await.unwrap();
    diesel::sql_query("BEGIN")
        .execute(&mut holder)
        .await
        .unwrap();
    diesel::sql_query("SELECT id FROM task WHERE id=$1 FOR UPDATE")
        .bind::<diesel::sql_types::Uuid, _>(tasks[0].id)
        .execute(&mut holder)
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(1),
            db_operation::cleanup_old_terminal_tasks(&mut conn, 30, 10)
        )
        .await
        .unwrap()
        .unwrap(),
        1
    );
    let actions: Count = diesel::sql_query("SELECT count(*) AS n FROM action WHERE task_id=$1")
        .bind::<diesel::sql_types::Uuid, _>(tasks[0].id)
        .get_result(&mut conn)
        .await
        .unwrap();
    assert_eq!(actions.n, 1);
    diesel::sql_query("ROLLBACK")
        .execute(&mut holder)
        .await
        .unwrap();
    assert_eq!(
        db_operation::cleanup_old_terminal_tasks(&mut conn, 30, 10)
            .await
            .unwrap(),
        1
    );
}

/// Fifty concurrent roots share ten levels of diamonds. No retry is installed:
/// every original transition must commit and every descendant is counted once.
#[tokio::test]
async fn test_bug3_many_concurrent_roots_preserve_terminal_counts() {
    let db = setup_test_db_with_pool_size(60).await;
    let ids: Vec<_> = (1..=2_050).map(Uuid::from_u128).collect();
    let batch = Uuid::new_v4();
    let mut conn = db.pool.get().await.unwrap();
    diesel::sql_query("INSERT INTO batch(id,on_complete,remaining) VALUES ($1,'[]',2050)")
        .bind::<diesel::sql_types::Uuid, _>(batch)
        .execute(&mut conn)
        .await
        .unwrap();
    diesel::sql_query(
        "INSERT INTO task(id,name,kind,status,timeout,created_at,started_at,last_updated,
         metadata,start_condition,wait_success,wait_finished,success,failures,dead_end_barrier,priority,batch_id)
         SELECT id,'stress','audit8','running',60,now(),now(),now(),
         '{}'::jsonb,'[]'::jsonb,0,0,0,0,false,0,$2 FROM unnest($1::uuid[]) AS id",
    ).bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(&ids)
        .bind::<diesel::sql_types::Uuid, _>(batch).execute(&mut conn).await.unwrap();
    let mut parents = Vec::new();
    let mut children = Vec::new();
    for i in 50..ids.len() {
        let (start, width, offset) = if i < 250 {
            (0, 50, (i - 50) % 50)
        } else {
            (50 + ((i - 50) / 200 - 1) * 200, 200, (i - 50) % 200)
        };
        for shift in 0..2 {
            parents.push(ids[start + (offset + shift) % width]);
            children.push(ids[i]);
        }
    }
    diesel::sql_query("INSERT INTO link(parent_id,child_id,requires_success) SELECT p,c,true FROM unnest($1::uuid[],$2::uuid[]) AS x(p,c)")
        .bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(parents)
        .bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(children)
        .execute(&mut conn).await.unwrap();
    for _ in 0..3 {
        diesel::sql_query("UPDATE task SET status=CASE WHEN id=ANY($1) THEN 'running'::status_kind ELSE 'waiting'::status_kind END, wait_finished=CASE WHEN id=ANY($1) THEN 0 ELSE 2 END,wait_success=CASE WHEN id=ANY($1) THEN 0 ELSE 2 END,ended_at=NULL,failure_reason=NULL")
            .bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(&ids[..50])
            .execute(&mut conn).await.unwrap();
        diesel::sql_query("UPDATE batch SET remaining=2050")
            .execute(&mut conn)
            .await
            .unwrap();
        let gate = std::sync::Arc::new(tokio::sync::Barrier::new(50));
        let mut requests = tokio::task::JoinSet::new();
        for id in ids.iter().take(50).copied() {
            let pool = db.pool.clone();
            let gate = gate.clone();
            requests.spawn(async move {
                let mut conn = pool.get().await.unwrap();
                gate.wait().await;
                db_operation::update_running_task(&mut conn, id, failure_dto("stress"), false).await
            });
        }
        tokio::time::timeout(Duration::from_secs(30), async {
            while let Some(result) = requests.join_next().await {
                assert!(matches!(
                    result.unwrap().unwrap(),
                    db_operation::UpdateTaskResult::Updated
                ));
            }
        })
        .await
        .unwrap();
        let failures: Count =
            diesel::sql_query("SELECT count(*) AS n FROM task WHERE status='failure'")
                .get_result(&mut conn)
                .await
                .unwrap();
        assert_eq!(failures.n, 2050);
        let remaining: Count = diesel::sql_query("SELECT remaining::bigint AS n FROM batch")
            .get_result(&mut conn)
            .await
            .unwrap();
        assert_eq!(remaining.n, 0);
    }
}

/// Manual end-to-end SQL-path comparison, without HTTP or other workers.
#[tokio::test]
#[ignore = "manual transition CPU/query benchmark"]
async fn benchmark_task_transition_locks() {
    use diesel::connection::InstrumentationEvent;
    use diesel_async::AsyncConnection;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    for (shape, n) in [
        ("leaf_success", 500),
        ("leaf_failure", 500),
        ("chain", 500),
        ("wide", 10_000),
        ("diamonds", 2_001),
    ] {
        for repetition in 0..3 {
            let db = setup_test_db().await;
            let mut conn = db.pool.get().await.unwrap();
            let ids: Vec<_> = (1..=n).map(|i| Uuid::from_u128(i as u128)).collect();
            diesel::sql_query(
                "INSERT INTO task(id,name,kind,status,timeout,created_at,started_at,last_updated,
                 metadata,start_condition,wait_success,wait_finished,success,failures,dead_end_barrier,priority)
                 SELECT id, 'bench', 'audit8', 'running', 60, now(), now(), now(),
                 '{}'::jsonb, '[]'::jsonb, 0, 0, 0, 0, false, 0 FROM unnest($1::uuid[]) AS id",
            ).bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(&ids)
                .execute(&mut conn).await.unwrap();
            let leaf = shape.starts_with("leaf");
            if !leaf {
                let mut parents = Vec::new();
                let mut children = Vec::new();
                for i in 1..n {
                    let parent = match shape {
                        "chain" => i - 1,
                        "wide" => 0,
                        _ if i <= 200 => 0,
                        _ => i - 200,
                    };
                    parents.push(ids[parent]);
                    children.push(ids[i]);
                    if shape == "diamonds" && i > 200 {
                        let start = 1 + ((i - 1) / 200 - 1) * 200;
                        parents.push(ids[start + (i - start + 1) % 200]);
                        children.push(ids[i]);
                    }
                }
                diesel::sql_query("INSERT INTO link(parent_id, child_id, requires_success) SELECT p,c,true FROM unnest($1::uuid[], $2::uuid[]) AS x(p,c)")
                    .bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(parents)
                    .bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(children)
                    .execute(&mut conn).await.unwrap();
                diesel::sql_query("UPDATE task t SET status='waiting', wait_finished=c.n, wait_success=c.n FROM (SELECT child_id,count(*)::int AS n FROM link GROUP BY child_id) c WHERE t.id=c.child_id")
                    .execute(&mut conn).await.unwrap();
            }
            diesel::sql_query("ANALYZE task")
                .execute(&mut conn)
                .await
                .unwrap();
            diesel::sql_query("ANALYZE link")
                .execute(&mut conn)
                .await
                .unwrap();
            let queries = Arc::new(AtomicUsize::new(0));
            let observed = queries.clone();
            conn.set_instrumentation(move |event: InstrumentationEvent<'_>| {
                if matches!(event, InstrumentationEvent::StartQuery { .. }) {
                    observed.fetch_add(1, Ordering::Relaxed);
                }
            });
            let cpu = process_cpu_seconds();
            let start = std::time::Instant::now();
            for id in ids.iter().take(if leaf { n } else { 1 }) {
                let dto = if shape == "leaf_success" {
                    success_dto()
                } else {
                    failure_dto("bench")
                };
                assert!(matches!(
                    db_operation::update_running_task(&mut conn, *id, dto, false)
                        .await
                        .unwrap(),
                    db_operation::UpdateTaskResult::Updated
                ));
            }
            let elapsed_ms = start.elapsed().as_secs_f64() * 1000.;
            let cpu_ms = (process_cpu_seconds() - cpu) * 1000.;
            println!(
                "AUDIT8_BENCH {}",
                json!({"shape":shape,"tasks":n,"repetition":repetition,
                "elapsed_ms":elapsed_ms,"app_cpu_ms":cpu_ms,"queries":queries.load(Ordering::Relaxed)})
            );
        }
    }
}

fn process_cpu_seconds() -> f64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes the structure on success, checked before reading.
    assert_eq!(
        unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) },
        0
    );
    let usage = unsafe { usage.assume_init() };
    usage.ru_utime.tv_sec as f64
        + usage.ru_utime.tv_usec as f64 / 1_000_000.
        + usage.ru_stime.tv_sec as f64
        + usage.ru_stime.tv_usec as f64 / 1_000_000.
}
