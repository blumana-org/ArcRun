// Diesel's generated QueryableByName impls need this at module scope.
#![allow(clippy::redundant_field_names)]

//! Bounded, scan-local negative caching must never authorize an admission.

use crate::common::*;
use arcrun::{
    Conn,
    db_operation::{ClaimResult, claim_task_with_rules, mark_task_running},
    models::{StatusKind, Task},
    rule::Strategy,
    workers::run_claim_loop,
};
use diesel::{associations::HasTable, connection::InstrumentationEvent, prelude::*};
use diesel_async::{AsyncConnection, RunQueryDsl};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Default)]
struct QueryCounts {
    transactions: AtomicUsize,
    queries: AtomicUsize,
}

fn count_queries(conn: &mut Conn<'_>) -> Arc<QueryCounts> {
    let counts = Arc::new(QueryCounts::default());
    let observed = counts.clone();
    conn.set_instrumentation(move |event: InstrumentationEvent<'_>| match event {
        InstrumentationEvent::BeginTransaction { .. } => {
            observed.transactions.fetch_add(1, Ordering::Relaxed);
        }
        InstrumentationEvent::StartQuery { .. } => {
            observed.queries.fetch_add(1, Ordering::Relaxed);
        }
        _ => {}
    });
    counts
}

fn rule(kind: &str, capacity: bool, threshold: i32) -> Value {
    let mut result = json!({
        "type": if capacity { "Capacity" } else { "Concurency" },
        "matcher": {"kind": kind, "status": "Running", "fields": ["key"]}
    });
    result[if capacity {
        "max_capacity"
    } else {
        "max_concurency"
    }] = json!(threshold);
    result
}

fn candidate(name: &str, key: &str, priority: i32, rules: Vec<Value>) -> Value {
    let mut task = task_with_rules(name, name, "cache", json!(rules));
    task["metadata"] = json!({"key": key});
    task["expected_count"] = json!(100);
    task["priority"] = json!(priority);
    task
}

async fn snapshot(conn: &mut Conn<'_>, id: uuid::Uuid) -> Task {
    Task::table().find(id).first(conn).await.unwrap()
}

async fn start_owner(conn: &mut Conn<'_>, id: uuid::Uuid) {
    let task = snapshot(conn, id).await;
    assert_eq!(
        claim_task_with_rules(conn, &task).await.unwrap(),
        ClaimResult::Claimed
    );
    mark_task_running(conn, &id).await.unwrap();
}

#[derive(diesel::QueryableByName)]
struct Used {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    used: i32,
}

fn slot_key(rule: Value, key: &str) -> String {
    let metadata = json!({"key": key});
    match serde_json::from_value::<Strategy>(rule).unwrap() {
        Strategy::Capacity(rule) => arcrun::rule::capacity_slot_key(&rule, &metadata),
        Strategy::Concurency(rule) => arcrun::rule::concurrency_slot_key(&rule, &metadata),
    }
    .unwrap()
}

async fn slot_used(conn: &mut Conn<'_>, rule: Value, key: &str) -> i32 {
    diesel::sql_query("SELECT used FROM rule_slot WHERE lock_key=$1")
        .bind::<diesel::sql_types::Text, _>(slot_key(rule, key))
        .get_result::<Used>(conn)
        .await
        .unwrap()
        .used
}

/// Bug 3 follow-up: every Capacity-blocked candidate used to start a transaction.
/// One refusal must cover matching candidates across pages, without hiding other
/// metadata keys. A fresh scan must see progress and preserve allowed overshoot.
#[actix_web::test]
async fn test_bug3_capacity_cache_skips_repeated_claims_and_resets_after_progress() {
    let (_guard, state) = setup_test_app().await;
    let app = test_service!(state);
    let cap = rule("cache", true, 100);
    let owner = create_tasks_ok(&app, &[candidate("owner", "hot", 0, vec![cap.clone()])]).await;
    let mut conn = state.pool.get().await.unwrap();
    start_owner(&mut conn, owner[0].id).await;

    let mut tasks: Vec<_> = (0..40)
        .map(|n| candidate(&format!("blocked-{n}"), "hot", 1000 - n, vec![cap.clone()]))
        .collect();
    tasks.push(candidate("other-key", "cold", 0, vec![cap.clone()]));
    let created = create_tasks_ok(&app, &tasks).await;
    let counts = count_queries(&mut conn);
    let claimed = run_claim_loop(&mut conn, 10, 3).await;
    assert_eq!(
        claimed.iter().map(|t| t.id).collect::<Vec<_>>(),
        vec![created[40].id]
    );
    assert_eq!(
        counts.transactions.load(Ordering::Relaxed),
        2,
        "one saturated probe plus the eligible key, not one transaction per blocked task"
    );
    for task in &created[..40] {
        let actual = snapshot(&mut conn, task.id).await;
        assert_eq!(actual.status, StatusKind::Pending);
        assert!(actual.claimed_slot_keys.is_none());
        assert!(actual.capacity_charge.is_none());
    }
    assert_eq!(slot_used(&mut conn, cap.clone(), "hot").await, 100);
    assert_eq!(slot_used(&mut conn, cap.clone(), "cold").await, 100);

    arcrun::workers::run_counter_flush_once(&mut conn, &[(owner[0].id, 70, 0)])
        .await
        .unwrap();
    assert_eq!(slot_used(&mut conn, cap.clone(), "hot").await, 30);
    let counts = count_queries(&mut conn);
    let claimed = run_claim_loop(&mut conn, 1, 3).await;
    assert_eq!(
        claimed.iter().map(|t| t.id).collect::<Vec<_>>(),
        vec![created[0].id]
    );
    assert_eq!(counts.transactions.load(Ordering::Relaxed), 1);
    assert_eq!(
        slot_used(&mut conn, cap, "hot").await,
        130,
        "existing used < threshold admission still allows candidate-charge overshoot"
    );
}

/// Bug 3 follow-up: a negative observation proves saturation only up to its
/// threshold. Larger limits must still reach SQL; a later stronger observation
/// must cover subsequent smaller limits, including across page boundaries.
#[actix_web::test]
async fn test_bug3_capacity_cache_respects_candidate_thresholds() {
    let (_guard, state) = setup_test_app().await;
    let app = test_service!(state);
    let owner = create_tasks_ok(
        &app,
        &[candidate("owner", "hot", 0, vec![rule("cache", true, 100)])],
    )
    .await;
    let mut conn = state.pool.get().await.unwrap();
    start_owner(&mut conn, owner[0].id).await;
    let tasks: Vec<_> = [100, 50, 200, 150, 125]
        .into_iter()
        .enumerate()
        .map(|(n, threshold)| {
            candidate(
                &format!("limit-{threshold}"),
                "hot",
                100 - n as i32,
                vec![rule("cache", true, threshold)],
            )
        })
        .collect();
    let created = create_tasks_ok(&app, &tasks).await;
    let counts = count_queries(&mut conn);
    let claimed = run_claim_loop(&mut conn, 10, 2).await;
    assert_eq!(
        claimed.iter().map(|t| t.id).collect::<Vec<_>>(),
        vec![created[2].id]
    );
    assert_eq!(
        counts.transactions.load(Ordering::Relaxed),
        3,
        "100 blocks, 200 admits, 150 blocks; 50 and 125 reuse proven thresholds"
    );
    assert_eq!(
        slot_used(&mut conn, rule("cache", true, 100), "hot").await,
        200
    );
}

/// Bug 3 follow-up: a mixed task must only cache its actual rejected slot.
/// Earlier successful reservations roll back; neither those slots nor rules
/// not reached by the transaction may poison a later candidate's prefilter.
#[actix_web::test]
async fn test_bug3_capacity_cache_isolates_rejected_slot_in_mixed_rules() {
    for (free_capacity, blocked_capacity) in [(true, false), (false, true), (true, true)] {
        let (_guard, state) = setup_test_app().await;
        let app = test_service!(state);
        let free_rule = rule(
            "a-cache",
            free_capacity,
            if free_capacity { 100 } else { 1 },
        );
        let full_rule = rule(
            "z-cache",
            blocked_capacity,
            if blocked_capacity { 100 } else { 1 },
        );
        let owner = create_tasks_ok(
            &app,
            &[candidate("owner", "hot", 0, vec![full_rule.clone()])],
        )
        .await;
        let mut conn = state.pool.get().await.unwrap();
        start_owner(&mut conn, owner[0].id).await;
        let created = create_tasks_ok(
            &app,
            &[
                candidate(
                    "mixed",
                    "hot",
                    30,
                    vec![free_rule.clone(), full_rule.clone()],
                ),
                candidate("free", "hot", 20, vec![free_rule.clone()]),
                candidate("blocked", "hot", 10, vec![full_rule.clone()]),
            ],
        )
        .await;
        let counts = count_queries(&mut conn);
        let claimed = run_claim_loop(&mut conn, 10, 1).await;
        assert_eq!(
            claimed.iter().map(|t| t.id).collect::<Vec<_>>(),
            vec![created[1].id]
        );
        assert_eq!(
            counts.transactions.load(Ordering::Relaxed),
            2,
            "only the mixed refusal and the independent eligible rule reach SQL"
        );
        assert_eq!(
            slot_used(&mut conn, free_rule, "hot").await,
            if free_capacity { 100 } else { 1 }
        );
        assert_eq!(
            slot_used(&mut conn, full_rule, "hot").await,
            if blocked_capacity { 100 } else { 1 }
        );
        for id in [created[0].id, created[2].id] {
            let task = snapshot(&mut conn, id).await;
            assert_eq!(task.status, StatusKind::Pending);
            assert!(task.claimed_slot_keys.is_none());
            assert!(task.capacity_charge.is_none());
        }
    }
}

/// Bug 3 follow-up: invalid rules and database errors are not observations of
/// saturation. Neither may suppress a valid later claim of the same slot.
#[actix_web::test]
async fn test_bug3_capacity_cache_does_not_remember_validation_or_sql_errors() {
    let (_guard, state) = setup_test_app().await;
    let app = test_service!(state);
    let cap = rule("cache", true, 100);
    let mut error_task = candidate("sql-error", "hot", 20, vec![cap.clone()]);
    error_task["expected_count"] = json!(13);
    let created = create_tasks_ok(
        &app,
        &[
            candidate("invalid", "hot", 30, vec![cap.clone()]),
            error_task,
            candidate("valid", "hot", 10, vec![cap.clone()]),
        ],
    )
    .await;
    let mut conn = state.pool.get().await.unwrap();
    diesel::sql_query("UPDATE task SET expected_count=NULL WHERE id=$1")
        .bind::<diesel::sql_types::Uuid, _>(created[0].id)
        .execute(&mut conn)
        .await
        .unwrap();
    diesel::sql_query(
        "CREATE FUNCTION reject_charge_13() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN IF NEW.used = 13 THEN RAISE EXCEPTION 'injected slot write failure';
         END IF; RETURN NEW; END $$",
    )
    .execute(&mut conn)
    .await
    .unwrap();
    diesel::sql_query(
        "CREATE TRIGGER reject_charge_13 BEFORE INSERT OR UPDATE ON rule_slot
         FOR EACH ROW EXECUTE FUNCTION reject_charge_13()",
    )
    .execute(&mut conn)
    .await
    .unwrap();
    let counts = count_queries(&mut conn);
    let claimed = run_claim_loop(&mut conn, 10, 1).await;
    assert_eq!(
        claimed.iter().map(|t| t.id).collect::<Vec<_>>(),
        vec![created[2].id]
    );
    assert_eq!(counts.transactions.load(Ordering::Relaxed), 2);
    assert_eq!(slot_used(&mut conn, cap, "hot").await, 100);
    for id in [created[0].id, created[1].id] {
        let task = snapshot(&mut conn, id).await;
        assert_eq!(task.status, StatusKind::Pending);
        assert!(task.claimed_slot_keys.is_none());
    }
}

/// Manual benchmark of the real paginated claim loop, including Diesel and JSON
/// decoding, excluding seeding/HTTP/leader checks. Run alone with --ignored
/// --nocapture --test-threads=1; print counts as well as wall time and process CPU.
#[tokio::test]
#[ignore = "manual Capacity-cache benchmark; needs PostgreSQL and an otherwise idle machine"]
async fn benchmark_capacity_cache_scans() {
    for (name, tasks, keys, saturated) in [
        ("shared", 10_000, 1, true),
        ("hundred_keys", 10_000, 100, true),
        ("unique_keys", 2_000, 2_000, true),
        ("accepted", 1_000, 1_000, false),
    ] {
        for repetition in 0..3 {
            let db = setup_test_db().await;
            let mut conn = db.pool.get().await.unwrap();
            let rules = json!([rule("cache", true, 100)]);
            diesel::sql_query(
                "INSERT INTO task (id, name, kind, status, timeout, created_at,
                 last_updated, metadata, start_condition, wait_success, wait_finished,
                 success, failures, expected_count, dead_end_barrier, priority)
                 SELECT gen_random_uuid(), 'candidate-' || n, 'cache', 'pending', 60,
                 now(), now(), jsonb_build_object('key', ((n-1) % $2)::text), $3,
                 0, 0, 0, 0, 100, false, 0 FROM generate_series(1, $1) AS n",
            )
            .bind::<diesel::sql_types::Integer, _>(tasks)
            .bind::<diesel::sql_types::Integer, _>(keys)
            .bind::<diesel::sql_types::Jsonb, _>(&rules)
            .execute(&mut conn)
            .await
            .unwrap();
            if saturated {
                let slot_keys: Vec<_> = (0..keys)
                    .map(|n| slot_key(rule("cache", true, 100), &n.to_string()))
                    .collect();
                diesel::sql_query("INSERT INTO rule_slot (lock_key, used) SELECT unnest($1), 100")
                    .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(&slot_keys)
                    .execute(&mut conn)
                    .await
                    .unwrap();
                diesel::sql_query(
                    "INSERT INTO task (id, name, kind, status, timeout, created_at,
                     started_at, last_updated, metadata, start_condition, wait_success,
                     wait_finished, success, failures, expected_count, dead_end_barrier,
                     priority, claimed_slot_keys, capacity_charge)
                     SELECT gen_random_uuid(), 'owner-' || n, 'cache', 'running', 60,
                     now(), now(), now(), jsonb_build_object('key', (n-1)::text), $2,
                     0, 0, 0, 0, 100, false, 0, ARRAY[k], 100
                     FROM unnest($1::text[]) WITH ORDINALITY AS s(k, n)",
                )
                .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(&slot_keys)
                .bind::<diesel::sql_types::Jsonb, _>(&rules)
                .execute(&mut conn)
                .await
                .unwrap();
            }
            diesel::sql_query("ANALYZE task")
                .execute(&mut conn)
                .await
                .unwrap();
            let counts = count_queries(&mut conn);
            let cpu = process_cpu_seconds();
            let start = std::time::Instant::now();
            let claimed = run_claim_loop(&mut conn, i64::from(tasks), 500).await;
            let elapsed_ms = start.elapsed().as_secs_f64() * 1000.;
            let cpu_ms = (process_cpu_seconds() - cpu) * 1000.;
            assert_eq!(claimed.len(), if saturated { 0 } else { tasks as usize });
            eprintln!(
                "CAPACITY_CACHE_BENCH {}",
                json!({
                    "scenario": name, "repetition": repetition, "tasks": tasks, "keys": keys,
                    "elapsed_ms": elapsed_ms, "app_cpu_ms": cpu_ms,
                    "transactions": counts.transactions.load(Ordering::Relaxed),
                    "queries": counts.queries.load(Ordering::Relaxed),
                })
            );
        }
    }
}

fn process_cpu_seconds() -> f64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage writes the whole rusage on success; we only read it then.
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    assert_eq!(result, 0);
    let usage = unsafe { usage.assume_init() };
    usage.ru_utime.tv_sec as f64
        + usage.ru_utime.tv_usec as f64 / 1_000_000.
        + usage.ru_stime.tv_sec as f64
        + usage.ru_stime.tv_usec as f64 / 1_000_000.
}
