// Diesel QueryableByName emits separate impls with explicit field initializers;
// an allow on the struct does not cover those generated impls.
#![allow(clippy::redundant_field_names)]

//! Audit 2, D1/D7 (7.3a) — DB-enforced Concurrency via `rule_slot` counters + the
//! start_loop leader lease.
//!
//! Concurrency rules are now enforced by incrementing a per-rule `rule_slot.used` in
//! the claim transaction (blocked when `used >= max_concurency`) and decrementing it on
//! every exit from Claimed/Running. These tests exercise the counter directly (asserting
//! `rule_slot.used` and `task.claimed_slot_keys`) across every release site, plus the GC
//! and the leader lease.

use crate::common::*;

use arcrun::db_operation::{ClaimResult, claim_task_with_rules};
use arcrun::models::{StatusKind, Task};
use arcrun::rule::{ConcurencyRule, Matcher, Rules, Strategy};
use arcrun::workers::WorkerNudges;
use diesel_async::RunQueryDsl;
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

// The start_loop leader-lease advisory key MUST mirror `START_LEADER_LOCK_KEY` in
// src/workers/start_loop.rs (kept in sync by hand — it is a private const there).
const LEADER_LOCK_KEY: i64 = 0x4152_4352_554E_0001;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn conc_rule(kind: &str, max: i32, fields: &[&str]) -> ConcurencyRule {
    ConcurencyRule {
        max_concurency: max,
        matcher: Matcher {
            kind: kind.to_string(),
            status: StatusKind::Running,
            fields: fields.iter().map(|s| s.to_string()).collect(),
        },
    }
}

fn rules_of(rule: &ConcurencyRule) -> Rules {
    Rules(vec![Strategy::Concurency(rule.clone())])
}

fn slot_key(rule: &ConcurencyRule, metadata: &serde_json::Value) -> String {
    arcrun::rule::concurrency_slot_key(rule, metadata).expect("slot key")
}

fn task_for_claim(id: uuid::Uuid, kind: &str, metadata: serde_json::Value, rules: Rules) -> Task {
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

fn task_json_with_rules(
    id: &str,
    kind: &str,
    metadata: serde_json::Value,
    max: i32,
) -> serde_json::Value {
    json!({
        "id": id,
        "name": id,
        "kind": kind,
        "timeout": 60,
        "metadata": metadata,
        "on_start": webhook_action(),
        "rules": [{
            "type": "Concurency",
            "max_concurency": max,
            "matcher": { "kind": kind, "status": "Running", "fields": [] }
        }]
    })
}

#[derive(diesel::QueryableByName)]
struct UsedRow {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    used: i32,
}

/// Current `used` for a slot key, or `None` if the row does not exist.
async fn slot_used(pool: &arcrun::DbPool, key: &str) -> Option<i32> {
    let mut conn = pool.get().await.unwrap();
    let rows = diesel::sql_query("SELECT used FROM rule_slot WHERE lock_key = $1")
        .bind::<diesel::sql_types::Text, _>(key)
        .get_results::<UsedRow>(&mut *conn)
        .await
        .unwrap();
    rows.into_iter().next().map(|r| r.used)
}

#[derive(diesel::QueryableByName)]
struct KeysRow {
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Array<diesel::sql_types::Text>>)]
    claimed_slot_keys: Option<Vec<String>>,
}

async fn claimed_keys(pool: &arcrun::DbPool, id: uuid::Uuid) -> Option<Vec<String>> {
    let mut conn = pool.get().await.unwrap();
    let row: KeysRow = diesel::sql_query("SELECT claimed_slot_keys FROM task WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(id)
        .get_result(&mut *conn)
        .await
        .unwrap();
    row.claimed_slot_keys
}

#[derive(diesel::QueryableByName)]
struct StatusRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    status: String,
}

async fn task_status_text(pool: &arcrun::DbPool, id: uuid::Uuid) -> String {
    let mut conn = pool.get().await.unwrap();
    let row: StatusRow = diesel::sql_query("SELECT status::text AS status FROM task WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(id)
        .get_result(&mut *conn)
        .await
        .unwrap();
    row.status
}

async fn set_last_updated_past(pool: &arcrun::DbPool, id: uuid::Uuid, secs_ago: i64) {
    let mut conn = pool.get().await.unwrap();
    let past = chrono::Utc::now() - chrono::Duration::seconds(secs_ago);
    diesel::sql_query("UPDATE task SET last_updated = $1 WHERE id = $2")
        .bind::<diesel::sql_types::Timestamptz, _>(past)
        .bind::<diesel::sql_types::Uuid, _>(id)
        .execute(&mut *conn)
        .await
        .unwrap();
}

/// Poll the task status until it is no longer `pending`, up to `timeout_ms`.
async fn wait_leaves_pending(pool: &arcrun::DbPool, id: uuid::Uuid, timeout_ms: u64) -> String {
    let deadline = std::time::Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let s = task_status_text(pool, id).await;
        if s != "pending" || std::time::Instant::now() >= deadline {
            return s;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

// ---------------------------------------------------------------------------
// Claim + block + release sequencing
// ---------------------------------------------------------------------------

/// max_concurency=1: two Pending, only one claims; the second starts only after the
/// first finishes (releasing its slot). Asserts the `rule_slot.used` counter throughout.
#[tokio::test]
async fn test_slot_max_one_blocks_then_unblocks_on_success() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let created = create_tasks_ok(
        &app,
        &[
            task_json_with_rules("s1", "slot-one", json!({"test": true}), 1),
            task_json_with_rules("s2", "slot-one", json!({"test": true}), 1),
        ],
    )
    .await;
    let (id1, id2) = (created[0].id, created[1].id);

    let rule = conc_rule("slot-one", 1, &[]);
    let key = slot_key(&rule, &json!({"test": true}));
    let rules = rules_of(&rule);

    let mut conn = state.pool.get().await.unwrap();

    // Claim t1 → slot used = 1, keys persisted.
    let t1 = task_for_claim(id1, "slot-one", json!({"test": true}), rules.clone());
    assert_eq!(
        claim_task_with_rules(&mut conn, &t1).await.unwrap(),
        ClaimResult::Claimed
    );
    assert_eq!(slot_used(&state.pool, &key).await, Some(1));
    assert_eq!(
        claimed_keys(&state.pool, id1).await,
        Some(vec![key.clone()])
    );

    // Claim t2 → blocked, slot stays 1, no keys persisted for t2.
    let t2 = task_for_claim(id2, "slot-one", json!({"test": true}), rules.clone());
    assert_eq!(
        claim_task_with_rules(&mut conn, &t2).await.unwrap(),
        ClaimResult::RuleBlocked
    );
    assert_eq!(slot_used(&state.pool, &key).await, Some(1));
    assert_eq!(claimed_keys(&state.pool, id2).await, None);
    assert_eq!(task_status_text(&state.pool, id2).await, "pending");

    // Complete t1 (Running → Success) → slot released to 0, keys NULLed.
    arcrun::db_operation::mark_task_running(&mut conn, &id1)
        .await
        .unwrap();
    assert_eq!(
        arcrun::db_operation::update_running_task(&mut conn, id1, success_dto(), true)
            .await
            .unwrap(),
        arcrun::db_operation::UpdateTaskResult::Updated
    );
    assert_eq!(slot_used(&state.pool, &key).await, Some(0));
    assert_eq!(claimed_keys(&state.pool, id1).await, None);

    // Now t2 claims.
    assert_eq!(
        claim_task_with_rules(&mut conn, &t2).await.unwrap(),
        ClaimResult::Claimed
    );
    assert_eq!(slot_used(&state.pool, &key).await, Some(1));
    assert_eq!(
        claimed_keys(&state.pool, id2).await,
        Some(vec![key.clone()])
    );
}

/// Release on failure via update_running_task(Failure).
#[tokio::test]
async fn test_slot_released_on_failure() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);
    let created = create_tasks_ok(
        &app,
        &[task_json_with_rules(
            "f1",
            "slot-fail",
            json!({"test": true}),
            1,
        )],
    )
    .await;
    let id = created[0].id;
    let rule = conc_rule("slot-fail", 1, &[]);
    let key = slot_key(&rule, &json!({"test": true}));

    let mut conn = state.pool.get().await.unwrap();
    let t = task_for_claim(id, "slot-fail", json!({"test": true}), rules_of(&rule));
    assert_eq!(
        claim_task_with_rules(&mut conn, &t).await.unwrap(),
        ClaimResult::Claimed
    );
    assert_eq!(slot_used(&state.pool, &key).await, Some(1));

    arcrun::db_operation::mark_task_running(&mut conn, &id)
        .await
        .unwrap();
    arcrun::db_operation::update_running_task(&mut conn, id, failure_dto("boom"), true)
        .await
        .unwrap();
    assert_eq!(slot_used(&state.pool, &key).await, Some(0));
    assert_eq!(claimed_keys(&state.pool, id).await, None);
}

/// Release when a Running task is canceled; a cancel of a Pending (never-claimed)
/// task holding NO slot leaves the counter untouched.
#[tokio::test]
async fn test_slot_released_on_cancel_running_but_not_pending() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);
    let created = create_tasks_ok(
        &app,
        &[
            task_json_with_rules("c1", "slot-cancel", json!({"test": true}), 5),
            task_json_with_rules("c2", "slot-cancel", json!({"test": true}), 5),
        ],
    )
    .await;
    let (id1, id2) = (created[0].id, created[1].id);
    let rule = conc_rule("slot-cancel", 5, &[]);
    let key = slot_key(&rule, &json!({"test": true}));
    let rules = rules_of(&rule);

    let mut conn = state.pool.get().await.unwrap();
    // Claim + run t1 (holds slot), t2 stays Pending (never claimed).
    let t1 = task_for_claim(id1, "slot-cancel", json!({"test": true}), rules.clone());
    claim_task_with_rules(&mut conn, &t1).await.unwrap();
    arcrun::db_operation::mark_task_running(&mut conn, &id1)
        .await
        .unwrap();
    assert_eq!(slot_used(&state.pool, &key).await, Some(1));

    // Cancel the Pending t2 → it never consumed a slot → counter unchanged.
    arcrun::workers::cancel_task(&id2, true, &mut conn)
        .await
        .unwrap();
    assert_eq!(
        slot_used(&state.pool, &key).await,
        Some(1),
        "cancel of a Pending task must not release a slot"
    );

    // Cancel the Running t1 → slot released.
    arcrun::workers::cancel_task(&id1, true, &mut conn)
        .await
        .unwrap();
    assert_eq!(slot_used(&state.pool, &key).await, Some(0));
    assert_eq!(claimed_keys(&state.pool, id1).await, None);
}

/// Release on timeout (Running task times out via the timeout loop).
#[tokio::test]
async fn test_slot_released_on_timeout() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);
    let created = create_tasks_ok(
        &app,
        &[task_json_with_rules(
            "to1",
            "slot-timeout",
            json!({"test": true}),
            1,
        )],
    )
    .await;
    let id = created[0].id;
    let rule = conc_rule("slot-timeout", 1, &[]);
    let key = slot_key(&rule, &json!({"test": true}));

    {
        let mut conn = state.pool.get().await.unwrap();
        let t = task_for_claim(id, "slot-timeout", json!({"test": true}), rules_of(&rule));
        claim_task_with_rules(&mut conn, &t).await.unwrap();
        arcrun::db_operation::mark_task_running(&mut conn, &id)
            .await
            .unwrap();
    }
    assert_eq!(slot_used(&state.pool, &key).await, Some(1));

    // Make it look stale so the timeout loop fails it.
    set_last_updated_past(&state.pool, id, 120).await;

    let (sd_tx, sd_rx) = tokio::sync::watch::channel(false);
    let pool = state.pool.clone();
    let h = tokio::spawn(async move {
        arcrun::workers::timeout_loop(
            pool,
            Duration::from_millis(50),
            Duration::from_secs(30),
            true,
            100,
            sd_rx,
            WorkerNudges::new(),
        )
        .await;
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    let _ = sd_tx.send(true);
    let _ = h.await;

    assert_eq!(task_status_text(&state.pool, id).await, "failure");
    assert_eq!(slot_used(&state.pool, &key).await, Some(0));
    assert_eq!(claimed_keys(&state.pool, id).await, None);
}

/// Release on requeue-stale: a stale Claimed task is requeued to Pending, its slot is
/// released and its `claimed_slot_keys` NULLed (so a later re-claim re-increments cleanly).
#[tokio::test]
async fn test_slot_released_on_requeue_stale() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);
    let created = create_tasks_ok(
        &app,
        &[task_json_with_rules(
            "rq1",
            "slot-requeue",
            json!({"test": true}),
            1,
        )],
    )
    .await;
    let id = created[0].id;
    let rule = conc_rule("slot-requeue", 1, &[]);
    let key = slot_key(&rule, &json!({"test": true}));

    {
        let mut conn = state.pool.get().await.unwrap();
        let t = task_for_claim(id, "slot-requeue", json!({"test": true}), rules_of(&rule));
        claim_task_with_rules(&mut conn, &t).await.unwrap();
    }
    assert_eq!(slot_used(&state.pool, &key).await, Some(1));
    assert_eq!(task_status_text(&state.pool, id).await, "claimed");

    // Stale Claimed → requeue loop moves it back to Pending.
    set_last_updated_past(&state.pool, id, 120).await;

    let (sd_tx, sd_rx) = tokio::sync::watch::channel(false);
    let pool = state.pool.clone();
    let h = tokio::spawn(async move {
        arcrun::workers::timeout_loop(
            pool,
            Duration::from_millis(50),
            Duration::from_secs(30),
            true,
            100,
            sd_rx,
            WorkerNudges::new(),
        )
        .await;
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    let _ = sd_tx.send(true);
    let _ = h.await;

    assert_eq!(task_status_text(&state.pool, id).await, "pending");
    assert_eq!(slot_used(&state.pool, &key).await, Some(0));
    assert_eq!(
        claimed_keys(&state.pool, id).await,
        None,
        "requeue must NULL claimed_slot_keys"
    );
}

/// Release on stop_batch (DELETE /batch): a Claimed task in the batch releases its slot.
#[tokio::test]
async fn test_slot_released_on_stop_batch() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);
    let created = create_tasks_ok(
        &app,
        &[task_json_with_rules(
            "sb1",
            "slot-stop",
            json!({"test": true}),
            1,
        )],
    )
    .await;
    let id = created[0].id;
    let batch_id = created[0].batch_id.unwrap();
    let rule = conc_rule("slot-stop", 1, &[]);
    let key = slot_key(&rule, &json!({"test": true}));

    {
        let mut conn = state.pool.get().await.unwrap();
        let t = task_for_claim(id, "slot-stop", json!({"test": true}), rules_of(&rule));
        claim_task_with_rules(&mut conn, &t).await.unwrap();
    }
    assert_eq!(slot_used(&state.pool, &key).await, Some(1));

    let stop_req = actix_web::test::TestRequest::delete()
        .uri(&format!("/batch/{}", batch_id))
        .to_request();
    let resp = actix_web::test::call_service(&app, stop_req).await;
    assert!(resp.status().is_success());

    assert_eq!(task_status_text(&state.pool, id).await, "canceled");
    assert_eq!(slot_used(&state.pool, &key).await, Some(0));
    assert_eq!(claimed_keys(&state.pool, id).await, None);
}

/// A full metadata replace while Running does NOT leak the slot: release reads the
/// stored `claimed_slot_keys` (from claim time), not a recomputed key.
#[tokio::test]
async fn test_slot_no_leak_after_metadata_mutation() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);
    let rule = conc_rule("slot-meta", 1, &["projectId"]);
    let mut payload = task_json_with_rules("mm1", "slot-meta", json!({"projectId": 1}), 1);
    // Persist the same metadata matcher that admission evaluates. A snapshot
    // with different rules is now correctly rejected as stale.
    payload["rules"] = json!(rules_of(&rule));
    let created = create_tasks_ok(&app, &[payload]).await;
    let id = created[0].id;
    let key = slot_key(&rule, &json!({"projectId": 1}));

    let mut conn = state.pool.get().await.unwrap();
    let t = task_for_claim(id, "slot-meta", json!({"projectId": 1}), rules_of(&rule));
    assert_eq!(
        claim_task_with_rules(&mut conn, &t).await.unwrap(),
        ClaimResult::Claimed
    );
    assert!(
        arcrun::db_operation::mark_task_running(&mut conn, &id)
            .await
            .unwrap()
    );
    assert_eq!(slot_used(&state.pool, &key).await, Some(1));

    // Full-replace the metadata (counter-only PATCH, no status change).
    let meta_update = arcrun::dtos::UpdateTaskDto {
        status: None,
        metadata: Some(json!({"projectId": 999, "changed": true})),
        new_success: None,
        new_failures: None,
        failure_reason: None,
        expected_count: None,
        priority: None,
    };
    arcrun::db_operation::update_running_task(&mut conn, id, meta_update, true)
        .await
        .unwrap();
    // The recomputed key would now differ — but the stored keys are unchanged.
    assert_eq!(claimed_keys(&state.pool, id).await, Some(vec![key.clone()]));

    // Finish → release must still hit the ORIGINAL slot (no leak).
    arcrun::db_operation::update_running_task(&mut conn, id, success_dto(), true)
        .await
        .unwrap();
    assert_eq!(
        slot_used(&state.pool, &key).await,
        Some(0),
        "release must use the stored claim-time key, not the mutated metadata"
    );
    assert_eq!(claimed_keys(&state.pool, id).await, None);
}

/// GC deletes `used = 0` rows; a live (`used > 0`) row survives.
#[tokio::test]
async fn test_gc_empty_rule_slots() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);
    let created = create_tasks_ok(
        &app,
        &[
            task_json_with_rules("gc1", "slot-gc-a", json!({"test": true}), 1),
            task_json_with_rules("gc2", "slot-gc-b", json!({"test": true}), 1),
        ],
    )
    .await;
    let (id1, id2) = (created[0].id, created[1].id);
    let rule_a = conc_rule("slot-gc-a", 1, &[]);
    let rule_b = conc_rule("slot-gc-b", 1, &[]);
    let key_a = slot_key(&rule_a, &json!({"test": true}));
    let key_b = slot_key(&rule_b, &json!({"test": true}));

    let mut conn = state.pool.get().await.unwrap();
    // A: claim then finish → row exists with used = 0.
    let ta = task_for_claim(id1, "slot-gc-a", json!({"test": true}), rules_of(&rule_a));
    claim_task_with_rules(&mut conn, &ta).await.unwrap();
    arcrun::db_operation::mark_task_running(&mut conn, &id1)
        .await
        .unwrap();
    arcrun::db_operation::update_running_task(&mut conn, id1, success_dto(), true)
        .await
        .unwrap();
    // B: claim + keep Running → row exists with used = 1.
    let tb = task_for_claim(id2, "slot-gc-b", json!({"test": true}), rules_of(&rule_b));
    claim_task_with_rules(&mut conn, &tb).await.unwrap();
    arcrun::db_operation::mark_task_running(&mut conn, &id2)
        .await
        .unwrap();

    assert_eq!(slot_used(&state.pool, &key_a).await, Some(0));
    assert_eq!(slot_used(&state.pool, &key_b).await, Some(1));

    let deleted = arcrun::db_operation::gc_empty_rule_slots(&mut conn)
        .await
        .unwrap();
    assert!(deleted >= 1);
    assert_eq!(
        slot_used(&state.pool, &key_a).await,
        None,
        "used=0 row GC'd"
    );
    assert_eq!(
        slot_used(&state.pool, &key_b).await,
        Some(1),
        "live row survives GC"
    );
}

// ---------------------------------------------------------------------------
// Leader lease (D7)
// ---------------------------------------------------------------------------

#[derive(diesel::QueryableByName)]
struct CntRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    cnt: i64,
}

/// Number of sessions currently holding the start_loop leader advisory lock.
async fn leader_lock_count(pool: &arcrun::DbPool) -> i64 {
    let mut conn = pool.get().await.unwrap();
    let row: CntRow = diesel::sql_query(
        "SELECT count(*)::bigint AS cnt FROM pg_locks
         WHERE locktype = 'advisory'
           AND (classid::bigint * 4294967296 + objid::bigint) = $1",
    )
    .bind::<diesel::sql_types::BigInt, _>(LEADER_LOCK_KEY)
    .get_result(&mut *conn)
    .await
    .unwrap();
    row.cnt
}

/// Two leased start loops against the same DB: exactly one is leader, yet scheduling
/// works; when the leader dies, the standby takes over and keeps scheduling.
#[tokio::test]
async fn test_leader_lease_single_leader_and_failover() {
    let (g, state) = setup_test_app().await;
    let app = test_service!(state);
    let url = g.url.clone();

    // A fast mock on_start so claimed tasks transition promptly and deterministically.
    let (hook_url, _hook_sd) = spawn_webhook_server(Arc::new(AtomicUsize::new(0)));
    let make = |id: &str| {
        json!({
            "id": id, "name": id, "kind": "leader", "timeout": 60,
            "metadata": {"test": true},
            "on_start": {"kind": "Webhook", "params": {"url": hook_url, "verb": "Post"}}
        })
    };

    // Spawn loop 1 (becomes leader).
    let (sd1_tx, sd1_rx) = tokio::sync::watch::channel(false);
    let ev1 = state.action_executor.clone();
    let pool1 = state.pool.clone();
    let url1 = url.clone();
    let h1 = tokio::spawn(async move {
        arcrun::workers::start_loop_leased(
            url1,
            &ev1,
            pool1,
            Duration::from_millis(50),
            true,
            50,
            10,
            sd1_rx,
            WorkerNudges::new(),
            Duration::from_secs(30),
        )
        .await;
    });

    // Task 1 gets scheduled by the (sole) leader.
    let c1 = create_tasks_ok(&app, &[make("lead-1")]).await;
    assert_ne!(
        wait_leaves_pending(&state.pool, c1[0].id, 3000).await,
        "pending",
        "leader should schedule task 1"
    );

    // Spawn loop 2 (a standby — loop 1 already holds the lease).
    let (sd2_tx, sd2_rx) = tokio::sync::watch::channel(false);
    let ev2 = state.action_executor.clone();
    let pool2 = state.pool.clone();
    let url2 = url.clone();
    let h2 = tokio::spawn(async move {
        arcrun::workers::start_loop_leased(
            url2,
            &ev2,
            pool2,
            Duration::from_millis(50),
            true,
            50,
            10,
            sd2_rx,
            WorkerNudges::new(),
            Duration::from_secs(30),
        )
        .await;
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Exactly one session holds the leader lock.
    assert_eq!(
        leader_lock_count(&state.pool).await,
        1,
        "exactly one leader while both loops run"
    );

    // Task 2 still gets scheduled (by whichever loop is leader).
    let c2 = create_tasks_ok(&app, &[make("lead-2")]).await;
    assert_ne!(
        wait_leaves_pending(&state.pool, c2[0].id, 3000).await,
        "pending",
        "leader should schedule task 2"
    );

    // Kill loop 1 → its lease connection drops → lock releases → loop 2 takes over.
    let _ = sd1_tx.send(true);
    let _ = h1.await;
    // Give loop 2 a few ticks to re-contend and Postgres to notice the closed socket.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        leader_lock_count(&state.pool).await,
        1,
        "standby should have taken over the single lease after the leader died"
    );

    // Task 3 is scheduled by the new leader (failover works).
    let c3 = create_tasks_ok(&app, &[make("lead-3")]).await;
    assert_ne!(
        wait_leaves_pending(&state.pool, c3[0].id, 3000).await,
        "pending",
        "the promoted standby should schedule task 3"
    );

    let _ = sd2_tx.send(true);
    let _ = h2.await;
}
