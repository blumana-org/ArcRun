// Diesel QueryableByName emits separate impls with explicit field initializers;
// an allow on the struct does not cover those generated impls.
#![allow(clippy::redundant_field_names)]

//! Audit 2, D1 (7.3b) — DB-enforced **Capacity** via `rule_slot` charge counters.
//!
//! Capacity rules are now enforced by charging a per-rule `rule_slot.used` at claim time
//! with the candidate's remaining work (`capacity_charge = GREATEST(expected_count -
//! success - failures, 0)`), shrinking that charge as progress is flushed by the
//! batch_updater, and releasing the outstanding charge on every exit from Claimed/Running.
//! These tests exercise the counter directly (asserting `rule_slot.used`,
//! `task.capacity_charge`, `task.claimed_slot_keys`) across claim/block, the flush delta,
//! terminal release, requeue-stale, the D1 semantic change, the new guards, and a task
//! carrying both a Concurrency and a Capacity rule.

use crate::common::*;

use arcrun::db_operation::{ClaimResult, claim_task_with_rules};
use arcrun::models::{StatusKind, Task};
use arcrun::rule::{CapacityRule, ConcurencyRule, Matcher, Rules, Strategy};
use arcrun::workers::WorkerNudges;
use diesel_async::RunQueryDsl;
use serde_json::json;
use std::time::Duration;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn cap_rule(kind: &str, max_capacity: i32, fields: &[&str]) -> CapacityRule {
    CapacityRule {
        max_capacity,
        matcher: Matcher {
            kind: kind.to_string(),
            status: StatusKind::Running,
            fields: fields.iter().map(|s| s.to_string()).collect(),
        },
    }
}

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

fn cap_slot_key(rule: &CapacityRule, metadata: &serde_json::Value) -> String {
    arcrun::rule::capacity_slot_key(rule, metadata).expect("cap slot key")
}

fn conc_slot_key(rule: &ConcurencyRule, metadata: &serde_json::Value) -> String {
    arcrun::rule::concurrency_slot_key(rule, metadata).expect("conc slot key")
}

/// Build a minimal Task for a direct `claim_task_with_rules` call.
fn task_for_claim(
    id: uuid::Uuid,
    kind: &str,
    metadata: serde_json::Value,
    rules: Rules,
    expected_count: Option<i32>,
) -> Task {
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
        expected_count,
        dead_end_barrier: false,
        priority: 0,
        claimed_slot_keys: None,
        capacity_charge: None,
    }
}

/// A capacity task JSON for `POST /task` (so a real row exists to claim against).
fn cap_task_json(
    id: &str,
    kind: &str,
    metadata: serde_json::Value,
    expected_count: i32,
    max_capacity: i32,
    fields: &[&str],
) -> serde_json::Value {
    json!({
        "id": id,
        "name": id,
        "kind": kind,
        "timeout": 60,
        "expected_count": expected_count,
        "metadata": metadata,
        "on_start": webhook_action(),
        "rules": [{
            "type": "Capacity",
            "max_capacity": max_capacity,
            "matcher": { "kind": kind, "status": "Running", "fields": fields }
        }]
    })
}

/// A plain (rule-less) task JSON.
fn plain_task_json(
    id: &str,
    kind: &str,
    metadata: serde_json::Value,
    expected: i32,
) -> serde_json::Value {
    json!({
        "id": id,
        "name": id,
        "kind": kind,
        "timeout": 60,
        "expected_count": expected,
        "metadata": metadata,
        "on_start": webhook_action()
    })
}

#[derive(diesel::QueryableByName)]
struct UsedRow {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    used: i32,
}

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
struct ChargeRow {
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Integer>)]
    capacity_charge: Option<i32>,
}

async fn capacity_charge_of(pool: &arcrun::DbPool, id: uuid::Uuid) -> Option<i32> {
    let mut conn = pool.get().await.unwrap();
    let row: ChargeRow = diesel::sql_query("SELECT capacity_charge FROM task WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(id)
        .get_result(&mut *conn)
        .await
        .unwrap();
    row.capacity_charge
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

// ---------------------------------------------------------------------------
// 1. Block at limit
// ---------------------------------------------------------------------------

/// A holder claims through a Capacity rule (charge = expected_count = max_capacity), so
/// `used = max_capacity`; a candidate whose admission check `used >= max_capacity` is
/// blocked. Asserts `rule_slot.used` and `task.capacity_charge` via raw SQL.
#[tokio::test]
async fn test_capacity_slot_blocks_at_limit() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let created = create_tasks_ok(
        &app,
        &[
            cap_task_json("hold", "cap-limit", json!({"p": 1}), 500, 500, &[]),
            cap_task_json("cand", "cap-limit", json!({"p": 1}), 100, 500, &[]),
        ],
    )
    .await;
    let (id1, id2) = (created[0].id, created[1].id);
    let rule = cap_rule("cap-limit", 500, &[]);
    let key = cap_slot_key(&rule, &json!({"p": 1}));
    let rules = Rules(vec![Strategy::Capacity(rule.clone())]);

    let mut conn = state.pool.get().await.unwrap();

    // Holder: charge = 500 → slot used = 500, capacity_charge = 500, keys persisted.
    let t1 = task_for_claim(id1, "cap-limit", json!({"p": 1}), rules.clone(), Some(500));
    assert_eq!(
        claim_task_with_rules(&mut conn, &t1).await.unwrap(),
        ClaimResult::Claimed
    );
    assert_eq!(slot_used(&state.pool, &key).await, Some(500));
    assert_eq!(capacity_charge_of(&state.pool, id1).await, Some(500));
    assert_eq!(
        claimed_keys(&state.pool, id1).await,
        Some(vec![key.clone()])
    );

    // Candidate: used = 500 >= max 500 → blocked. Slot + candidate unchanged.
    let t2 = task_for_claim(id2, "cap-limit", json!({"p": 1}), rules.clone(), Some(100));
    assert_eq!(
        claim_task_with_rules(&mut conn, &t2).await.unwrap(),
        ClaimResult::RuleBlocked
    );
    assert_eq!(slot_used(&state.pool, &key).await, Some(500));
    assert_eq!(capacity_charge_of(&state.pool, id2).await, None);
    assert_eq!(claimed_keys(&state.pool, id2).await, None);
    assert_eq!(task_status_text(&state.pool, id2).await, "pending");
}

// ---------------------------------------------------------------------------
// 2. Flush frees capacity
// ---------------------------------------------------------------------------

/// Holder Running with expected=100, max=100 → slot full → candidate blocked. Pushing
/// counter progress through the REAL flush path shrinks the charge and the slot, so the
/// candidate then claims.
#[tokio::test]
async fn test_capacity_slot_flush_frees_capacity() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let created = create_tasks_ok(
        &app,
        &[
            cap_task_json("fhold", "cap-flush", json!({"p": 1}), 100, 100, &[]),
            cap_task_json("fcand", "cap-flush", json!({"p": 1}), 40, 100, &[]),
        ],
    )
    .await;
    let (id1, id2) = (created[0].id, created[1].id);
    let rule = cap_rule("cap-flush", 100, &[]);
    let key = cap_slot_key(&rule, &json!({"p": 1}));
    let rules = Rules(vec![Strategy::Capacity(rule.clone())]);

    let mut conn = state.pool.get().await.unwrap();

    // Holder claims (charge 100) then runs; slot full.
    let t1 = task_for_claim(id1, "cap-flush", json!({"p": 1}), rules.clone(), Some(100));
    assert_eq!(
        claim_task_with_rules(&mut conn, &t1).await.unwrap(),
        ClaimResult::Claimed
    );
    arcrun::db_operation::mark_task_running(&mut conn, &id1)
        .await
        .unwrap();
    assert_eq!(slot_used(&state.pool, &key).await, Some(100));

    // Candidate blocked at the limit.
    let t2 = task_for_claim(id2, "cap-flush", json!({"p": 1}), rules.clone(), Some(40));
    assert_eq!(
        claim_task_with_rules(&mut conn, &t2).await.unwrap(),
        ClaimResult::RuleBlocked
    );

    // Flush +60 success through the real batch-updater flush path → remaining 40,
    // capacity_charge 100 -> 40, slot 100 -> 40.
    arcrun::workers::run_counter_flush_once(&mut conn, &[(id1, 60, 0)])
        .await
        .unwrap();
    assert_eq!(capacity_charge_of(&state.pool, id1).await, Some(40));
    assert_eq!(slot_used(&state.pool, &key).await, Some(40));

    // Candidate (charge 40): used 40 < 100 → claims, slot 40 -> 80.
    assert_eq!(
        claim_task_with_rules(&mut conn, &t2).await.unwrap(),
        ClaimResult::Claimed
    );
    assert_eq!(slot_used(&state.pool, &key).await, Some(80));
    assert_eq!(capacity_charge_of(&state.pool, id2).await, Some(40));
}

/// A flush after further progress is monotone and idempotent on the slot: a second flush
/// that adds counters again only releases the incremental shrink; a flush with no real
/// progress is a no-op on the slot.
#[tokio::test]
async fn test_capacity_slot_flush_is_monotone() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let created = create_tasks_ok(
        &app,
        &[cap_task_json(
            "mono",
            "cap-mono",
            json!({"p": 1}),
            100,
            100,
            &[],
        )],
    )
    .await;
    let id = created[0].id;
    let rule = cap_rule("cap-mono", 100, &[]);
    let key = cap_slot_key(&rule, &json!({"p": 1}));
    let rules = Rules(vec![Strategy::Capacity(rule.clone())]);

    let mut conn = state.pool.get().await.unwrap();
    let t = task_for_claim(id, "cap-mono", json!({"p": 1}), rules.clone(), Some(100));
    claim_task_with_rules(&mut conn, &t).await.unwrap();
    arcrun::db_operation::mark_task_running(&mut conn, &id)
        .await
        .unwrap();
    assert_eq!(slot_used(&state.pool, &key).await, Some(100));

    // First flush: +30 → charge 70, slot 70.
    arcrun::workers::run_counter_flush_once(&mut conn, &[(id, 30, 0)])
        .await
        .unwrap();
    assert_eq!(slot_used(&state.pool, &key).await, Some(70));

    // Second flush: +20 failures → remaining 50, charge 50, slot 50 (only the -20 delta).
    arcrun::workers::run_counter_flush_once(&mut conn, &[(id, 0, 20)])
        .await
        .unwrap();
    assert_eq!(capacity_charge_of(&state.pool, id).await, Some(50));
    assert_eq!(slot_used(&state.pool, &key).await, Some(50));
}

// ---------------------------------------------------------------------------
// 3. Terminal release
// ---------------------------------------------------------------------------

/// A holder PATCHed to Success releases its outstanding charge: slot drops by the
/// remaining charge, `capacity_charge` NULL, keys NULL; a candidate then claims.
#[tokio::test]
async fn test_capacity_slot_terminal_release() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let created = create_tasks_ok(
        &app,
        &[
            cap_task_json("thold", "cap-term", json!({"p": 1}), 500, 500, &[]),
            cap_task_json("tcand", "cap-term", json!({"p": 1}), 300, 500, &[]),
        ],
    )
    .await;
    let (id1, id2) = (created[0].id, created[1].id);
    let rule = cap_rule("cap-term", 500, &[]);
    let key = cap_slot_key(&rule, &json!({"p": 1}));
    let rules = Rules(vec![Strategy::Capacity(rule.clone())]);

    let mut conn = state.pool.get().await.unwrap();
    let t1 = task_for_claim(id1, "cap-term", json!({"p": 1}), rules.clone(), Some(500));
    claim_task_with_rules(&mut conn, &t1).await.unwrap();
    arcrun::db_operation::mark_task_running(&mut conn, &id1)
        .await
        .unwrap();
    assert_eq!(slot_used(&state.pool, &key).await, Some(500));

    // Candidate blocked while holder occupies the full slot.
    let t2 = task_for_claim(id2, "cap-term", json!({"p": 1}), rules.clone(), Some(300));
    assert_eq!(
        claim_task_with_rules(&mut conn, &t2).await.unwrap(),
        ClaimResult::RuleBlocked
    );

    // Complete the holder → release the outstanding 500.
    assert_eq!(
        arcrun::db_operation::update_running_task(&mut conn, id1, success_dto(), true)
            .await
            .unwrap(),
        arcrun::db_operation::UpdateTaskResult::Updated
    );
    assert_eq!(slot_used(&state.pool, &key).await, Some(0));
    assert_eq!(capacity_charge_of(&state.pool, id1).await, None);
    assert_eq!(claimed_keys(&state.pool, id1).await, None);

    // Candidate now claims (slot 0 -> 300).
    assert_eq!(
        claim_task_with_rules(&mut conn, &t2).await.unwrap(),
        ClaimResult::Claimed
    );
    assert_eq!(slot_used(&state.pool, &key).await, Some(300));
}

/// A terminal release AFTER a partial flush releases only the *remaining* charge, so the
/// slot returns exactly to 0 (flush delta + release = original charge, no leak/underflow).
#[tokio::test]
async fn test_capacity_slot_partial_flush_then_release_zeroes() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let created = create_tasks_ok(
        &app,
        &[cap_task_json(
            "pr",
            "cap-pr",
            json!({"p": 1}),
            100,
            100,
            &[],
        )],
    )
    .await;
    let id = created[0].id;
    let rule = cap_rule("cap-pr", 100, &[]);
    let key = cap_slot_key(&rule, &json!({"p": 1}));
    let rules = Rules(vec![Strategy::Capacity(rule.clone())]);

    let mut conn = state.pool.get().await.unwrap();
    let t = task_for_claim(id, "cap-pr", json!({"p": 1}), rules.clone(), Some(100));
    claim_task_with_rules(&mut conn, &t).await.unwrap();
    arcrun::db_operation::mark_task_running(&mut conn, &id)
        .await
        .unwrap();

    // Flush 70 progress → slot 30, charge 30.
    arcrun::workers::run_counter_flush_once(&mut conn, &[(id, 70, 0)])
        .await
        .unwrap();
    assert_eq!(slot_used(&state.pool, &key).await, Some(30));

    // Complete → release remaining 30 → slot back to exactly 0.
    arcrun::db_operation::update_running_task(&mut conn, id, success_dto(), true)
        .await
        .unwrap();
    assert_eq!(slot_used(&state.pool, &key).await, Some(0));
    assert_eq!(capacity_charge_of(&state.pool, id).await, None);
}

// ---------------------------------------------------------------------------
// 4. Requeue-stale releases charge
// ---------------------------------------------------------------------------

/// A stale Claimed capacity task is requeued to Pending; its charge is released and both
/// `capacity_charge` and `claimed_slot_keys` are NULLed (so a later re-claim recharges
/// cleanly).
#[tokio::test]
async fn test_capacity_slot_released_on_requeue_stale() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let created = create_tasks_ok(
        &app,
        &[cap_task_json(
            "rq",
            "cap-rq",
            json!({"p": 1}),
            200,
            500,
            &[],
        )],
    )
    .await;
    let id = created[0].id;
    let rule = cap_rule("cap-rq", 500, &[]);
    let key = cap_slot_key(&rule, &json!({"p": 1}));
    let rules = Rules(vec![Strategy::Capacity(rule.clone())]);

    {
        let mut conn = state.pool.get().await.unwrap();
        let t = task_for_claim(id, "cap-rq", json!({"p": 1}), rules.clone(), Some(200));
        claim_task_with_rules(&mut conn, &t).await.unwrap();
    }
    assert_eq!(slot_used(&state.pool, &key).await, Some(200));
    assert_eq!(task_status_text(&state.pool, id).await, "claimed");

    // Make it stale so the requeue loop moves it back to Pending.
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
    assert_eq!(capacity_charge_of(&state.pool, id).await, None);
    assert_eq!(
        claimed_keys(&state.pool, id).await,
        None,
        "requeue must NULL claimed_slot_keys"
    );
}

// ---------------------------------------------------------------------------
// 5. Semantic change: matcher-match without the rule does not block
// ---------------------------------------------------------------------------

/// A Running task that merely MATCHES the capacity matcher (same kind/metadata) but was
/// claimed WITHOUT the rule contributes nothing to the slot — so a capacity candidate is
/// admitted even though the old CTE-SUM probe (which counted all matcher-matching Running
/// tasks) would have blocked it.
#[tokio::test]
async fn test_capacity_slot_matcher_match_without_rule_does_not_block() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let created = create_tasks_ok(
        &app,
        &[
            // Rule-less holder with a large expected_count (matches the matcher).
            plain_task_json("shold", "cap-sem", json!({"p": 1}), 1000),
            cap_task_json("scand", "cap-sem", json!({"p": 1}), 100, 500, &[]),
        ],
    )
    .await;
    let (id1, id2) = (created[0].id, created[1].id);
    let rule = cap_rule("cap-sem", 500, &[]);
    let key = cap_slot_key(&rule, &json!({"p": 1}));
    let cap_rules = Rules(vec![Strategy::Capacity(rule.clone())]);

    let mut conn = state.pool.get().await.unwrap();

    // Holder claims rule-less → occupies NO capacity slot.
    let t1 = task_for_claim(id1, "cap-sem", json!({"p": 1}), Rules(vec![]), Some(1000));
    claim_task_with_rules(&mut conn, &t1).await.unwrap();
    arcrun::db_operation::mark_task_running(&mut conn, &id1)
        .await
        .unwrap();
    assert_eq!(
        slot_used(&state.pool, &key).await,
        None,
        "rule-less holder must not create/charge the cap slot"
    );

    // Candidate (charge 100): fresh slot, 100 < 500 → claims. Old semantics would block
    // (holder's remaining 1000 >= 500).
    let t2 = task_for_claim(
        id2,
        "cap-sem",
        json!({"p": 1}),
        cap_rules.clone(),
        Some(100),
    );
    assert_eq!(
        claim_task_with_rules(&mut conn, &t2).await.unwrap(),
        ClaimResult::Claimed
    );
    assert_eq!(slot_used(&state.pool, &key).await, Some(100));
}

// ---------------------------------------------------------------------------
// 6. Guards: missing expected_count, and max_capacity <= 0
// ---------------------------------------------------------------------------

/// A capacity candidate with no `expected_count` is blocked, and no slot row is created.
/// A capacity candidate whose `max_capacity <= 0` is blocked by the Rust-side guard
/// (the fresh-INSERT upsert arm would otherwise admit it) — again no slot row is created.
#[tokio::test]
async fn test_capacity_slot_guards_block() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // A plain row we can claim against with an injected rule.
    let created = create_tasks_ok(
        &app,
        &[
            plain_task_json("g1", "cap-guard", json!({"p": 1}), 0),
            plain_task_json("g2", "cap-guard-zero", json!({"p": 1}), 0),
        ],
    )
    .await;
    let (id1, id2) = (created[0].id, created[1].id);

    let mut conn = state.pool.get().await.unwrap();

    // (a) Missing expected_count → RuleBlocked, no slot.
    let rule = cap_rule("cap-guard", 500, &[]);
    let key = cap_slot_key(&rule, &json!({"p": 1}));
    let t1 = task_for_claim(
        id1,
        "cap-guard",
        json!({"p": 1}),
        Rules(vec![Strategy::Capacity(rule.clone())]),
        None,
    );
    assert_eq!(
        claim_task_with_rules(&mut conn, &t1).await.unwrap(),
        ClaimResult::RuleBlocked
    );
    assert_eq!(slot_used(&state.pool, &key).await, None);
    assert_eq!(task_status_text(&state.pool, id1).await, "pending");

    // (b) max_capacity = 0 with a valid expected_count → blocked by the new guard, no slot.
    let rule0 = cap_rule("cap-guard-zero", 0, &[]);
    let key0 = cap_slot_key(&rule0, &json!({"p": 1}));
    let t2 = task_for_claim(
        id2,
        "cap-guard-zero",
        json!({"p": 1}),
        Rules(vec![Strategy::Capacity(rule0.clone())]),
        Some(10),
    );
    assert_eq!(
        claim_task_with_rules(&mut conn, &t2).await.unwrap(),
        ClaimResult::RuleBlocked
    );
    assert_eq!(slot_used(&state.pool, &key0).await, None);
    assert_eq!(task_status_text(&state.pool, id2).await, "pending");
}

// ---------------------------------------------------------------------------
// 7. Mixed rules: Concurrency + Capacity on one task
// ---------------------------------------------------------------------------

/// A task carrying BOTH a Concurrency and a Capacity rule consumes both slots at claim
/// (both keys persisted, capacity_charge set); a terminal release frees both.
#[tokio::test]
async fn test_capacity_slot_mixed_concurrency_and_capacity() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let combined = json!([
        { "type": "Concurency", "max_concurency": 2,
          "matcher": { "kind": "cap-mix", "status": "Running", "fields": [] } },
        { "type": "Capacity", "max_capacity": 500,
          "matcher": { "kind": "cap-mix", "status": "Running", "fields": [] } }
    ]);
    let created = create_tasks_ok(
        &app,
        &[json!({
            "id": "mix", "name": "mix", "kind": "cap-mix", "timeout": 60,
            "expected_count": 100, "metadata": {"p": 1},
            "on_start": webhook_action(), "rules": combined
        })],
    )
    .await;
    let id = created[0].id;

    let crule = conc_rule("cap-mix", 2, &[]);
    let caprule = cap_rule("cap-mix", 500, &[]);
    let conc_key = conc_slot_key(&crule, &json!({"p": 1}));
    let cap_key = cap_slot_key(&caprule, &json!({"p": 1}));
    let rules = Rules(vec![
        Strategy::Concurency(crule.clone()),
        Strategy::Capacity(caprule.clone()),
    ]);

    let mut conn = state.pool.get().await.unwrap();
    let t = task_for_claim(id, "cap-mix", json!({"p": 1}), rules.clone(), Some(100));
    assert_eq!(
        claim_task_with_rules(&mut conn, &t).await.unwrap(),
        ClaimResult::Claimed
    );

    // Both slots consumed: conc +1, cap +charge(100). capacity_charge set. Both keys
    // persisted (sorted: cap: before conc:).
    assert_eq!(slot_used(&state.pool, &conc_key).await, Some(1));
    assert_eq!(slot_used(&state.pool, &cap_key).await, Some(100));
    assert_eq!(capacity_charge_of(&state.pool, id).await, Some(100));
    let mut keys = claimed_keys(&state.pool, id).await.unwrap();
    keys.sort();
    let mut expected = vec![cap_key.clone(), conc_key.clone()];
    expected.sort();
    assert_eq!(keys, expected);

    // Terminal release frees both.
    arcrun::db_operation::mark_task_running(&mut conn, &id)
        .await
        .unwrap();
    arcrun::db_operation::update_running_task(&mut conn, id, success_dto(), true)
        .await
        .unwrap();
    assert_eq!(slot_used(&state.pool, &conc_key).await, Some(0));
    assert_eq!(slot_used(&state.pool, &cap_key).await, Some(0));
    assert_eq!(capacity_charge_of(&state.pool, id).await, None);
    assert_eq!(claimed_keys(&state.pool, id).await, None);
}
