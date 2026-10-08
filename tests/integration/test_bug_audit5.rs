// Diesel's generated QueryableByName impls need this at module scope.
#![allow(clippy::redundant_field_names)]

//! Bounded admission, streaming results, and recovery of delivery acknowledgements.

use crate::common::*;
use arcrun::{
    DbPool,
    handlers::AppState,
    workers::{DeliveryConfig, WorkerNudges},
};
use diesel_async::{
    AsyncPgConnection, RunQueryDsl,
    pooled_connection::{AsyncDieselConnectionManager, bb8::Pool},
};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
};

struct Call {
    key: String,
    respond: oneshot::Sender<u16>,
}

// Each request remains in flight until the test explicitly responds. This makes
// the slow/fast ordering deterministic, without depending on machine speed.
async fn gated_server() -> (String, mpsc::UnboundedReceiver<Call>, oneshot::Sender<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/webhook", listener.local_addr().unwrap());
    let (calls, rx) = mpsc::unbounded_channel();
    let (stop, mut shutdown) = oneshot::channel();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut shutdown => break,
                accepted = listener.accept() => {
                    let (mut socket, _) = accepted.unwrap();
                    let calls = calls.clone();
                    tokio::spawn(async move {
                        let mut data = Vec::new();
                        let mut buf = [0; 4096];
                        loop {
                            let n = socket.read(&mut buf).await.unwrap();
                            if n == 0 { return; }
                            data.extend_from_slice(&buf[..n]);
                            if let Some(end) = data.windows(4).position(|w| w == b"\r\n\r\n") {
                                let headers = String::from_utf8_lossy(&data[..end]);
                                let length: usize = headers.lines().filter_map(|line| line.split_once(':'))
                                    .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
                                    .map(|(_, v)| v.trim().parse().unwrap()).unwrap_or(0);
                                if data.len() >= end + 4 + length { break; }
                            }
                        }
                        let headers = String::from_utf8_lossy(&data);
                        let key = headers.lines().filter_map(|line| line.split_once(':'))
                            .find(|(k, _)| k.eq_ignore_ascii_case("idempotency-key"))
                            .unwrap().1.trim().to_string();
                        let (respond, response) = oneshot::channel();
                        if calls.send(Call { key, respond }).is_err() { return; }
                        if let Ok(status) = response.await {
                            let reply = format!("HTTP/1.1 {status} Test\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                            let _ = socket.write_all(reply.as_bytes()).await;
                        }
                    });
                }
            }
        }
    });
    (url, rx, stop)
}

async fn next_call(calls: &mut mpsc::UnboundedReceiver<Call>) -> Call {
    tokio::time::timeout(Duration::from_secs(5), calls.recv())
        .await
        .expect("a free worker must take the next event without waiting for the slow one")
        .expect("mock server closed")
}

#[derive(diesel::QueryableByName)]
struct Count {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    n: i64,
}

async fn count(pool: &DbPool, query: &str) -> i64 {
    let mut conn = pool.get().await.unwrap();
    diesel::sql_query(query)
        .get_result::<Count>(&mut conn)
        .await
        .unwrap()
        .n
}

async fn wait_count(pool: &DbPool, query: &str, expected: i64) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if count(pool, query).await == expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("database result was not recorded promptly");
}

const SUCCESSES: &str =
    "SELECT count(*) AS n FROM webhook_execution WHERE trigger='end' AND status='success'";
const RUNNING: &str = "SELECT count(*) AS n FROM task WHERE status='running'";
const PENDING: &str = "SELECT count(*) AS n FROM task WHERE status='pending'";

fn tasks(url: &str, n: usize, delivery: bool) -> Vec<serde_json::Value> {
    (0..n)
        .map(|i| {
            let mut t = task_json(&i.to_string(), &i.to_string(), "streaming");
            t["priority"] = json!(n - i);
            let action = json!({"kind":"Webhook", "params":{"url":url, "verb":"Post"}});
            if delivery {
                t["on_success"] = json!([action]);
            } else {
                t["on_start"] = action;
            }
            t
        })
        .collect()
}

fn delivery_worker(state: &AppState, cfg: DeliveryConfig) -> (watch::Sender<bool>, JoinHandle<()>) {
    let (stop, rx) = watch::channel(false);
    let worker = tokio::spawn(arcrun::workers::delivery_loop(
        Arc::new(state.action_executor.clone()),
        state.pool.clone(),
        Duration::from_millis(20),
        cfg,
        rx,
        WorkerNudges::new(),
    ));
    (stop, worker)
}

fn start_worker(
    state: &AppState,
    batch: i64,
    concurrency: usize,
) -> (watch::Sender<bool>, JoinHandle<()>) {
    let (stop, rx) = watch::channel(false);
    let evaluator = state.action_executor.clone();
    let pool = state.pool.clone();
    let worker = tokio::spawn(async move {
        arcrun::workers::start_loop(
            &evaluator,
            pool,
            Duration::from_millis(20),
            false,
            batch,
            concurrency,
            rx,
            WorkerNudges::new(),
            Duration::from_secs(30),
        )
        .await;
    });
    (stop, worker)
}

async fn joined(worker: JoinHandle<()>) {
    tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .unwrap()
        .unwrap();
}

/// Bug 6: a slow event blocked acknowledgement and admission of the next batch.
/// Fast results must become durable and refill the free slot while the slow HTTP
/// request is still in flight; at most two events may be admitted concurrently.
#[tokio::test]
async fn test_bug6_delivery_records_and_refills_before_slow_event_finishes() {
    let (_guard, state) = setup_test_app().await;
    let app = test_service!(state);
    let (url, mut calls, _server) = gated_server().await;
    for t in create_tasks_ok(&app, &tasks(&url, 3, true)).await {
        succeed_task(&state, t.id).await;
    }
    let (stop, worker) = delivery_worker(
        &state,
        DeliveryConfig {
            batch_size: 2,
            concurrency: 2,
            ..default_delivery_cfg()
        },
    );
    let slow = next_call(&mut calls).await;
    let fast = next_call(&mut calls).await;
    assert!(calls.try_recv().is_err());
    fast.respond.send(200).unwrap();
    wait_count(&state.pool, SUCCESSES, 1).await;
    let next = next_call(&mut calls).await;
    next.respond.send(200).unwrap();
    wait_count(&state.pool, SUCCESSES, 2).await;
    stop.send(true).unwrap();
    slow.respond.send(200).unwrap();
    joined(worker).await;
    assert_eq!(count(&state.pool, SUCCESSES).await, 3);
    assert!(calls.try_recv().is_err());
}

/// Bug 6: start batches waited for their slowest on_start before admitting more.
/// A third task must reach Running before the first slow request is released.
#[tokio::test]
async fn test_bug6_start_refills_before_slow_event_finishes() {
    let (_guard, state) = setup_test_app().await;
    let app = test_service!(state);
    let (url, mut calls, _server) = gated_server().await;
    create_tasks_ok(&app, &tasks(&url, 3, false)).await;
    let (stop, worker) = start_worker(&state, 2, 2);
    let slow = next_call(&mut calls).await;
    next_call(&mut calls).await.respond.send(200).unwrap();
    let next = next_call(&mut calls).await;
    next.respond.send(200).unwrap();
    wait_count(&state.pool, RUNNING, 2).await;
    stop.send(true).unwrap();
    slow.respond.send(200).unwrap();
    joined(worker).await;
    assert_eq!(count(&state.pool, RUNNING).await, 3);
}

/// Bug 4: a whole batch was Claimed although only the first semaphore waiter
/// received heartbeats. Bound claims to free slots instead; queued tasks stay
/// Pending, occupy no quota and cannot expire as abandoned claims. Shutdown must
/// finish the admitted request without starting any of the backlog.
#[tokio::test]
async fn test_bug4_start_only_claims_available_slots_and_drains_on_shutdown() {
    let (_guard, state) = setup_test_app().await;
    let app = test_service!(state);
    let (url, mut calls, _server) = gated_server().await;
    let mut input = tasks(&url, 4, false);
    for task in &mut input {
        task["rules"] = json!([{"type":"Concurency", "max_concurency":4,
            "matcher":{"kind":"streaming", "status":"Running", "fields":[]}}]);
    }
    create_tasks_ok(&app, &input).await;
    let (stop, worker) = start_worker(&state, 50, 1);
    let active = next_call(&mut calls).await;
    assert_eq!(count(&state.pool, PENDING).await, 3);
    assert_eq!(
        count(
            &state.pool,
            "SELECT coalesce(sum(used),0)::bigint AS n FROM rule_slot"
        )
        .await,
        1
    );
    stop.send(true).unwrap();
    active.respond.send(200).unwrap();
    joined(worker).await;
    assert_eq!(count(&state.pool, RUNNING).await, 1);
    assert_eq!(count(&state.pool, PENDING).await, 3);
    // Finishing on_start frees an HTTP slot, not the task's business reservation.
    assert_eq!(
        count(
            &state.pool,
            "SELECT coalesce(sum(used),0)::bigint AS n FROM rule_slot"
        )
        .await,
        1
    );
    assert!(calls.try_recv().is_err());
}

async fn short_timeout_state() -> (TestApp, AppState) {
    let (db, mut state) = setup_test_app().await;
    state.pool = Pool::builder()
        .max_size(1)
        .connection_timeout(Duration::from_millis(100))
        .build(AsyncDieselConnectionManager::<AsyncPgConnection>::new(
            &db.url,
        ))
        .await
        .unwrap();
    (db, state)
}

/// Bug 6: a transient failure to reacquire a pooled connection discarded a
/// successful HTTP result. Retain it within the concurrency bound and retry only
/// its fenced database mark, before lease expiry and without repeating HTTP.
#[tokio::test]
async fn test_bug6_delivery_keeps_success_across_pool_timeout() {
    let (_db, state) = short_timeout_state().await;
    let app = test_service!(state);
    let (url, mut calls, _server) = gated_server().await;
    for t in create_tasks_ok(&app, &tasks(&url, 1, true)).await {
        succeed_task(&state, t.id).await;
    }
    let (stop, worker) = delivery_worker(
        &state,
        DeliveryConfig {
            concurrency: 1,
            ..default_delivery_cfg()
        },
    );
    let call = next_call(&mut calls).await;
    let held = state.pool.get().await.unwrap();
    call.respond.send(200).unwrap();
    tokio::time::sleep(Duration::from_millis(350)).await;
    drop(held);
    wait_count(&state.pool, SUCCESSES, 1).await;
    stop.send(true).unwrap();
    joined(worker).await;
    assert!(
        calls.try_recv().is_err(),
        "only the DB mark should have been retried"
    );
}

/// Bug 6 recovery boundary: process death can lose an in-memory success but must
/// not lose the durable event. After lease expiry a new worker repeats the same
/// Idempotency-Key and records exactly one history row (at-least-once delivery).
#[tokio::test]
async fn test_bug6_delivery_crash_before_ack_recovers_with_same_key() {
    let (db, state) = short_timeout_state().await;
    let app = test_service!(state);
    let (url, mut calls, _server) = gated_server().await;
    for t in create_tasks_ok(&app, &tasks(&url, 1, true)).await {
        succeed_task(&state, t.id).await;
    }
    let (_stop, worker) = delivery_worker(&state, default_delivery_cfg());
    let first = next_call(&mut calls).await;
    let held = state.pool.get().await.unwrap();
    first.respond.send(200).unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    worker.abort();
    assert!(worker.await.unwrap_err().is_cancelled());
    drop(held);
    assert_eq!(count(&db.pool, SUCCESSES).await, 0);
    {
        let mut conn = db.pool.get().await.unwrap();
        diesel::sql_query("UPDATE webhook_outbox SET next_attempt_at=now()-interval '1 second'")
            .execute(&mut conn)
            .await
            .unwrap();
    }
    let (stop, worker) = delivery_worker(&state, default_delivery_cfg());
    let second = next_call(&mut calls).await;
    assert_eq!(first.key, second.key);
    second.respond.send(200).unwrap();
    wait_count(&db.pool, SUCCESSES, 1).await;
    stop.send(true).unwrap();
    joined(worker).await;
    assert!(calls.try_recv().is_err());
}

/// Bug 6: continuous refill must stop admitting rows once shutdown is signalled,
/// while committing outcomes of requests already sent. Backlog remains unleased.
#[tokio::test]
async fn test_bug6_delivery_shutdown_drains_only_admitted_events() {
    let (_guard, state) = setup_test_app().await;
    let app = test_service!(state);
    let (url, mut calls, _server) = gated_server().await;
    for t in create_tasks_ok(&app, &tasks(&url, 4, true)).await {
        succeed_task(&state, t.id).await;
    }
    let (stop, worker) = delivery_worker(
        &state,
        DeliveryConfig {
            batch_size: 50,
            concurrency: 2,
            ..default_delivery_cfg()
        },
    );
    let a = next_call(&mut calls).await;
    let b = next_call(&mut calls).await;
    stop.send(true).unwrap();
    a.respond.send(200).unwrap();
    b.respond.send(200).unwrap();
    joined(worker).await;
    assert_eq!(count(&state.pool, SUCCESSES).await, 2);
    assert_eq!(
        count(
            &state.pool,
            "SELECT count(*) AS n FROM webhook_outbox WHERE lease_token IS NULL"
        )
        .await,
        2
    );
    assert!(calls.try_recv().is_err());
}

/// Bug 6: retaining a result for later persistence must not let an old lease
/// overwrite a newer delivery. The new owner succeeds while the old request is
/// blocked; its later HTTP failure must not schedule a retry or change history.
#[tokio::test]
async fn test_bug6_late_result_cannot_overwrite_new_lease_owner() {
    let (_guard, state) = setup_test_app().await;
    let app = test_service!(state);
    let (url, mut calls, _server) = gated_server().await;
    for t in create_tasks_ok(&app, &tasks(&url, 1, true)).await {
        succeed_task(&state, t.id).await;
    }
    let cfg = DeliveryConfig {
        concurrency: 1,
        ..default_delivery_cfg()
    };
    let (stop_old, old) = delivery_worker(&state, cfg);
    let first = next_call(&mut calls).await;
    {
        let mut conn = state.pool.get().await.unwrap();
        diesel::sql_query("UPDATE webhook_outbox SET next_attempt_at=now()-interval '1 second'")
            .execute(&mut conn)
            .await
            .unwrap();
    }
    let (stop_new, new) = delivery_worker(&state, cfg);
    let second = next_call(&mut calls).await;
    assert_eq!(first.key, second.key);
    second.respond.send(200).unwrap();
    wait_count(&state.pool, SUCCESSES, 1).await;
    stop_new.send(true).unwrap();
    joined(new).await;
    stop_old.send(true).unwrap();
    first.respond.send(500).unwrap();
    joined(old).await;
    assert_eq!(
        count(&state.pool, "SELECT count(*) AS n FROM webhook_outbox").await,
        0
    );
    assert_eq!(count(&state.pool, SUCCESSES).await, 1);
    assert_eq!(
        count(
            &state.pool,
            "SELECT sum(attempts)::bigint AS n FROM webhook_execution WHERE trigger='end'"
        )
        .await,
        0
    );
}

/// Bug 6: an HTTP slot becoming available must not release a Running task's
/// business quota. A second task with a concurrency limit of one stays Pending
/// until the first task actually finishes, then starts without waiting for a lot.
#[tokio::test]
async fn test_bug6_refill_preserves_business_concurrency_reservations() {
    let (_guard, state) = setup_test_app().await;
    let app = test_service!(state);
    let (url, mut calls, _server) = gated_server().await;
    let mut input = tasks(&url, 2, false);
    for task in &mut input {
        task["rules"] = json!([{"type":"Concurency", "max_concurency":1,
            "matcher":{"kind":"streaming", "status":"Running", "fields":[]}}]);
    }
    let created = create_tasks_ok(&app, &input).await;
    let (stop, worker) = start_worker(&state, 50, 2);
    let first = next_call(&mut calls).await;
    let first_id = created
        .iter()
        .find(|t| first.key.starts_with(&t.id.to_string()))
        .unwrap()
        .id;
    first.respond.send(200).unwrap();
    wait_count(&state.pool, RUNNING, 1).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(150), calls.recv())
            .await
            .is_err()
    );
    assert_eq!(count(&state.pool, PENDING).await, 1);
    let req = actix_web::test::TestRequest::patch()
        .uri(&format!("/task/{first_id}"))
        .set_json(json!({"status":"Success"}))
        .to_request();
    assert!(
        actix_web::test::call_service(&app, req)
            .await
            .status()
            .is_success()
    );
    next_call(&mut calls).await.respond.send(200).unwrap();
    wait_count(&state.pool, RUNNING, 1).await;
    stop.send(true).unwrap();
    joined(worker).await;
    assert_eq!(count(&state.pool, PENDING).await, 0);
}

/// Bug 6: a failed acknowledgement statement used to be logged and discarded.
/// Inject a real PostgreSQL error after HTTP success, then remove it: the worker
/// must retry the mark, without repeating HTTP or waiting for lease expiry.
#[tokio::test]
async fn test_bug6_delivery_keeps_success_across_mark_error() {
    let (_guard, state) = setup_test_app().await;
    let app = test_service!(state);
    let (url, mut calls, _server) = gated_server().await;
    for t in create_tasks_ok(&app, &tasks(&url, 1, true)).await {
        succeed_task(&state, t.id).await;
    }
    {
        let mut conn = state.pool.get().await.unwrap();
        // Sequence increments survive the failing statement's rollback, so the
        // test observes the actual failure instead of relying on a fixed sleep.
        for sql in [
            "CREATE SEQUENCE audit_mark_attempts",
            "CREATE FUNCTION reject_audit_mark() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM nextval('audit_mark_attempts'); RAISE EXCEPTION 'injected mark failure'; END $$",
            "CREATE TRIGGER reject_audit_mark BEFORE DELETE ON webhook_outbox FOR EACH ROW EXECUTE FUNCTION reject_audit_mark()",
        ] {
            diesel::sql_query(sql).execute(&mut conn).await.unwrap();
        }
    }
    let (stop, worker) = delivery_worker(&state, default_delivery_cfg());
    next_call(&mut calls).await.respond.send(200).unwrap();
    wait_count(
        &state.pool,
        "SELECT is_called::integer::bigint AS n FROM audit_mark_attempts",
        1,
    )
    .await;
    assert_eq!(count(&state.pool, SUCCESSES).await, 0);
    {
        let mut conn = state.pool.get().await.unwrap();
        diesel::sql_query("DROP TRIGGER reject_audit_mark ON webhook_outbox")
            .execute(&mut conn)
            .await
            .unwrap();
    }
    wait_count(&state.pool, SUCCESSES, 1).await;
    stop.send(true).unwrap();
    joined(worker).await;
    assert!(calls.try_recv().is_err());
}
