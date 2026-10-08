// Diesel QueryableByName emits separate impls with explicit field initializers;
// an allow on the struct does not cover those generated impls.
#![allow(clippy::redundant_field_names)]

//! Lot 2 follow-up — lease-based claim + parallel out-of-transaction delivery.
//!
//! `run_delivery_once` now claims mature outbox rows in a short transaction (pushing
//! `next_attempt_at` a *lease* into the future), prefetches their inputs out of the
//! lock, delivers the HTTP in parallel (`buffer_unordered`), and posts the marks in
//! short autocommit statements. These tests cover the lease (anti double-claim +
//! expiry), the parallelism, and the per-row independence of marks.

use crate::common::*;

use arcrun::handlers::AppState;
use arcrun::workers::DeliveryConfig;
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

fn lease_cfg() -> DeliveryConfig {
    DeliveryConfig {
        batch_size: 100,
        max_attempts: 10,
        backoff_base_secs: 1,
        backoff_cap_secs: 1,
        lease_secs: 120,
        concurrency: 10,
        start_stale_secs: 30,
    }
}

/// Count outbox rows for a task in a given status.
async fn outbox_count(pool: &arcrun::DbPool, task_id: uuid::Uuid, status: &str) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Cnt {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        c: i64,
    }
    let mut conn = pool.get().await.unwrap();
    // D3: pending end/cancel rows live in the dedicated `webhook_outbox` queue; delivered
    // (success/failure/exhausted) rows are historised into `webhook_execution`.
    let r: Cnt = if status == "pending" {
        diesel_async::RunQueryDsl::get_result(
            diesel::sql_query("SELECT count(*) AS c FROM webhook_outbox WHERE task_id = $1")
                .bind::<diesel::sql_types::Uuid, _>(task_id),
            &mut *conn,
        )
        .await
        .unwrap()
    } else {
        diesel_async::RunQueryDsl::get_result(
            diesel::sql_query(
                "SELECT count(*) AS c FROM webhook_execution \
                 WHERE task_id = $1 AND status = $2::webhook_execution_status",
            )
            .bind::<diesel::sql_types::Uuid, _>(task_id)
            .bind::<diesel::sql_types::Text, _>(status),
            &mut *conn,
        )
        .await
        .unwrap()
    };
    r.c
}

/// Force every pending outbox row's `next_attempt_at` into the past (simulating an
/// expired lease) so it becomes mature again without waiting for the real lease.
async fn expire_leases(pool: &arcrun::DbPool) {
    let mut conn = pool.get().await.unwrap();
    diesel_async::RunQueryDsl::execute(
        // D3: the delivery queue is webhook_outbox now — every row is a pending attempt.
        diesel::sql_query("UPDATE webhook_outbox SET next_attempt_at = now() - interval '1 hour'"),
        &mut *conn,
    )
    .await
    .unwrap();
}

/// Create a single task whose `on_success` posts to `url`, then drive it to Success so
/// an `end:success` outbox row is enqueued. Returns the task id.
async fn task_with_success_webhook<S, B>(
    app: &S,
    state: &AppState,
    id: &str,
    url: &str,
) -> uuid::Uuid
where
    S: actix_web::dev::Service<
            actix_http::Request,
            Response = actix_web::dev::ServiceResponse<B>,
            Error = actix_web::Error,
        >,
    B: actix_web::body::MessageBody,
{
    let payload = json!({
        "id": id,
        "name": id,
        "kind": "lease-test",
        "timeout": 60,
        "metadata": {"test": true},
        "on_start": webhook_action(),
        "on_success": [{"kind": "Webhook", "params": {"url": url, "verb": "Post"}}]
    });
    let created = create_tasks_ok(app, &[payload]).await;
    let task_id = created[0].id;
    succeed_task(state, task_id).await;
    task_id
}

/// Lease anti double-claim: after one `claim_due_outbox_leased`, a second claim on a
/// fresh connection returns nothing for the leased rows (they are no longer mature).
#[tokio::test]
async fn test_lease_prevents_double_claim() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let hits = Arc::new(AtomicUsize::new(0));
    let (url, shutdown_server) = spawn_webhook_server(hits.clone());

    let task_id = task_with_success_webhook(&app, &state, "lease-1", &url).await;
    assert_eq!(outbox_count(&state.pool, task_id, "pending").await, 1);

    // First claim leases the row.
    let mut conn1 = state.pool.get().await.unwrap();
    let claimed1 = arcrun::db_operation::claim_due_outbox_leased(&mut conn1, 100, 120, 30)
        .await
        .unwrap();
    assert_eq!(
        claimed1.len(),
        1,
        "first claim should pick up the mature row"
    );
    drop(conn1);

    // Second claim on a fresh connection sees nothing (lease pushed maturity out).
    let mut conn2 = state.pool.get().await.unwrap();
    let claimed2 = arcrun::db_operation::claim_due_outbox_leased(&mut conn2, 100, 120, 30)
        .await
        .unwrap();
    assert_eq!(
        claimed2.len(),
        0,
        "second claim must not re-claim the leased row"
    );

    // The lease did NOT bump attempts (it is not an observed failure).
    assert_eq!(claimed1[0].attempts, 0, "lease must not increment attempts");

    let _ = shutdown_server.send(());
}

/// Lease expiration: a row claimed but never marked (simulated crash) becomes
/// deliverable again once its `next_attempt_at` passes, and delivery then succeeds.
#[tokio::test]
async fn test_lease_expires_and_redelivers() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let hits = Arc::new(AtomicUsize::new(0));
    let (url, shutdown_server) = spawn_webhook_server(hits.clone());

    let task_id = task_with_success_webhook(&app, &state, "lease-2", &url).await;

    // Claim (lease) the row but do NOT deliver or mark it — this simulates a crash
    // mid-delivery: the row stays pending, leased into the future.
    let mut conn = state.pool.get().await.unwrap();
    let claimed = arcrun::db_operation::claim_due_outbox_leased(&mut conn, 100, 120, 30)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    drop(conn);

    // Still pending, no HTTP yet.
    assert_eq!(outbox_count(&state.pool, task_id, "pending").await, 1);
    assert_eq!(
        hits.load(Ordering::SeqCst),
        0,
        "no delivery during the lease"
    );

    // Expire the lease (push next_attempt_at into the past) and run the loop: the row
    // is mature again and gets delivered.
    expire_leases(&state.pool).await;
    drain_outbox_with(&state, lease_cfg(), 5).await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "row should be redelivered after the lease expired"
    );
    assert_eq!(outbox_count(&state.pool, task_id, "success").await, 1);

    let _ = shutdown_server.send(());
}

/// A worker finishing after its lease expired must not be able to delete or reschedule
/// a row that a newer worker has reclaimed.
#[tokio::test]
async fn test_stale_lease_token_cannot_mark_reclaimed_row() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let hits = Arc::new(AtomicUsize::new(0));
    let (url, shutdown_server) = spawn_webhook_server(hits);
    let task_id = task_with_success_webhook(&app, &state, "lease-fence", &url).await;

    let mut conn = state.pool.get().await.unwrap();
    let first = arcrun::db_operation::claim_due_outbox_leased(&mut conn, 1, 120, 30)
        .await
        .unwrap();
    let old_token = first[0].lease_token.expect("claim assigns a lease token");
    drop(conn);

    expire_leases(&state.pool).await;
    let mut conn = state.pool.get().await.unwrap();
    let second = arcrun::db_operation::claim_due_outbox_leased(&mut conn, 1, 120, 30)
        .await
        .unwrap();
    let current_token = second[0]
        .lease_token
        .expect("reclaim assigns a lease token");
    assert_ne!(old_token, current_token);

    assert!(
        !arcrun::db_operation::mark_outbox_success(&mut conn, &first[0].idempotency_key, old_token)
            .await
            .unwrap(),
        "stale worker mark is fenced out"
    );
    assert_eq!(outbox_count(&state.pool, task_id, "pending").await, 1);
    assert!(
        arcrun::db_operation::mark_outbox_success(
            &mut conn,
            &second[0].idempotency_key,
            current_token,
        )
        .await
        .unwrap(),
        "current lease owner can mark the row"
    );
    drop(conn);
    assert_eq!(outbox_count(&state.pool, task_id, "success").await, 1);

    let _ = shutdown_server.send(());
}

/// Parallelism: N rows whose webhooks each take ~500ms must be delivered concurrently,
/// so a single `run_delivery_once` finishes in ~1 delay, not ~N delays.
#[tokio::test]
async fn test_parallel_delivery_is_concurrent() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let hits = Arc::new(AtomicUsize::new(0));
    let delay = std::time::Duration::from_millis(500);
    let (url, shutdown_server) = spawn_slow_200_webhook_server(hits.clone(), delay);

    // 4 independent tasks, each with an end:success webhook pointing at the slow server.
    const N: usize = 4;
    for i in 0..N {
        task_with_success_webhook(&app, &state, &format!("par-{}", i), &url).await;
    }

    // One delivery pass must process all 4 rows concurrently.
    let start = std::time::Instant::now();
    let processed =
        arcrun::workers::run_delivery_once(&state.action_executor, &state.pool, lease_cfg())
            .await
            .unwrap();
    let elapsed = start.elapsed();

    assert_eq!(processed, N, "all {} rows processed in one pass", N);
    assert_eq!(
        hits.load(Ordering::SeqCst),
        N,
        "all {} webhooks received",
        N
    );
    // Sequential would be ~N*500ms = 2s. Concurrent is ~500ms; allow generous margin.
    assert!(
        elapsed < std::time::Duration::from_millis(1500),
        "delivery should be concurrent (~1 delay), took {:?} for {} x {:?}",
        elapsed,
        N,
        delay
    );

    let _ = shutdown_server.send(());
}

/// Mark independence: in one batch, a row whose endpoint returns 500 does NOT prevent
/// the other rows from being marked `success`, and the failing row is rescheduled
/// (stays pending, attempts incremented) rather than exhausted.
#[tokio::test]
async fn test_failing_row_does_not_block_others() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // Two healthy endpoints + one always-500 endpoint.
    let ok_hits = Arc::new(AtomicUsize::new(0));
    let (ok_url, ok_shutdown) = spawn_webhook_server(ok_hits.clone());
    let (bad_url, bad_shutdown) = spawn_500_webhook_server();

    let good1 = task_with_success_webhook(&app, &state, "ind-good-1", &ok_url).await;
    let good2 = task_with_success_webhook(&app, &state, "ind-good-2", &ok_url).await;
    let bad = task_with_success_webhook(&app, &state, "ind-bad", &bad_url).await;

    // One pass: good rows succeed, the bad row is rescheduled with backoff.
    let processed =
        arcrun::workers::run_delivery_once(&state.action_executor, &state.pool, lease_cfg())
            .await
            .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    assert_eq!(processed, 3, "all three rows processed in one pass");

    assert_eq!(
        outbox_count(&state.pool, good1, "success").await,
        1,
        "first healthy row should be marked success despite the failing sibling"
    );
    assert_eq!(
        outbox_count(&state.pool, good2, "success").await,
        1,
        "second healthy row should be marked success despite the failing sibling"
    );

    // The bad row is still pending (rescheduled), not exhausted, with attempts bumped.
    assert_eq!(
        outbox_count(&state.pool, bad, "pending").await,
        1,
        "failing row should be rescheduled (still pending), not lost or exhausted"
    );
    assert_eq!(outbox_count(&state.pool, bad, "success").await, 0);

    #[derive(diesel::QueryableByName)]
    struct AttemptsRow {
        #[diesel(sql_type = diesel::sql_types::Integer)]
        attempts: i32,
    }
    let mut conn = state.pool.get().await.unwrap();
    let a: AttemptsRow = diesel_async::RunQueryDsl::get_result(
        // D3: a still-failing (not exhausted) row stays in the webhook_outbox queue.
        diesel::sql_query("SELECT attempts FROM webhook_outbox WHERE task_id = $1")
            .bind::<diesel::sql_types::Uuid, _>(bad),
        &mut *conn,
    )
    .await
    .unwrap();
    assert_eq!(
        a.attempts, 1,
        "failing row should have one observed failed attempt"
    );

    let _ = ok_shutdown.send(());
    let _ = bad_shutdown.send(());
}
