//! Audit 2, A1 — `run_in_transaction` cancel-safety regression tests.
//!
//! # Original bug
//! `db::run_in_transaction` used to emit raw `sql_query("BEGIN")` / `"COMMIT"` /
//! `"ROLLBACK"`, bypassing diesel-async's `AnsiTransactionManager`. bb8 inspects
//! only that manager state in its `has_broken` check, which the raw SQL never
//! updated. If actix dropped a handler future between `BEGIN` and `COMMIT`
//! (client disconnect, request timeout, any cancellation) the physical
//! connection returned to the pool with an **open transaction still holding row
//! locks** (e.g. the batch `FOR UPDATE`). The next borrower then ran its
//! "autocommit" statements *inside* that leaked transaction, and a later
//! `COMMIT` could make a half-propagated transition durable — violating
//! "API response = durable state" in the other direction.
//!
//! # Fix
//! `run_in_transaction` now delegates to `AsyncConnection::transaction`, which
//! drives the transaction manager. On mid-transaction cancellation the manager
//! is left reporting an open (non-test) transaction, so bb8's `has_broken`
//! returns true and the connection is discarded instead of reused.
//!
//! # What these tests assert
//! * `test_audit2_a1_cancelled_midtx_does_not_leak_transaction` — cancelling a
//!   transaction mid-flight (a `tokio::time::timeout` around a `pg_sleep` inside
//!   the closure) leaves the pool clean: the aborted write is not committed, and
//!   the *next* borrower runs in autocommit (its write is durably visible from an
//!   INDEPENDENT connection with no explicit COMMIT). With the old raw-SQL
//!   implementation the leaked-transaction connection would be reused, so either
//!   the follow-up statement errors on a desynced protocol or the independent
//!   observer sees no committed rows — the test fails.
//! * `test_audit2_a1_commits_on_ok` / `test_audit2_a1_rolls_back_on_err` —
//!   baseline semantics are preserved: `Ok` commits, `Err` rolls back and
//!   propagates the original error, and the connection stays usable afterwards.
//!
//! A dedicated `max_size = 1` pool is used so the (potentially leaked) physical
//! connection is deterministically handed back to the next borrower, and a
//! separate observer pool provides a genuinely independent view of committed
//! state.

use crate::common::*;

use arcrun::action::idempotency_key;
use arcrun::error::ArcRunError;
use arcrun::models::{StatusKind, TriggerCondition, TriggerKind};
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

// NOTE: `diesel_async::RunQueryDsl` is intentionally NOT imported at module scope.
// It is a blanket-implemented trait, so bringing it into scope shadows the inherent
// `AtomicUsize::load` on `Arc<AtomicUsize>` (our `hits` counters), turning every
// `hits.load(...)` into an ambiguous call. Fully-qualify `RunQueryDsl::…` in the SQL
// helpers instead (same convention as `tests/integration/test_outbox.rs`).

#[derive(diesel::QueryableByName)]
struct ProbeId {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    id: i32,
}

/// Create the scratch table used to observe writes.
async fn create_probe_table(pool: &arcrun::DbPool) {
    use diesel_async::RunQueryDsl;
    let mut conn = pool.get().await.unwrap();
    diesel::sql_query("CREATE TABLE tx_probe (id INT PRIMARY KEY)")
        .execute(&mut conn)
        .await
        .expect("create probe table");
}

/// Return the committed `tx_probe` ids as seen through `pool` (sorted).
async fn probe_ids(pool: &arcrun::DbPool) -> Vec<i32> {
    use diesel_async::RunQueryDsl;
    let mut conn = pool.get().await.unwrap();
    let rows: Vec<ProbeId> = diesel::sql_query("SELECT id FROM tx_probe ORDER BY id")
        .get_results(&mut conn)
        .await
        .expect("select probe ids");
    rows.into_iter().map(|r| r.id).collect()
}

/// Build an independent single-connection pool to the same database — a view of
/// committed state that is unaffected by whatever the pool-under-test is doing.
async fn build_observer(url: &str) -> arcrun::DbPool {
    use diesel_async::pooled_connection::AsyncDieselConnectionManager;
    use diesel_async::pooled_connection::bb8::Pool;
    let config =
        AsyncDieselConnectionManager::<diesel_async::AsyncPgConnection>::new(url.to_string());
    Pool::builder()
        .max_size(1)
        .build(config)
        .await
        .expect("observer pool")
}

#[tokio::test]
async fn test_audit2_a1_cancelled_midtx_does_not_leak_transaction() {
    use diesel_async::RunQueryDsl;
    let app = setup_test_db_with_pool_size(1).await;
    create_probe_table(&app.pool).await;
    let observer = build_observer(&app.url).await;

    // 1) Open a transaction that inserts id=1, then blocks on a long server-side
    //    sleep. Cancel it mid-flight by dropping the future via a short timeout.
    {
        let mut conn = app.pool.get().await.unwrap();
        let fut = arcrun::db::run_in_transaction(&mut conn, |conn| {
            Box::pin(async move {
                diesel::sql_query("INSERT INTO tx_probe (id) VALUES (1)")
                    .execute(conn)
                    .await?;
                // The future is dropped while awaiting this — i.e. mid-transaction.
                diesel::sql_query("SELECT pg_sleep(30)")
                    .execute(conn)
                    .await?;
                Ok::<(), ArcRunError>(())
            })
        });
        let res = tokio::time::timeout(Duration::from_millis(500), fut).await;
        assert!(
            res.is_err(),
            "the pg_sleep transaction future should have been cancelled by the timeout"
        );
        // `conn` drops here → returned to the pool. If the transaction manager
        // reports the connection broken (open tx), bb8 discards it rather than
        // handing it — with the leaked transaction — to the next borrower.
    }

    // 2) The next borrower must be in a clean autocommit state: this INSERT is
    //    NOT wrapped in an explicit transaction, so it must commit on its own.
    {
        let mut conn = app.pool.get().await.unwrap();
        diesel::sql_query("INSERT INTO tx_probe (id) VALUES (2)")
            .execute(&mut conn)
            .await
            .expect("autocommit insert on the reused/fresh connection must succeed");
    }

    // 3) Observed from an INDEPENDENT connection: id=1 (cancelled) must have
    //    rolled back, and id=2 must be durably committed. If the connection had
    //    leaked its transaction, id=2 would be trapped uncommitted (observer sees
    //    nothing) — proving real durability, not same-session visibility.
    let ids = probe_ids(&observer).await;
    assert_eq!(
        ids,
        vec![2],
        "observer must see only the autocommit write (id=2); the cancelled tx (id=1) \
         must have rolled back and id=2 must be durably committed — got {ids:?}"
    );
}

#[tokio::test]
async fn test_audit2_a1_commits_on_ok() {
    use diesel_async::RunQueryDsl;
    let app = setup_test_db_with_pool_size(1).await;
    create_probe_table(&app.pool).await;
    let observer = build_observer(&app.url).await;

    {
        let mut conn = app.pool.get().await.unwrap();
        arcrun::db::run_in_transaction(&mut conn, |conn| {
            Box::pin(async move {
                diesel::sql_query("INSERT INTO tx_probe (id) VALUES (10)")
                    .execute(conn)
                    .await?;
                Ok::<(), ArcRunError>(())
            })
        })
        .await
        .expect("Ok closure should commit");
    }

    assert_eq!(
        probe_ids(&observer).await,
        vec![10],
        "an Ok closure must commit its writes"
    );
}

#[tokio::test]
async fn test_audit2_a1_rolls_back_on_err() {
    use diesel_async::RunQueryDsl;
    let app = setup_test_db_with_pool_size(1).await;
    create_probe_table(&app.pool).await;
    let observer = build_observer(&app.url).await;

    {
        let mut conn = app.pool.get().await.unwrap();
        let res: Result<(), ArcRunError> = arcrun::db::run_in_transaction(&mut conn, |conn| {
            Box::pin(async move {
                diesel::sql_query("INSERT INTO tx_probe (id) VALUES (20)")
                    .execute(conn)
                    .await?;
                Err(ArcRunError::Internal("intentional rollback".into()))
            })
        })
        .await;
        assert!(
            matches!(res, Err(ArcRunError::Internal(_))),
            "the closure's error must propagate unchanged"
        );
    }

    assert_eq!(
        probe_ids(&observer).await,
        Vec::<i32>::new(),
        "an Err closure must roll back its writes"
    );

    // The connection must remain usable (clean rollback, not broken): a following
    // autocommit write commits normally.
    {
        let mut conn = app.pool.get().await.unwrap();
        diesel::sql_query("INSERT INTO tx_probe (id) VALUES (21)")
            .execute(&mut conn)
            .await
            .expect("connection must be reusable after a rolled-back transaction");
    }

    assert_eq!(
        probe_ids(&observer).await,
        vec![21],
        "post-rollback autocommit write must commit"
    );
}

// =============================================================================
// Audit 2, A2 — start-before-end gate: no permanent block on end/cancel delivery
// =============================================================================
//
// # Original bug
// The delivery loop's start-before-end gate (`claim_due_outbox_leased`) held an
// `end`/`cancel` outbox row back while ANY `start` row for the same task was still
// `pending`, with no time bound. Two paths could leave a start row `pending`
// forever, so the gate blocked the task's end/cancel delivery indefinitely:
//   1. A crash (or a swallowed UPDATE error) between `mark_task_running` and the
//      start-row completion — the task is Running, but nothing ever re-runs its
//      start (the stale-claim requeue only touches Claimed tasks).
//   2. A Claimed task canceled / stop_batched / dead-end-canceled after a crash
//      mid-webhook, never re-claimed.
// Side effect: those mature-but-unplayable rows inflated `outbox_backlog_stats`
// `oldest_ready_age_secs` without bound (a permanent false alert).
//
// # Fix
//   * `execute_webhook_for_task` now commits `mark_task_running` AND the start-row
//     completion in ONE transaction, closing the crash window on the nominal path.
//   * The gate is bounded by freshness: a pending start row only blocks while its
//     `updated_at > now() - start_stale_secs` (mirror of the existing
//     `webhook_idempotency_timeout` / `WORKER_CLAIM_TIMEOUT_SECS` staleness). A
//     start row that never completes eventually goes stale and stops blocking, so
//     end/cancel are delivered anyway — a deliberate relaxation that keeps
//     start-before-end for healthy starts while forbidding an eternal block.

#[derive(diesel::QueryableByName)]
struct TextStatus {
    #[diesel(sql_type = diesel::sql_types::Text)]
    status: String,
}

/// Status of the (single) `start` outbox row for a task, or `None` if absent.
async fn start_row_status(pool: &arcrun::DbPool, task_id: uuid::Uuid) -> Option<String> {
    let mut conn = pool.get().await.unwrap();
    let rows: Vec<TextStatus> = diesel_async::RunQueryDsl::get_results(
        diesel::sql_query(
            "SELECT status::text AS status FROM webhook_execution \
             WHERE task_id = $1 AND trigger = 'start'",
        )
        .bind::<diesel::sql_types::Uuid, _>(task_id),
        &mut *conn,
    )
    .await
    .unwrap();
    rows.into_iter().next().map(|r| r.status)
}

/// Count outbox rows for a task with the given status (raw SQL).
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

/// Age the task's pending `start` outbox row so its `updated_at` is `secs` in the
/// past — simulating a start row that has been stuck `pending` (crash between
/// `mark_task_running` and its completion) beyond the freshness bound.
async fn age_start_row(pool: &arcrun::DbPool, task_id: uuid::Uuid, secs: i64) {
    let mut conn = pool.get().await.unwrap();
    diesel_async::RunQueryDsl::execute(
        diesel::sql_query(
            "UPDATE webhook_execution \
             SET updated_at = now() - ($2::bigint * interval '1 second') \
             WHERE task_id = $1 AND trigger = 'start'",
        )
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .bind::<diesel::sql_types::BigInt, _>(secs),
        &mut *conn,
    )
    .await
    .unwrap();
}

/// Insert a `pending` `start` outbox row for `task_id` (claims the idempotency slot
/// but never completes it) — the pathological state left by a crash mid-start.
async fn insert_pending_start_row(pool: &arcrun::DbPool, task_id: uuid::Uuid) {
    let mut conn = pool.get().await.unwrap();
    let key = idempotency_key(task_id, &TriggerKind::Start, &TriggerCondition::Success);
    let claimed = arcrun::db_operation::try_claim_webhook_execution(
        &mut conn,
        task_id,
        TriggerKind::Start,
        TriggerCondition::Success,
        &key,
        None,
    )
    .await
    .unwrap();
    assert!(claimed, "start row should be claimed (pending)");
}

/// A2 key test — a **stale** pending `start` row must NOT block end delivery.
///
/// Builds the pathological state: a task with a `start` row stuck `pending` whose
/// `updated_at` is aged well past the freshness bound, plus a mature `end:success`
/// row. Driving the delivery loop must deliver the end row anyway (the gate relaxes
/// on the stale start). With the unbounded gate (fix reverted) the end row would be
/// held back forever and this test fails (no delivery).
#[tokio::test]
async fn test_audit2_a2_stale_start_row_unblocks_end_delivery() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let hits = Arc::new(AtomicUsize::new(0));
    let (webhook_url, shutdown_server) = spawn_webhook_server(hits.clone());

    let payload = json!({
        "id": "a2-stale",
        "name": "A2 Stale Start",
        "kind": "a2-test",
        "timeout": 60,
        "metadata": {"test": true},
        "on_start": webhook_action(),
        "on_success": [{"kind": "Webhook", "params": {"url": webhook_url, "verb": "Post"}}]
    });
    let created = create_tasks_ok(&app, &[payload]).await;
    let task_id = created[0].id;

    // Pathological state: a start row stuck pending, then aged past the freshness bound.
    insert_pending_start_row(&state.pool, task_id).await;

    // Terminal transition -> enqueue the end:success outbox row.
    succeed_task(&state, task_id).await;

    // Age the stuck start row to 1h old (>> the 30s freshness bound of default cfg).
    age_start_row(&state.pool, task_id, 3600).await;
    assert_eq!(
        start_row_status(&state.pool, task_id).await.as_deref(),
        Some("pending"),
        "the start row is still pending (never completed) — the pathological state"
    );

    // default_delivery_cfg() uses start_stale_secs = 30, so the 1h-old start row is
    // stale and the gate must relax.
    let processed = drain_outbox(&state).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert!(
        processed >= 1,
        "the end row must be claimed/delivered despite the stale pending start row"
    );
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "end webhook delivered exactly once despite a stale pending start row"
    );
    assert_eq!(
        outbox_count(&state.pool, task_id, "success").await,
        1,
        "the end row should be marked success"
    );

    let _ = shutdown_server.send(());
}

/// A2 order-preserved test — a **fresh** pending `start` row STILL blocks end
/// delivery (the gate holds for healthy in-flight starts). Once the same row is
/// aged past the freshness bound, the end row delivers — proving the block is
/// time-bounded, not permanent.
#[tokio::test]
async fn test_audit2_a2_fresh_start_row_still_blocks_end() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let hits = Arc::new(AtomicUsize::new(0));
    let (webhook_url, shutdown_server) = spawn_webhook_server(hits.clone());

    let payload = json!({
        "id": "a2-fresh",
        "name": "A2 Fresh Start",
        "kind": "a2-test",
        "timeout": 60,
        "metadata": {"test": true},
        "on_start": webhook_action(),
        "on_success": [{"kind": "Webhook", "params": {"url": webhook_url, "verb": "Post"}}]
    });
    let created = create_tasks_ok(&app, &[payload]).await;
    let task_id = created[0].id;

    // A fresh pending start row (updated_at = now()) — a healthy in-flight start.
    insert_pending_start_row(&state.pool, task_id).await;

    // Terminal transition -> enqueue the end:success outbox row.
    succeed_task(&state, task_id).await;

    // With a fresh start row, the gate holds: end must NOT be delivered.
    let processed = drain_outbox(&state).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        processed, 0,
        "end row must be held while a FRESH start row is pending (start-before-end)"
    );
    assert_eq!(hits.load(Ordering::SeqCst), 0, "no end webhook fired yet");

    // Now age the start row past the freshness bound: the gate relaxes and the end
    // row becomes deliverable (the block is bounded, never permanent).
    age_start_row(&state.pool, task_id, 3600).await;
    drain_outbox(&state).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "end webhook delivered once the start row went stale"
    );

    let _ = shutdown_server.send(());
}

/// A2 same-tx (nominal path) test — driving a task through the real `start_loop`,
/// the on_start webhook succeeds and the running transition + start-row completion
/// commit together, so the `start` row ends up `success` (never a lingering
/// `pending`). The task's end row then delivers normally through the healthy gate.
#[tokio::test]
async fn test_audit2_a2_start_row_completed_atomically_with_running() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let start_hits = Arc::new(AtomicUsize::new(0));
    let (start_url, start_shutdown) = spawn_webhook_server(start_hits.clone());
    let end_hits = Arc::new(AtomicUsize::new(0));
    let (end_url, end_shutdown) = spawn_webhook_server(end_hits.clone());

    let payload = json!({
        "id": "a2-nominal",
        "name": "A2 Nominal Start",
        "kind": "a2-test",
        "timeout": 60,
        "metadata": {"test": true},
        "on_start": {"kind": "Webhook", "params": {"url": start_url, "verb": "Post"}},
        "on_success": [{"kind": "Webhook", "params": {"url": end_url, "verb": "Post"}}]
    });
    let created = create_tasks_ok(&app, &[payload]).await;
    let task_id = created[0].id;
    assert_eq!(created[0].status, StatusKind::Pending);

    // Drive the real start_loop: claim -> on_start webhook -> mark_task_running +
    // start-row completion (one tx).
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let evaluator = state.action_executor.clone();
    let pool = state.pool.clone();
    let handle = tokio::spawn(async move {
        arcrun::workers::start_loop(
            &evaluator,
            pool,
            Duration::from_millis(50),
            true,
            50,
            10,
            shutdown_rx,
            arcrun::workers::WorkerNudges::new(),
            std::time::Duration::from_secs(30),
        )
        .await;
    });

    tokio::time::sleep(Duration::from_millis(500)).await;
    let _ = shutdown_tx.send(true);
    let _ = handle.await;

    assert_eq!(
        start_hits.load(Ordering::SeqCst),
        1,
        "on_start webhook should have fired once"
    );
    assert_task_status(
        &app,
        task_id,
        StatusKind::Running,
        "task should be Running after start_loop",
    )
    .await;

    // The A2 fix: the start row is `success` (committed atomically with the running
    // transition), NOT a lingering `pending` that would block end/cancel forever.
    assert_eq!(
        start_row_status(&state.pool, task_id).await.as_deref(),
        Some("success"),
        "start row must be completed (success) atomically with the running transition"
    );

    // Transition Running -> Success, enqueuing the end row. The (success) start row
    // does not block it — the end webhook delivers normally.
    {
        let mut conn = state.pool.get().await.unwrap();
        let dto = arcrun::dtos::UpdateTaskDto {
            status: Some(StatusKind::Success),
            metadata: None,
            new_success: None,
            new_failures: None,
            failure_reason: None,
            expected_count: None,
            priority: None,
        };
        let result = arcrun::db_operation::update_running_task(&mut conn, task_id, dto, true)
            .await
            .unwrap();
        assert_eq!(result, arcrun::db_operation::UpdateTaskResult::Updated);
    }

    drain_outbox(&state).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        end_hits.load(Ordering::SeqCst),
        1,
        "end webhook delivered normally after a healthy (success) start row"
    );

    let _ = start_shutdown.send(());
    let _ = end_shutdown.send(());
}

// =============================================================================
// Audit 2, A4 — Claimed + on_start in flight: no zombie work at the consumer
// =============================================================================
//
// # Original bug
// The `Claimed` state spans the ENTIRE on_start webhook-in-flight window (the
// worker's permit queue plus up to the webhook timeout per action), not just the
// instant before on_start. Two gaps let a consumer receive on_start, start work,
// and then never get a cancel:
//   1. `execute_webhook_for_task` only called `save_cancel_actions` in the
//      `mark_task_running == true` branch. If the task left `Claimed` while on_start
//      was in flight (a concurrent DELETE / stop_batch / requeue), the transition
//      returned false and the cancel action the consumer returned was silently
//      dropped — so even if a cancel was later delivered, it carried no action.
//   2. `cancel_task` enqueued a cancel outbox row only for `Running`, and
//      `stop_batch` treated `Claimed` as "on_start not yet called" (no cancel), and
//      the dead-end path gated the cancel on `was_running`. So a Claimed task that
//      had received on_start was never sent a cancel at all.
//
// # Fix
//   * `save_cancel_actions` now runs INSIDE the running-transition/start-row-
//     completion transaction, unconditionally when on_start actually executed
//     (`claimed == true`) — regardless of whether `mark_task_running` transitioned
//     the row. Committing the actions atomically with the start-row completion also
//     closes the race with the delivery loop's start-before-end gate: the cancel row
//     (enqueued by the concurrent transition) is held while the start row is pending,
//     and by the time the gate releases (start row completed in this tx) the cancel
//     actions are already committed. Validation is best-effort (invalid actions are
//     skipped, never rolling back the transition — the 4.2 decision).
//   * `cancel_task`, `stop_batch`, and the dead-end path now enqueue a cancel outbox
//     row for `Claimed` as well as `Running`. A Claimed task that never returned a
//     cancel action prefetches zero cancel actions ⇒ fast-path success (no HTTP), so
//     the broadened enqueue is innocuous.

/// Count `cancel`-trigger action rows registered for a task.
async fn cancel_action_count(pool: &arcrun::DbPool, task_id: uuid::Uuid) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Cnt {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        c: i64,
    }
    let mut conn = pool.get().await.unwrap();
    let r: Cnt = diesel_async::RunQueryDsl::get_result(
        diesel::sql_query(
            "SELECT count(*) AS c FROM action WHERE task_id = $1 AND trigger = 'cancel'",
        )
        .bind::<diesel::sql_types::Uuid, _>(task_id),
        &mut *conn,
    )
    .await
    .unwrap();
    r.c
}

/// Count `cancel`-trigger outbox rows for a task with the given status.
async fn cancel_outbox_count(pool: &arcrun::DbPool, task_id: uuid::Uuid, status: &str) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Cnt {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        c: i64,
    }
    let mut conn = pool.get().await.unwrap();
    // D3: a pending cancel row lives in the queue; delivered ones in the ledger/history.
    let r: Cnt = if status == "pending" {
        diesel_async::RunQueryDsl::get_result(
            diesel::sql_query(
                "SELECT count(*) AS c FROM webhook_outbox \
                 WHERE task_id = $1 AND trigger = 'cancel'",
            )
            .bind::<diesel::sql_types::Uuid, _>(task_id),
            &mut *conn,
        )
        .await
        .unwrap()
    } else {
        diesel_async::RunQueryDsl::get_result(
            diesel::sql_query(
                "SELECT count(*) AS c FROM webhook_execution \
                 WHERE task_id = $1 AND trigger = 'cancel' \
                   AND status = $2::webhook_execution_status",
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

/// Spawn a mock on_start server that responds — after `delay` — with a `NewActionDto`
/// (a cancel action pointing at `cancel_url`). The delay keeps the task `Claimed`
/// (on_start in flight) long enough for a concurrent cancel to fire.
fn spawn_slow_server_returning_cancel_action(
    cancel_url: &str,
    delay: Duration,
) -> (String, tokio::sync::oneshot::Sender<()>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    let listener = tokio::net::TcpListener::from_std(listener).unwrap();

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let cancel_url = cancel_url.to_string();

    tokio::spawn(async move {
        loop {
            tokio::select! {
                result = listener.accept() => {
                    if let Ok((mut stream, _)) = result {
                        let cancel_url = cancel_url.clone();
                        tokio::spawn(async move {
                            let mut buf = [0u8; 4096];
                            let _ = stream.read(&mut buf).await;
                            // Hold the response so the task stays Claimed (in flight).
                            tokio::time::sleep(delay).await;
                            let body = serde_json::json!({
                                "kind": "Webhook",
                                "params": {"url": cancel_url, "verb": "Post"}
                            });
                            let body_str = serde_json::to_string(&body).unwrap();
                            let response = format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                                body_str.len(),
                                body_str
                            );
                            let _ = stream.write_all(response.as_bytes()).await;
                        });
                    }
                }
                _ = &mut shutdown_rx => break,
            }
        }
    });

    (format!("http://{}/start", addr), shutdown_tx)
}

/// A4 core test — cancelling a task WHILE its on_start is in flight must NOT drop the
/// cancel action the consumer returned, and the consumer must still receive the cancel.
///
/// The task is claimed and its (slow) on_start webhook begins; while it is in flight a
/// DELETE cancels the task (it leaves `Claimed`), so when on_start returns
/// `mark_task_running` == false. The A4 fix persists the returned cancel action inside
/// the same tx anyway, and the cancel outbox row (enqueued by the DELETE, now allowed
/// for `Claimed`) is delivered WITH that action after draining. With the fix reverted,
/// the action is dropped (and/or no cancel row is enqueued) → 0 deliveries.
#[tokio::test]
async fn test_audit2_a4_cancel_action_saved_when_task_left_claimed_during_on_start() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // Cancel webhook target — counts deliveries to the consumer.
    let cancel_hits = Arc::new(AtomicUsize::new(0));
    let (cancel_url, cancel_shutdown) = spawn_webhook_server(cancel_hits.clone());

    // Slow on_start that returns a cancel action once it finally responds.
    let (start_url, start_shutdown) =
        spawn_slow_server_returning_cancel_action(&cancel_url, Duration::from_millis(1500));

    let payload = json!({
        "id": "a4-inflight",
        "name": "A4 In-flight Cancel",
        "kind": "a4-test",
        "timeout": 60,
        "metadata": {"test": true},
        "on_start": {"kind": "Webhook", "params": {"url": start_url, "verb": "Post"}}
    });
    let created = create_tasks_ok(&app, &[payload]).await;
    let task_id = created[0].id;

    // Drive the real start_loop.
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let evaluator = state.action_executor.clone();
    let pool = state.pool.clone();
    let handle = tokio::spawn(async move {
        arcrun::workers::start_loop(
            &evaluator,
            pool,
            Duration::from_millis(50),
            true,
            50,
            10,
            shutdown_rx,
            arcrun::workers::WorkerNudges::new(),
            std::time::Duration::from_secs(30),
        )
        .await;
    });

    // Wait for start_loop to claim the task and begin the slow on_start webhook.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_task_status(
        &app,
        task_id,
        StatusKind::Claimed,
        "task should be Claimed with on_start in flight",
    )
    .await;

    // Cancel it WHILE on_start is in flight — the task leaves Claimed for Canceled and
    // (A4) a cancel outbox row is enqueued.
    let cancel_req = actix_web::test::TestRequest::delete()
        .uri(&format!("/task/{}", task_id))
        .to_request();
    let cancel_resp = actix_web::test::call_service(&app, cancel_req).await;
    assert!(cancel_resp.status().is_success(), "cancel should succeed");
    assert_task_status(
        &app,
        task_id,
        StatusKind::Canceled,
        "task should be Canceled",
    )
    .await;

    // Let the slow on_start finish; the start_loop tx then runs mark_task_running
    // (== false) but STILL saves the cancel action + completes the start row (A4).
    // Shutting down waits for the in-flight webhook + its tx to complete.
    let _ = shutdown_tx.send(true);
    let _ = handle.await;

    assert_eq!(
        cancel_action_count(&state.pool, task_id).await,
        1,
        "cancel action returned by on_start must be saved even though mark_task_running == false"
    );

    // Deliver the outbox: the cancel row was held by the start-before-end gate until
    // the start row completed (same tx as the save), so it now delivers WITH the action.
    drain_outbox(&state).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        cancel_hits.load(Ordering::SeqCst),
        1,
        "the consumer must receive the cancel webhook exactly once (no zombie work)"
    );

    let _ = cancel_shutdown.send(());
    let _ = start_shutdown.send(());
}

/// A4 — cancelling a `Claimed` task (via DELETE /task) must enqueue a cancel outbox
/// row. Before the fix, `cancel_task` only enqueued for `Running`.
#[tokio::test]
async fn test_audit2_a4_cancel_task_on_claimed_enqueues_cancel_outbox() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let payload = json!({
        "id": "a4-claimed",
        "name": "A4 Claimed Cancel",
        "kind": "a4-test",
        "timeout": 60,
        "metadata": {"test": true},
        "on_start": webhook_action()
    });
    let created = create_tasks_ok(&app, &[payload]).await;
    let task_id = created[0].id;

    // Drive the task to Claimed directly — the window the audit flags.
    {
        let mut conn = state.pool.get().await.unwrap();
        let claimed = arcrun::db_operation::claim_task(&mut conn, &task_id)
            .await
            .unwrap();
        assert!(claimed, "task should be claimed");
    }
    assert_task_status(&app, task_id, StatusKind::Claimed, "task should be Claimed").await;

    let cancel_req = actix_web::test::TestRequest::delete()
        .uri(&format!("/task/{}", task_id))
        .to_request();
    let resp = actix_web::test::call_service(&app, cancel_req).await;
    assert!(resp.status().is_success(), "cancel should succeed");
    assert_task_status(
        &app,
        task_id,
        StatusKind::Canceled,
        "task should be Canceled",
    )
    .await;

    assert_eq!(
        cancel_outbox_count(&state.pool, task_id, "pending").await,
        1,
        "canceling a Claimed task must enqueue a pending cancel outbox row"
    );
}

/// A4 — stopping a batch that contains a `Claimed` task must enqueue a cancel outbox
/// row for that task. Before the fix, `stop_batch` treated Claimed as "on_start not
/// yet called" and enqueued nothing.
#[tokio::test]
async fn test_audit2_a4_stop_batch_on_claimed_enqueues_cancel_outbox() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let payload = json!({
        "id": "a4-batch-claimed",
        "name": "A4 Batch Claimed",
        "kind": "a4-test",
        "timeout": 60,
        "metadata": {"test": true},
        "on_start": webhook_action()
    });
    let created = create_tasks_ok(&app, &[payload]).await;
    let task_id = created[0].id;
    let batch_id = created[0].batch_id.expect("task should have a batch_id");

    {
        let mut conn = state.pool.get().await.unwrap();
        assert!(
            arcrun::db_operation::claim_task(&mut conn, &task_id)
                .await
                .unwrap(),
            "task should be claimed"
        );
    }
    assert_task_status(&app, task_id, StatusKind::Claimed, "task should be Claimed").await;

    let req = actix_web::test::TestRequest::delete()
        .uri(&format!("/batch/{}", batch_id))
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert!(resp.status().is_success(), "stop_batch should succeed");
    assert_task_status(
        &app,
        task_id,
        StatusKind::Canceled,
        "task should be Canceled",
    )
    .await;

    assert_eq!(
        cancel_outbox_count(&state.pool, task_id, "pending").await,
        1,
        "stopping a batch with a Claimed task must enqueue a pending cancel outbox row"
    );
}

/// A4 innocuousness — a `Claimed` task that never started (no cancel action ever
/// registered) canceled and drained: the cancel row exists but delivers via the
/// zero-action fast-path (marked success) with ZERO HTTP hits.
#[tokio::test]
async fn test_audit2_a4_claimed_cancel_without_action_delivers_no_http() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // A server we assert is never hit.
    let hits = Arc::new(AtomicUsize::new(0));
    let (_url, shutdown) = spawn_webhook_server(hits.clone());

    let payload = json!({
        "id": "a4-noop",
        "name": "A4 No-op Cancel",
        "kind": "a4-test",
        "timeout": 60,
        "metadata": {"test": true},
        "on_start": webhook_action()
    });
    let created = create_tasks_ok(&app, &[payload]).await;
    let task_id = created[0].id;

    {
        let mut conn = state.pool.get().await.unwrap();
        assert!(
            arcrun::db_operation::claim_task(&mut conn, &task_id)
                .await
                .unwrap(),
            "task should be claimed"
        );
    }

    let req = actix_web::test::TestRequest::delete()
        .uri(&format!("/task/{}", task_id))
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert!(resp.status().is_success(), "cancel should succeed");
    assert_task_status(
        &app,
        task_id,
        StatusKind::Canceled,
        "task should be Canceled",
    )
    .await;

    assert_eq!(
        cancel_outbox_count(&state.pool, task_id, "pending").await,
        1,
        "cancel outbox row enqueued for the Claimed task"
    );

    drain_outbox(&state).await;
    tokio::time::sleep(Duration::from_millis(150)).await;

    assert_eq!(
        hits.load(Ordering::SeqCst),
        0,
        "no cancel action registered -> zero HTTP deliveries (innocuous)"
    );
    assert_eq!(
        cancel_outbox_count(&state.pool, task_id, "success").await,
        1,
        "the zero-action cancel row must be marked success via the fast-path"
    );

    let _ = shutdown.send(());
}

/// A4 permit-wait window — a task canceled while queued for a webhook permit (claimed,
/// but its `start` outbox row not yet created) must NOT receive on_start at all.
///
/// In that window the cancel outbox row is not held by the start-before-end gate
/// (there is no start row yet), so it can be drained as a zero-action fast-path
/// BEFORE on_start fires. If on_start then fires anyway, the consumer starts zombie
/// work whose cancel has already been consumed — unfixable after the fact. The fix
/// re-checks the task's status in `start_task` right after creating the start row
/// (post-permit): no longer `Claimed` ⇒ skip on_start and complete the start row
/// (nothing executed, so the zero-action cancel was correct). Any transition landing
/// after that check is gated normally (start row pending + fresh) until the
/// completion tx commits the cancel actions.
///
/// Deterministic setup: `webhook_concurrency = 1`; T1's slow on_start (2 s) holds the
/// only permit while T2 — claimed in the same iteration — waits. T2 is canceled and
/// the outbox drained during that wait. With the fix, T2's on_start never fires and
/// its start row ends `success`; with the fix reverted, T2's mock receives on_start.
#[tokio::test]
async fn test_audit2_a4_cancel_during_permit_wait_skips_on_start() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // T1's slow on_start: holds the single permit for 2 s.
    let t1_cancel_hits = Arc::new(AtomicUsize::new(0));
    let (t1_cancel_url, t1_cancel_shutdown) = spawn_webhook_server(t1_cancel_hits.clone());
    let (t1_start_url, t1_start_shutdown) =
        spawn_slow_server_returning_cancel_action(&t1_cancel_url, Duration::from_millis(2000));

    // T2's on_start: must NEVER be hit.
    let t2_start_hits = Arc::new(AtomicUsize::new(0));
    let (t2_start_url, t2_start_shutdown) = spawn_webhook_server(t2_start_hits.clone());

    // Higher priority on T1 so it is claimed/processed first and takes the permit.
    let t1 = json!({
        "id": "a4-permit-t1",
        "name": "A4 Permit Holder",
        "kind": "a4-test",
        "timeout": 60,
        "priority": 10,
        "metadata": {"test": true},
        "on_start": {"kind": "Webhook", "params": {"url": t1_start_url, "verb": "Post"}}
    });
    let t2 = json!({
        "id": "a4-permit-t2",
        "name": "A4 Permit Waiter",
        "kind": "a4-test",
        "timeout": 60,
        "priority": 0,
        "metadata": {"test": true},
        "on_start": {"kind": "Webhook", "params": {"url": t2_start_url, "verb": "Post"}}
    });
    let created = create_tasks_ok(&app, &[t1, t2]).await;
    let t2_id = created[1].id;

    // Drive the real start_loop with webhook_concurrency = 1.
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let evaluator = state.action_executor.clone();
    let pool = state.pool.clone();
    let handle = tokio::spawn(async move {
        arcrun::workers::start_loop(
            &evaluator,
            pool,
            Duration::from_millis(50),
            true,
            50,
            1,
            shutdown_rx,
            arcrun::workers::WorkerNudges::new(),
            std::time::Duration::from_secs(30),
        )
        .await;
    });

    // Both tasks are claimed in the first iteration; T1's webhook is in flight, T2 is
    // Claimed, queued for the permit, with NO start row yet.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_task_status(
        &app,
        t2_id,
        StatusKind::Claimed,
        "T2 should be Claimed, waiting for the permit",
    )
    .await;
    assert_eq!(
        start_row_status(&state.pool, t2_id).await,
        None,
        "T2's start row must not exist yet (permit-wait window)"
    );

    // Cancel T2 during the permit wait and drain: the cancel row is not gated (no
    // start row) and is consumed as a zero-action fast-path — as in production.
    let req = actix_web::test::TestRequest::delete()
        .uri(&format!("/task/{}", t2_id))
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert!(resp.status().is_success(), "cancel should succeed");
    drain_outbox(&state).await;
    assert_eq!(
        cancel_outbox_count(&state.pool, t2_id, "success").await,
        1,
        "T2's cancel row was drained (zero-action) during the permit wait"
    );

    // Let T1's slow webhook finish; the loop then processes T2 with the permit.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let _ = shutdown_tx.send(true);
    let _ = handle.await;

    assert_eq!(
        t2_start_hits.load(Ordering::SeqCst),
        0,
        "T2's on_start must NOT fire for a task that left Claimed during the permit wait"
    );
    assert_eq!(
        start_row_status(&state.pool, t2_id).await.as_deref(),
        Some("success"),
        "T2's start row must be completed by the skip path (gate released)"
    );
    assert_task_status(&app, t2_id, StatusKind::Canceled, "T2 stays Canceled").await;

    let _ = t1_cancel_shutdown.send(());
    let _ = t1_start_shutdown.send(());
    let _ = t2_start_shutdown.send(());
}

// ---------------------------------------------------------------------------
// A7 — batch-updater counter pipeline: terminal guard, i32 clamp, anti-poison.
// ---------------------------------------------------------------------------

// NOTE: `RunQueryDsl` is intentionally not in module scope (see top-of-file
// comment), so `execute` is invoked in fully-qualified form here.

/// Force a task's status (and, for `running`, a known-old `last_updated`) directly.
async fn force_status_running_old(pool: &arcrun::DbPool, task_id: uuid::Uuid) {
    use diesel::sql_types::{Timestamptz, Uuid as SqlUuid};
    let mut conn = pool.get().await.unwrap();
    let past = chrono::Utc::now() - chrono::Duration::seconds(3600);
    let q = diesel::sql_query(
        "UPDATE task SET status = 'running', started_at = $1, last_updated = $1 WHERE id = $2",
    )
    .bind::<Timestamptz, _>(past)
    .bind::<SqlUuid, _>(task_id);
    diesel_async::RunQueryDsl::execute(q, &mut conn)
        .await
        .unwrap();
}

/// Force a task to terminal `success`, stamping `last_updated = now()`.
async fn force_status_terminal_success(pool: &arcrun::DbPool, task_id: uuid::Uuid) {
    use diesel::sql_types::Uuid as SqlUuid;
    let mut conn = pool.get().await.unwrap();
    let q = diesel::sql_query(
        "UPDATE task SET status = 'success', ended_at = now(), last_updated = now() WHERE id = $1",
    )
    .bind::<SqlUuid, _>(task_id);
    diesel_async::RunQueryDsl::execute(q, &mut conn)
        .await
        .unwrap();
}

/// Set a task's raw success counter (used to place it near i32::MAX).
async fn set_success_counter(pool: &arcrun::DbPool, task_id: uuid::Uuid, value: i32) {
    use diesel::sql_types::{Integer, Uuid as SqlUuid};
    let mut conn = pool.get().await.unwrap();
    let q = diesel::sql_query("UPDATE task SET success = $1 WHERE id = $2")
        .bind::<Integer, _>(value)
        .bind::<SqlUuid, _>(task_id);
    diesel_async::RunQueryDsl::execute(q, &mut conn)
        .await
        .unwrap();
}

async fn put_counts(
    app: &impl actix_web::dev::Service<
        actix_http::Request,
        Response = actix_web::dev::ServiceResponse<impl actix_web::body::MessageBody>,
        Error = actix_web::Error,
    >,
    task_id: uuid::Uuid,
    success: i32,
    failures: i32,
) {
    let req = actix_web::test::TestRequest::put()
        .uri(&format!("/task/{}", task_id))
        .set_json(&json!({"new_success": success, "new_failures": failures}))
        .to_request();
    let resp = actix_web::test::call_service(app, req).await;
    assert_eq!(
        resp.status(),
        actix_web::http::StatusCode::ACCEPTED,
        "PUT counter update should be accepted"
    );
}

/// # Original bug (A7)
/// The batch-updater flush UNNEST statement had **no status guard**. A counter
/// flush (from a buffered `PUT /task/{id}`) that landed *after* the task became
/// terminal mutated `success`/`failures`/`last_updated` of a terminal task —
/// violating "terminal states are immutable" and diverging from the counts that
/// were already delivered with the task's end notification.
///
/// # Fix
/// The flush now carries `AND task.status NOT IN ('success','failure','canceled')`.
/// A row whose task is terminal no longer matches, so its buffered counts are
/// **dropped** (a terminal task's counters are frozen — dropping, not re-queuing,
/// is correct). A non-match is not an error, so nothing is re-queued.
///
/// # What this test asserts
/// * Control: while the task is `running`, a flush DOES apply the counts and bumps
///   `last_updated` (keepalive preserved).
/// * After the task is terminal, a later flush leaves `success`, `failures` AND
///   `last_updated` **unchanged**. Reverting the guard makes the terminal counters
///   move and this test go red.
#[tokio::test]
async fn test_audit2_a7_terminal_guard_drops_late_flush() {
    let (_g, test_state) = setup_test_app_with_batch_updater().await;
    let state = test_state.state;
    let app = test_service!(state);

    let created = create_tasks_ok(&app, &[task_json("a7-guard", "A7 terminal guard", "a7")]).await;
    let task_id = created[0].id;

    // Control: while running, a flush must apply counts + bump last_updated.
    force_status_running_old(&state.pool, task_id).await;
    let running_before = get_task_ok(&app, task_id).await;
    put_counts(&app, task_id, 2, 0).await;
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    let running_after = get_task_ok(&app, task_id).await;
    assert_eq!(
        running_after.success, 2,
        "running task must receive counter updates"
    );
    assert!(
        running_after.last_updated > running_before.last_updated,
        "running task flush must bump last_updated (keepalive)"
    );

    // Transition to terminal; snapshot the frozen counters + last_updated.
    force_status_terminal_success(&state.pool, task_id).await;
    let terminal = get_task_ok(&app, task_id).await;

    // A late flush for the now-terminal task must be dropped entirely.
    put_counts(&app, task_id, 5, 3).await;
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;

    let after = get_task_ok(&app, task_id).await;
    assert_eq!(
        after.success, terminal.success,
        "success must be frozen on a terminal task (late flush dropped)"
    );
    assert_eq!(
        after.failures, terminal.failures,
        "failures must be frozen on a terminal task (late flush dropped)"
    );
    assert_eq!(
        after.last_updated, terminal.last_updated,
        "last_updated must NOT move on a terminal task (late flush dropped)"
    );
}

/// # Original bug (A7 — poison pill)
/// The flush summed counters as `task.success + batch.s` in `int4`. A task whose
/// counter approached `i32::MAX` (realistic for a long-lived high-throughput
/// batch) would overflow on the next delta, raising `integer out of range`. That
/// error failed the whole multi-row flush, which re-queued **every** count with
/// no backoff or cap — the same overflowing row erroring on each iteration, so the
/// entire counter pipeline (all tasks) was wedged until process restart (which
/// loses the in-memory DashMap).
///
/// # Fix
/// The sum is computed in `bigint` and clamped:
/// `LEAST(task.success::bigint + batch.s, 2147483647)`. The `::bigint` cast makes
/// the addition overflow-proof; `LEAST(..)` saturates at `i32::MAX`. No error is
/// ever raised, so the flush commits and the pipeline keeps flowing.
///
/// # What this test asserts
/// A task set to `i32::MAX - 100` that receives a `+1000` delta saturates to
/// exactly `2147483647` with no error, and the updater loop remains alive
/// (a second, independent task's counters are still flushed afterwards). Reverting
/// the clamp makes the flush raise an overflow error: the delta is never applied
/// (counter stays at `i32::MAX - 100`) and this test goes red.
#[tokio::test]
async fn test_audit2_a7_clamp_prevents_i32_overflow() {
    let (_g, test_state) = setup_test_app_with_batch_updater().await;
    let state = test_state.state;
    let app = test_service!(state);

    let created = create_tasks_ok(&app, &[task_json("a7-clamp", "A7 clamp", "a7")]).await;
    let task_id = created[0].id;

    // Place the counter just below i32::MAX while the task is active.
    force_status_running_old(&state.pool, task_id).await;
    let near_max = i32::MAX - 100; // 2_147_483_547
    set_success_counter(&state.pool, task_id, near_max).await;

    // A +1000 delta would overflow int4 without the bigint/LEAST clamp.
    put_counts(&app, task_id, 1000, 0).await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let after = get_task_ok(&app, task_id).await;
    assert_eq!(
        after.success,
        i32::MAX,
        "counter must saturate at i32::MAX, not overflow (got {})",
        after.success
    );

    // Pipeline liveness: an independent task's counters still flush afterwards,
    // proving the near-overflow row did not wedge the updater loop.
    let created2 = create_tasks_ok(
        &app,
        &[task_json("a7-clamp-live", "A7 clamp liveness", "a7")],
    )
    .await;
    let live_id = created2[0].id;
    force_status_running_old(&state.pool, live_id).await;
    put_counts(&app, live_id, 7, 0).await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let live = get_task_ok(&app, live_id).await;
    assert_eq!(
        live.success, 7,
        "pipeline must stay alive: a later task's counters are still persisted"
    );
}

// ---------------------------------------------------------------------------
// A3 — `Paused` is no longer an escape-hatch / state-trap.
// ---------------------------------------------------------------------------
//
// # Original bug
// `Paused` combined three defects: (1) no resume path existed (the endpoint doc
// promised "PATCH to Pending" but `validate_update_task` only accepted
// Success/Failure and `update_running_task` only matched Running/Claimed, so the
// only way out of Paused was cancel); (2) all propagation filtered `status =
// 'waiting'`, so a task paused while `Waiting` never received the `wait_*`
// decrements nor the cascade-fail from parents that finished during the pause —
// counters were stranded forever, the batch never went terminal and
// `on_batch_complete` never fired; (3) a `Running` task could be paused, escaping
// the timeout loop while the external worker's PATCH 404'd — a wedged task.
//
// # Fix
// * Pause is restricted to `Pending`/`Waiting` (atomic guarded UPDATE); Running
//   (suggest cancel), Claimed and terminal states are refused with a 400.
// * A new `PATCH /task/resume/{id}` moves a `Paused` task back to `Waiting` (if any
//   dependency is still outstanding) or `Pending` (all met), in one atomic guarded
//   UPDATE (`CASE` on the counters — no SELECT-then-UPDATE race). Only `Paused` is
//   resumable.
// * Propagation now targets `status IN ('waiting','paused')` for the `wait_*`
//   decrements AND the cascade-fail, so a paused task is kept consistent and is not
//   shielded from a required parent's failure. The transition-to-Pending stays
//   `Waiting`-only: a Paused task whose counters reach 0 stays Paused until resume.

use actix_http::Request as ActixRequest;
use actix_web::body::MessageBody as ActixMessageBody;
use actix_web::dev::{Service as ActixService, ServiceResponse as ActixServiceResponse};
use actix_web::http::StatusCode;
use arcrun::handlers::AppState;

/// PATCH /task/pause/{id}, returning the HTTP status.
async fn pause_task_http<S, B>(app: &S, id: uuid::Uuid) -> StatusCode
where
    S: ActixService<ActixRequest, Response = ActixServiceResponse<B>, Error = actix_web::Error>,
    B: ActixMessageBody,
{
    let req = actix_web::test::TestRequest::patch()
        .uri(&format!("/task/pause/{}", id))
        .to_request();
    actix_web::test::call_service(app, req).await.status()
}

/// PATCH /task/resume/{id}, returning the HTTP status.
async fn resume_task_http<S, B>(app: &S, id: uuid::Uuid) -> StatusCode
where
    S: ActixService<ActixRequest, Response = ActixServiceResponse<B>, Error = actix_web::Error>,
    B: ActixMessageBody,
{
    let req = actix_web::test::TestRequest::patch()
        .uri(&format!("/task/resume/{}", id))
        .to_request();
    actix_web::test::call_service(app, req).await.status()
}

/// Drive a Pending task straight to `Claimed` (no on_start), for the refusal tests.
async fn force_claimed(state: &AppState, task_id: uuid::Uuid) {
    let mut conn = state.pool.get().await.unwrap();
    assert!(
        arcrun::db_operation::claim_task(&mut conn, &task_id)
            .await
            .unwrap(),
        "task should be claimed"
    );
}

/// A3 test 1 — a `Paused` task still receives `wait_*` decrements when a parent
/// finishes, but does NOT auto-transition to Pending; resume then unblocks it.
///
/// DAG: parent -> child (requires_success). Child is Waiting, then paused. When the
/// parent succeeds, the (Paused) child's counters must be decremented to 0 while it
/// stays Paused. Resuming it (counters 0) moves it to Pending. With the fix reverted
/// the decrement is filtered out (`status = 'waiting'` only) and the counters stay
/// frozen at 1/1 — the child could never run even after resume.
#[tokio::test]
async fn test_audit2_a3_paused_receives_decrements() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let created = create_tasks_ok(
        &app,
        &[
            task_json("parent", "Parent", "a3"),
            task_with_deps("child", "Child", "a3", vec![("parent", true)]),
        ],
    )
    .await;
    let parent = created[0].id;
    let child = created[1].id;
    assert_eq!(created[1].status, StatusKind::Waiting);

    // Pause the Waiting child.
    assert_eq!(pause_task_http(&app, child).await, StatusCode::OK);
    assert_task_status(&app, child, StatusKind::Paused, "child should be Paused").await;
    assert_eq!(
        read_wait_counters(&state.pool, child).await,
        (1, 1),
        "child starts with one unmet dependency"
    );

    // Parent succeeds -> the Paused child must be decremented, but stay Paused.
    succeed_task(&state, parent).await;
    assert_eq!(
        read_wait_counters(&state.pool, child).await,
        (0, 0),
        "Paused child must receive the wait_* decrements from its parent"
    );
    assert_task_status(
        &app,
        child,
        StatusKind::Paused,
        "child must stay Paused (no auto-transition to Pending)",
    )
    .await;

    // Resume -> counters are 0, so it goes straight to Pending.
    assert_eq!(resume_task_http(&app, child).await, StatusCode::OK);
    assert_task_status(
        &app,
        child,
        StatusKind::Pending,
        "resumed child with met deps should be Pending",
    )
    .await;
}

/// A3 test 2 — cascade-fail reaches a `Paused` task and propagates recursively.
///
/// DAG: parent -> child -> grandchild (all requires_success). Child is paused, then
/// the parent fails. The Paused child must be marked Failure (a pause does not shield
/// it), and the failure must cascade recursively to the grandchild. With the fix
/// reverted the cascade skips the Paused child (and thus the grandchild), leaving both
/// stuck Waiting/Paused forever.
#[tokio::test]
async fn test_audit2_a3_cascade_fail_reaches_paused() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let created = create_tasks_ok(
        &app,
        &[
            task_json("parent", "Parent", "a3"),
            task_with_deps("child", "Child", "a3", vec![("parent", true)]),
            task_with_deps("grandchild", "Grandchild", "a3", vec![("child", true)]),
        ],
    )
    .await;
    let parent = created[0].id;
    let child = created[1].id;
    let grandchild = created[2].id;

    assert_eq!(pause_task_http(&app, child).await, StatusCode::OK);
    assert_task_status(&app, child, StatusKind::Paused, "child should be Paused").await;

    // Parent fails -> cascade must reach the Paused child AND recurse to grandchild.
    fail_task(&state, parent, "boom").await;
    assert_task_status(
        &app,
        child,
        StatusKind::Failure,
        "Paused child must be cascade-failed by its required parent",
    )
    .await;
    assert_task_status(
        &app,
        grandchild,
        StatusKind::Failure,
        "failure must propagate recursively through the (formerly Paused) child",
    )
    .await;
}

/// A3 test 3 — resume is contextual: a task with outstanding deps resumes to Waiting,
/// a task with all deps met resumes to Pending.
#[tokio::test]
async fn test_audit2_a3_resume_is_contextual() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // Two-parent child.
    let created = create_tasks_ok(
        &app,
        &[
            task_json("p1", "P1", "a3"),
            task_json("p2", "P2", "a3"),
            task_with_deps("child", "Child", "a3", vec![("p1", true), ("p2", true)]),
        ],
    )
    .await;
    let p1 = created[0].id;
    let p2 = created[1].id;
    let child = created[2].id;

    assert_eq!(pause_task_http(&app, child).await, StatusCode::OK);

    // Only ONE parent succeeds -> child still has an outstanding dependency.
    succeed_task(&state, p1).await;
    assert_eq!(read_wait_counters(&state.pool, child).await, (1, 1));

    // Resume -> Waiting (deps still outstanding), NOT Pending.
    assert_eq!(resume_task_http(&app, child).await, StatusCode::OK);
    assert_task_status(
        &app,
        child,
        StatusKind::Waiting,
        "resume with outstanding deps must return to Waiting",
    )
    .await;

    // The remaining parent succeeds -> child (now Waiting) transitions to Pending.
    succeed_task(&state, p2).await;
    assert_task_status(
        &app,
        child,
        StatusKind::Pending,
        "child becomes Pending once all deps are met",
    )
    .await;

    // Simple case: pause a Pending task, resume -> Pending.
    let standalone = create_tasks_ok(&app, &[task_json("solo", "Solo", "a3")]).await;
    let solo = standalone[0].id;
    assert_eq!(solo_status(&app, solo).await, StatusKind::Pending);
    assert_eq!(pause_task_http(&app, solo).await, StatusCode::OK);
    assert_task_status(&app, solo, StatusKind::Paused, "solo should be Paused").await;
    assert_eq!(resume_task_http(&app, solo).await, StatusCode::OK);
    assert_task_status(
        &app,
        solo,
        StatusKind::Pending,
        "resumed Pending task returns to Pending",
    )
    .await;
}

/// Read a task's status via GET /task/{id}.
async fn solo_status<S, B>(app: &S, id: uuid::Uuid) -> StatusKind
where
    S: ActixService<ActixRequest, Response = ActixServiceResponse<B>, Error = actix_web::Error>,
    B: ActixMessageBody,
{
    get_task_ok(app, id).await.status
}

/// A3 test 4 — pause/resume refusals: pausing a Running/Claimed/terminal task and
/// resuming a non-Paused task all return 400.
#[tokio::test]
async fn test_audit2_a3_pause_resume_refusals() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let created = create_tasks_ok(
        &app,
        &[
            task_json("running", "Running", "a3"),
            task_json("claimed", "Claimed", "a3"),
            task_json("terminal", "Terminal", "a3"),
            task_json("pending", "Pending", "a3"),
        ],
    )
    .await;
    let running = created[0].id;
    let claimed = created[1].id;
    let terminal = created[2].id;
    let pending = created[3].id;

    // Running -> pause refused.
    {
        let mut conn = state.pool.get().await.unwrap();
        assert!(
            arcrun::db_operation::claim_task(&mut conn, &running)
                .await
                .unwrap()
        );
        assert!(
            arcrun::db_operation::mark_task_running(&mut conn, &running)
                .await
                .unwrap()
        );
    }
    assert_task_status(&app, running, StatusKind::Running, "should be Running").await;
    assert_eq!(
        pause_task_http(&app, running).await,
        StatusCode::BAD_REQUEST,
        "pausing a Running task must be refused (cancel instead)"
    );

    // Claimed -> pause refused.
    force_claimed(&state, claimed).await;
    assert_task_status(&app, claimed, StatusKind::Claimed, "should be Claimed").await;
    assert_eq!(
        pause_task_http(&app, claimed).await,
        StatusCode::BAD_REQUEST,
        "pausing a Claimed task must be refused"
    );

    // Terminal -> pause refused.
    succeed_task(&state, terminal).await;
    assert_task_status(&app, terminal, StatusKind::Success, "should be Success").await;
    assert_eq!(
        pause_task_http(&app, terminal).await,
        StatusCode::BAD_REQUEST,
        "pausing a terminal task must be refused"
    );

    // Resume of a non-Paused (Pending) task -> refused.
    assert_eq!(
        resume_task_http(&app, pending).await,
        StatusCode::BAD_REQUEST,
        "resuming a non-Paused task must be refused"
    );
}

/// A3 test 5 — the `on_batch_complete` signal still fires when the batch's last
/// non-terminal task is a `Paused` task that gets cascade-failed.
///
/// Batch (object form) with `on_batch_complete`: parent -> child (requires_success).
/// The child is paused, then the parent fails, cascade-failing the Paused child. Both
/// tasks are now terminal, so the batch-complete signal (enqueued via the centralized
/// helper on the parent's terminal transition) must be delivered exactly once. With
/// the cascade-fail-reaches-Paused fix reverted, the child stays Paused (non-terminal)
/// and the signal never fires.
#[tokio::test]
async fn test_audit2_a3_batch_complete_after_cascade_fail_of_paused() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let hits = Arc::new(AtomicUsize::new(0));
    let (webhook_url, shutdown_server) = spawn_webhook_server(hits.clone());

    let body = json!({
        "tasks": [
            task_json("parent", "Parent", "a3bc"),
            task_with_deps("child", "Child", "a3bc", vec![("parent", true)])
        ],
        "on_batch_complete": [
            {"kind": "Webhook", "params": {"url": webhook_url, "verb": "Post"}}
        ]
    });
    let req = actix_web::test::TestRequest::post()
        .uri("/task")
        .insert_header(("requester", "test"))
        .set_json(&body)
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::CREATED);
    let batch_id: uuid::Uuid = resp
        .headers()
        .get("X-Batch-ID")
        .unwrap()
        .to_str()
        .unwrap()
        .parse()
        .unwrap();

    // Identify parent (Pending) and child (Waiting) via the DAG.
    let dag_req = actix_web::test::TestRequest::get()
        .uri(&format!("/dag/{}", batch_id))
        .to_request();
    let dag_resp = actix_web::test::call_service(&app, dag_req).await;
    let dag: serde_json::Value = actix_web::test::read_body_json(dag_resp).await;
    let mut parent = None;
    let mut child = None;
    for t in dag["tasks"].as_array().unwrap() {
        let id = uuid::Uuid::parse_str(t["id"].as_str().unwrap()).unwrap();
        match t["name"].as_str().unwrap() {
            "Parent" => parent = Some(id),
            "Child" => child = Some(id),
            _ => {}
        }
    }
    let parent = parent.unwrap();
    let child = child.unwrap();

    // Pause the Waiting child -> the batch now has a Pending parent + a Paused child
    // (no task terminal yet, no signal).
    assert_eq!(pause_task_http(&app, child).await, StatusCode::OK);
    assert_task_status(&app, child, StatusKind::Paused, "child should be Paused").await;

    // Parent fails -> cascade-fails the Paused child; both terminal -> signal enqueued.
    fail_task(&state, parent, "boom").await;
    assert_task_status(
        &app,
        child,
        StatusKind::Failure,
        "Paused child cascade-failed",
    )
    .await;

    drain_outbox(&state).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "on_batch_complete must fire once after the last non-terminal (Paused) task is cascade-failed"
    );

    let _ = shutdown_server.send(());
}

// =============================================================================
// Audit 2, A8 — inter-request dedupe: check-then-act now guarded by an advisory lock
// =============================================================================
//
// # Original bug
// `handle_dedupe` (`src/db/task_crud.rs`) evaluates `dedupe_strategy` with a
// `COUNT(*)` over the committed snapshot, then the caller INSERTs the task later in
// the SAME transaction. There was no lock spanning that check-then-act window, so two
// concurrent `POST /task` requests carrying the same dedupe key both saw `count == 0`
// and both inserted — producing duplicates despite `dedupe_strategy`.
//
// # Fix
// Before running the counts, `handle_dedupe` now takes a `pg_advisory_xact_lock` on a
// stable hash of each applicable matcher (`rule::dedupe_lock_key` = kind + status +
// the matcher's metadata field values). Because it runs inside the `insert_task_batch`
// transaction, the lock is held until COMMIT/ROLLBACK, so a second concurrent request
// with the same key parks on the lock until the first commits, then observes the
// just-inserted row (`count > 0`) and correctly dedupes. Keys are acquired
// sorted+deduped in one round-trip to keep a consistent global lock order.
//
// # What these tests assert
// * `test_audit2_a8_concurrent_same_key_creates_exactly_one` — N concurrent identical
//   dedupe requests create exactly ONE task total. With the fix reverted this races
//   and usually inserts several duplicates.
// * `test_audit2_a8_concurrent_distinct_keys_all_created` — N concurrent requests with
//   DISTINCT dedupe keys all succeed (the lock serializes only same-key requests, it
//   does not over-block distinct keys).
// * The pre-existing `test_dedupe.rs` guards (bug #7) stay green — the guard branches
//   take no lock and preserve the "allow creation" semantics.

/// Count `task` rows of the given kind (the ground truth for "how many were created").
async fn count_tasks_of_kind(pool: &arcrun::DbPool, kind: &str) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Cnt {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        c: i64,
    }
    let mut conn = pool.get().await.unwrap();
    let r: Cnt = diesel_async::RunQueryDsl::get_result(
        diesel::sql_query("SELECT count(*) AS c FROM task WHERE kind = $1")
            .bind::<diesel::sql_types::Text, _>(kind),
        &mut *conn,
    )
    .await
    .unwrap();
    r.c
}

/// Build a single-task `POST /task` body (bare array) carrying a `dedupe_strategy`
/// that matches on `kind` + `status=Pending` + the `unique_key` metadata field.
fn dedupe_task_body(kind: &str, unique_key: &str) -> serde_json::Value {
    json!([{
        "id": "dedupe-race",
        "name": "A8 Dedupe Race",
        "kind": kind,
        "timeout": 60,
        "metadata": {"unique_key": unique_key},
        "on_start": webhook_action(),
        "dedupe_strategy": [{
            "kind": kind,
            "status": "Pending",
            "fields": ["unique_key"]
        }]
    }])
}

/// A8 race test — N concurrent `POST /task` with the SAME dedupe key create exactly
/// one task. The requests are fired concurrently via `join_all` on the shared test
/// service; a pool larger than N ensures each request holds its own connection (so the
/// advisory lock genuinely serializes them rather than the pool doing it by accident).
#[tokio::test]
async fn test_audit2_a8_concurrent_same_key_creates_exactly_one() {
    // Pool comfortably larger than the concurrency so every in-flight request holds
    // its own connection and blocks on the advisory lock, not on connection checkout.
    const N: usize = 16;
    let test_app = setup_test_db_with_pool_size((N as u32) + 8).await;
    let state = create_test_state(test_app.pool.clone());
    let app = test_service!(state);

    let kind = "a8-same-key";
    let futs = (0..N).map(|_| {
        let body = dedupe_task_body(kind, "same-key");
        let app = &app;
        async move {
            let req = actix_web::test::TestRequest::post()
                .uri("/task")
                .insert_header(("requester", "test"))
                .set_json(&body)
                .to_request();
            actix_web::test::call_service(app, req).await.status()
        }
    });
    let statuses = futures_util::future::join_all(futs).await;

    // Every request returned a well-formed response (201 for the single winner, 204
    // No Content for the deduped losers). No 5xx / deadlock error.
    for s in &statuses {
        assert!(
            *s == StatusCode::CREATED || *s == StatusCode::NO_CONTENT,
            "each concurrent request must return 201 or 204, got {s}"
        );
    }

    // Ground truth: exactly one task of this kind exists in the DB.
    assert_eq!(
        count_tasks_of_kind(&state.pool, kind).await,
        1,
        "exactly one task must be created despite {N} concurrent same-key requests"
    );
}

/// A8 no-over-blocking test — N concurrent `POST /task` with DISTINCT dedupe keys all
/// create their task. The advisory lock keyed on (kind,status,field-values) must not
/// serialize away legitimately-distinct tasks (a hash collision may serialize two of
/// them, but every request must still succeed and produce a row).
#[tokio::test]
async fn test_audit2_a8_concurrent_distinct_keys_all_created() {
    const N: usize = 16;
    let test_app = setup_test_db_with_pool_size((N as u32) + 8).await;
    let state = create_test_state(test_app.pool.clone());
    let app = test_service!(state);

    let kind = "a8-distinct-keys";
    let futs = (0..N).map(|i| {
        let body = dedupe_task_body(kind, &format!("key-{i}"));
        let app = &app;
        async move {
            let req = actix_web::test::TestRequest::post()
                .uri("/task")
                .insert_header(("requester", "test"))
                .set_json(&body)
                .to_request();
            actix_web::test::call_service(app, req).await.status()
        }
    });
    let statuses = futures_util::future::join_all(futs).await;

    // Distinct keys never dedupe -> every request must be a 201 Created.
    for s in &statuses {
        assert_eq!(
            *s,
            StatusCode::CREATED,
            "distinct-key requests must all create a task (got {s})"
        );
    }

    assert_eq!(
        count_tasks_of_kind(&state.pool, kind).await,
        N as i64,
        "all {N} distinct-key tasks must be created (lock must not over-serialize)"
    );
}

// =============================================================================
// Audit 2, A10 — grab-bag: cancel a Waiting task, precise DELETE/PATCH status
//                codes (404/400/409), idempotent PATCH, 400 on bad metadata filter
// =============================================================================
//
// # Original bugs
//   * `cancel_task` rejected a `Waiting` task even though the DELETE endpoint doc
//     advertised it — an operator could not prune a not-yet-eligible DAG branch.
//   * The DELETE handler collapsed every worker error to an empty `400`: a missing
//     id and a DB failure were indistinguishable from a genuine "wrong state".
//   * PATCH was non-idempotent: a 0-row guarded UPDATE always answered `404`, so a
//     client retrying after a lost response could not tell "already applied" from
//     "wrong id", and a wrong-state PATCH never surfaced the current status.
//   * A malformed `?metadata=` filter was parsed with `serde_json::from_str(..).ok()`
//     and silently dropped — the request then returned ALL tasks instead of a 400.
//
// # Fix
//   * `cancel_task` accepts `Waiting` (Canceled == Failed for propagation, so its
//     children still cascade), maps a missing task to `NotFound` (404) and a
//     non-cancelable state to a contextual `InvalidState` (400); the handler now
//     `map_err(ApiError::from)?` so DB failures surface as 500.
//   * `update_running_task` runs a follow-up SELECT on a 0-row UPDATE and returns
//     `NotFound` / `AlreadyInRequestedState` / `Conflict(status)`, which the handler
//     maps to 404 / 200 (idempotent) / 409.
//   * `FilterDto::resolve` returns `Err` on a malformed `metadata`, handled as 400.

/// Count outbox (`webhook_execution`) rows for a task with the given trigger,
/// regardless of status. Used to prove no cancel row is enqueued for a Waiting
/// cancel, and that an idempotent re-PATCH does not duplicate the end row.
async fn outbox_count_by_trigger(pool: &arcrun::DbPool, task_id: uuid::Uuid, trigger: &str) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Cnt {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        c: i64,
    }
    let mut conn = pool.get().await.unwrap();
    let r: Cnt = diesel_async::RunQueryDsl::get_result(
        diesel::sql_query(
            // D3: pending end/cancel rows are in the queue, delivered ones in history;
            // start rows only ever live in the ledger (queue has none) — count both.
            "SELECT (\
                (SELECT count(*) FROM webhook_execution WHERE task_id = $1 AND trigger = $2::trigger_kind) + \
                (SELECT count(*) FROM webhook_outbox    WHERE task_id = $1 AND trigger = $2::trigger_kind)\
             ) AS c",
        )
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .bind::<diesel::sql_types::Text, _>(trigger),
        &mut *conn,
    )
    .await
    .unwrap();
    r.c
}

/// A10 — cancelling a `Waiting` task prunes its DAG branch: the task becomes
/// `Canceled`, its `requires_success` child cascade-fails, and (like a Pending
/// cancel) NO cancel outbox row is enqueued because the task never ran on_start.
/// Later succeeding the parent does not resurrect the pruned branch.
#[tokio::test]
async fn test_audit2_a10_cancel_waiting_prunes_branch() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // parent (Pending) -> child (Waiting) -> grandchild (Waiting), all requires_success.
    // A `sibling` also depends on the parent: it keeps the parent alive after the
    // child is canceled (otherwise dead-end detection would cancel the now-childless
    // parent) so we can later succeed the parent and prove the pruned branch stays dead.
    let tasks = vec![
        task_json("a10-parent", "A10 Parent", "a10-cancel-waiting"),
        task_with_deps(
            "a10-child",
            "A10 Child",
            "a10-cancel-waiting",
            vec![("a10-parent", true)],
        ),
        task_with_deps(
            "a10-grandchild",
            "A10 Grandchild",
            "a10-cancel-waiting",
            vec![("a10-child", true)],
        ),
        task_with_deps(
            "a10-sibling",
            "A10 Sibling",
            "a10-cancel-waiting",
            vec![("a10-parent", true)],
        ),
    ];
    let created = create_tasks_ok(&app, &tasks).await;
    let parent_id = created[0].id;
    let child_id = created[1].id;
    let grandchild_id = created[2].id;
    let sibling_id = created[3].id;

    assert_eq!(created[0].status, StatusKind::Pending);
    assert_eq!(created[1].status, StatusKind::Waiting);
    assert_eq!(created[2].status, StatusKind::Waiting);
    assert_eq!(created[3].status, StatusKind::Waiting);

    // DELETE the Waiting middle task.
    let del = actix_web::test::TestRequest::delete()
        .uri(&format!("/task/{}", child_id))
        .to_request();
    let resp = actix_web::test::call_service(&app, del).await;
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "cancelling a Waiting task must return 200"
    );

    assert_task_status(
        &app,
        child_id,
        StatusKind::Canceled,
        "child must be Canceled",
    )
    .await;
    assert_task_status(
        &app,
        grandchild_id,
        StatusKind::Failure,
        "grandchild must cascade-fail (Canceled == Failed for propagation)",
    )
    .await;

    // No cancel outbox row for the Waiting task — it never received on_start (same
    // contract as a Pending cancel).
    assert_eq!(
        outbox_count_by_trigger(&state.pool, child_id, "cancel").await,
        0,
        "a Waiting cancel must NOT enqueue a cancel webhook (on_start never ran)"
    );

    // Succeeding the parent afterwards must not resurrect the pruned branch, while the
    // healthy sibling proceeds to Pending.
    succeed_task(&state, parent_id).await;
    assert_task_status(
        &app,
        child_id,
        StatusKind::Canceled,
        "child stays Canceled after parent success",
    )
    .await;
    assert_task_status(
        &app,
        grandchild_id,
        StatusKind::Failure,
        "grandchild stays Failure after parent success",
    )
    .await;
    assert_task_status(
        &app,
        sibling_id,
        StatusKind::Pending,
        "healthy sibling proceeds to Pending after parent success",
    )
    .await;
}

/// A10 — DELETE precise status codes: a missing id → 404, a terminal task → 400
/// with a message naming the current state (not a bare/empty 400).
#[tokio::test]
async fn test_audit2_a10_delete_status_codes() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // Unknown id → 404.
    let missing = uuid::Uuid::new_v4();
    let del_missing = actix_web::test::TestRequest::delete()
        .uri(&format!("/task/{}", missing))
        .to_request();
    let resp = actix_web::test::call_service(&app, del_missing).await;
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "DELETE of an unknown id must return 404"
    );

    // Drive a task to a terminal (Success) state, then DELETE it → 400 + message.
    let created = create_tasks_ok(&app, &[task_json("a10-term", "A10 Terminal", "a10-del")]).await;
    let task_id = created[0].id;
    succeed_task(&state, task_id).await;
    assert_task_status(&app, task_id, StatusKind::Success, "task should be Success").await;

    let del_term = actix_web::test::TestRequest::delete()
        .uri(&format!("/task/{}", task_id))
        .to_request();
    let resp = actix_web::test::call_service(&app, del_term).await;
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "DELETE of a terminal task must return 400"
    );
    let body: serde_json::Value = actix_web::test::read_body_json(resp).await;
    let msg = body["error"].as_str().unwrap_or_default().to_lowercase();
    assert!(
        msg.contains("cannot cancel") && msg.contains("success"),
        "400 body must name the non-cancelable state, got: {}",
        body["error"]
    );
}

/// A10 — a malformed `?metadata=` filter is a hard 400, not a silent "return
/// everything". A valid JSON-object metadata filter still returns 200.
#[tokio::test]
async fn test_audit2_a10_invalid_metadata_filter_is_400() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // Seed a task so an "ignored filter" would visibly leak rows.
    let _ = create_tasks_ok(
        &app,
        &[task_with_metadata(
            "a10-meta",
            "A10 Meta",
            "a10-meta",
            json!({"env": "prod"}),
        )],
    )
    .await;

    // Malformed JSON (`{bad`) → 400.
    let bad = actix_web::test::TestRequest::get()
        .uri("/task?metadata=%7Bbad")
        .to_request();
    let resp = actix_web::test::call_service(&app, bad).await;
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "a malformed metadata filter must return 400"
    );

    // Valid JSON object → 200.
    let good = actix_web::test::TestRequest::get()
        .uri("/task?metadata=%7B%22env%22%3A%22prod%22%7D")
        .to_request();
    let resp = actix_web::test::call_service(&app, good).await;
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "a valid metadata filter must still return 200"
    );
}

/// A10 — PATCH is idempotent and retry-safe: PATCH Success → 200; re-PATCH Success
/// → 200 no-op (exactly one end outbox row, no duplicate); PATCH Failure on the
/// now-terminal task → 409 with the current status; PATCH on an unknown id → 404.
#[tokio::test]
async fn test_audit2_a10_patch_idempotent_and_conflict() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let created = create_tasks_ok(&app, &[task_json("a10-patch", "A10 Patch", "a10-patch")]).await;
    let task_id = created[0].id;

    // Bring the task to Running so the first PATCH transitions it.
    {
        let mut conn = state.pool.get().await.unwrap();
        assert!(
            arcrun::db_operation::claim_task(&mut conn, &task_id)
                .await
                .unwrap()
        );
        assert!(
            arcrun::db_operation::mark_task_running(&mut conn, &task_id)
                .await
                .unwrap()
        );
    }

    // First PATCH Success → 200 (real transition).
    let patch_success = || {
        actix_web::test::TestRequest::patch()
            .uri(&format!("/task/{}", task_id))
            .set_json(json!({"status": "Success"}))
            .to_request()
    };
    let resp = actix_web::test::call_service(&app, patch_success()).await;
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "first PATCH Success must be 200"
    );

    // Re-PATCH Success → 200 idempotent no-op.
    let resp = actix_web::test::call_service(&app, patch_success()).await;
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "re-PATCH of the same status must be an idempotent 200"
    );

    // Exactly ONE end outbox row — the no-op did not re-enqueue.
    assert_eq!(
        outbox_count_by_trigger(&state.pool, task_id, "end").await,
        1,
        "an idempotent re-PATCH must NOT duplicate the end outbox row"
    );

    // PATCH Failure on the now-Success task → 409 with the current status.
    let patch_fail = actix_web::test::TestRequest::patch()
        .uri(&format!("/task/{}", task_id))
        .set_json(json!({"status": "Failure", "failure_reason": "late"}))
        .to_request();
    let resp = actix_web::test::call_service(&app, patch_fail).await;
    assert_eq!(
        resp.status(),
        StatusCode::CONFLICT,
        "PATCH to a different status on a terminal task must be 409"
    );
    let body: serde_json::Value = actix_web::test::read_body_json(resp).await;
    assert_eq!(
        body["current_status"], "Success",
        "409 body must carry the current status, got: {}",
        body
    );

    // PATCH on an unknown id → 404.
    let missing = uuid::Uuid::new_v4();
    let patch_missing = actix_web::test::TestRequest::patch()
        .uri(&format!("/task/{}", missing))
        .set_json(json!({"status": "Success"}))
        .to_request();
    let resp = actix_web::test::call_service(&app, patch_missing).await;
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "PATCH on an unknown id must be 404"
    );
}

// =============================================================================
// Audit 2, A9 — deadlocks on multi-row UPDATEs (diamond propagation)
// =============================================================================
//
// # Original bug
// `propagate_to_children` marks/decrements children with batched
// `UPDATE task … WHERE id = ANY($ids)` statements. Postgres locks the matched rows
// in the query planner's order, NOT the array order. In a "diamond" DAG (two parents
// sharing the same set of children), two concurrent parent transitions — a PATCH
// Success on parent A and a PATCH Success/Failure on parent B — could acquire the
// shared child-row locks in opposite orders and deadlock (`40P01`). Postgres aborts
// one transaction, `run_in_transaction` surfaces it as a DB error, and the PATCH
// returns 500. (The batch-updater flush had the same exposure crossing a
// propagation.)
//
// # Fix
// Before any batched UPDATE, `propagate_to_children` pre-locks the whole level's
// child set once with `SELECT id … WHERE id = ANY(...) ORDER BY id FOR UPDATE`,
// giving every propagation a single canonical lock-acquisition order, so two
// concurrent diamond propagations queue on the same ordered locks instead of
// cycling. The batch-updater flush was hardened symmetrically (sorted ids +
// ordered pre-lock in a transaction; a residual `40P01` falls back to the
// deadlock-free per-row path).
//
// # What these tests assert
// * `test_audit2_a9_concurrent_diamond_success_no_deadlock` — many diamonds whose
//   two parents are PATCHed Success concurrently: no request returns 500, and every
//   shared child ends `Pending` with `wait_finished = wait_success = 0`.
// * `test_audit2_a9_concurrent_diamond_mixed_fail_success_no_deadlock` — one parent
//   fails while the other succeeds, concurrently: no 500, and every `requires_success`
//   child ends `Failure` (Canceled/Failure cascade wins over the success decrement).
//
// The deadlock is probabilistic pre-fix, so the tests are made aggressive (many
// diamonds, wide shared child fan-out, fired via `join_all`) while their assertions
// are deterministic (the A9 contract: no 500, correct final state).

/// Bring a Pending task straight to `Running` (claim + mark_running, no on_start),
/// so a subsequent PATCH transitions it terminal.
async fn force_running(state: &arcrun::handlers::AppState, task_id: uuid::Uuid) {
    let mut conn = state.pool.get().await.unwrap();
    assert!(
        arcrun::db_operation::claim_task(&mut conn, &task_id)
            .await
            .unwrap(),
        "task {task_id} should be claimable"
    );
    assert!(
        arcrun::db_operation::mark_task_running(&mut conn, &task_id)
            .await
            .unwrap(),
        "task {task_id} should be markable running"
    );
}

/// PATCH /task/{id} with the given status body, returning the HTTP status.
fn patch_status_req(task_id: uuid::Uuid, body: serde_json::Value) -> ActixRequest {
    actix_web::test::TestRequest::patch()
        .uri(&format!("/task/{}", task_id))
        .set_json(body)
        .to_request()
}

#[tokio::test]
async fn test_audit2_a9_concurrent_diamond_success_no_deadlock() {
    // Diamonds: parent_a[i] and parent_b[i] both feed M shared children, all in one
    // batch. N parents pairs => 2*N concurrent PATCH Success requests contend on the
    // shared child rows.
    const N: usize = 12;
    const M: usize = 10;

    let test_app = setup_test_db_with_pool_size((2 * N as u32) + 8).await;
    let state = create_test_state(test_app.pool.clone());
    let app = test_service!(state);

    let kind = "a9-diamond-success";
    let mut tasks: Vec<serde_json::Value> = Vec::new();
    for i in 0..N {
        let pa = format!("pa-{i}");
        let pb = format!("pb-{i}");
        tasks.push(task_json(&pa, &pa, kind));
        tasks.push(task_json(&pb, &pb, kind));
        for j in 0..M {
            let cid = format!("c-{i}-{j}");
            tasks.push(task_with_deps(
                &cid,
                &cid,
                kind,
                vec![(&pa, true), (&pb, true)],
            ));
        }
    }
    let created = create_tasks_ok(&app, &tasks).await;

    // Map local name -> server uuid.
    let by_name: std::collections::HashMap<String, uuid::Uuid> =
        created.iter().map(|t| (t.name.clone(), t.id)).collect();

    // Bring all parents to Running (sequentially — the contention we test is on the
    // concurrent terminal PATCHes, not the setup).
    let mut parent_ids: Vec<uuid::Uuid> = Vec::new();
    for i in 0..N {
        for p in [format!("pa-{i}"), format!("pb-{i}")] {
            let id = by_name[&p];
            force_running(&state, id).await;
            parent_ids.push(id);
        }
    }

    // Fire every parent's PATCH Success concurrently.
    let futs = parent_ids.iter().map(|id| {
        let app = &app;
        let id = *id;
        async move {
            let resp = actix_web::test::call_service(
                app,
                patch_status_req(id, json!({"status": "Success"})),
            )
            .await;
            resp.status()
        }
    });
    let statuses = futures_util::future::join_all(futs).await;

    for s in &statuses {
        assert_ne!(
            *s,
            StatusCode::INTERNAL_SERVER_ERROR,
            "no PATCH may 500 (deadlock) under concurrent diamond propagation, got {s}"
        );
        assert_eq!(
            *s,
            StatusCode::OK,
            "each Running->Success PATCH must be a 200 transition, got {s}"
        );
    }

    // Every shared child received both decrements -> Pending, counters at 0/0.
    for i in 0..N {
        for j in 0..M {
            let cid = by_name[&format!("c-{i}-{j}")];
            let (wf, ws) = read_wait_counters(&state.pool, cid).await;
            assert_eq!(
                (wf, ws),
                (0, 0),
                "child c-{i}-{j} must have both wait counters decremented to 0"
            );
            assert_task_status(
                &app,
                cid,
                StatusKind::Pending,
                "child must reach Pending after both parents succeed",
            )
            .await;
        }
    }
}

#[tokio::test]
async fn test_audit2_a9_concurrent_diamond_mixed_fail_success_no_deadlock() {
    // Same diamond topology, but parent_a fails while parent_b succeeds, concurrently.
    const N: usize = 12;
    const M: usize = 10;

    let test_app = setup_test_db_with_pool_size((2 * N as u32) + 8).await;
    let state = create_test_state(test_app.pool.clone());
    let app = test_service!(state);

    let kind = "a9-diamond-mixed";
    let mut tasks: Vec<serde_json::Value> = Vec::new();
    for i in 0..N {
        let pa = format!("pa-{i}");
        let pb = format!("pb-{i}");
        tasks.push(task_json(&pa, &pa, kind));
        tasks.push(task_json(&pb, &pb, kind));
        // Per-parent keepalive children (requires_success=false, NOT shared): they
        // stay non-terminal (→ Pending) through the parent's transition, so neither
        // parent ever becomes a dead-end ancestor once the shared children fail. This
        // isolates the test from dead-end cancellation (which would otherwise cancel
        // the still-Running parent_b and turn its PATCH into a 409) WITHOUT relieving
        // the shared-child lock contention that A9 is about.
        tasks.push(task_with_deps(
            &format!("ka-{i}"),
            &format!("ka-{i}"),
            kind,
            vec![(&pa, false)],
        ));
        tasks.push(task_with_deps(
            &format!("kb-{i}"),
            &format!("kb-{i}"),
            kind,
            vec![(&pb, false)],
        ));
        for j in 0..M {
            let cid = format!("c-{i}-{j}");
            tasks.push(task_with_deps(
                &cid,
                &cid,
                kind,
                vec![(&pa, true), (&pb, true)],
            ));
        }
    }
    let created = create_tasks_ok(&app, &tasks).await;
    let by_name: std::collections::HashMap<String, uuid::Uuid> =
        created.iter().map(|t| (t.name.clone(), t.id)).collect();

    // Bring all parents to Running.
    for i in 0..N {
        for p in [format!("pa-{i}"), format!("pb-{i}")] {
            force_running(&state, by_name[&p]).await;
        }
    }

    // Fire: parent_a[i] -> Failure and parent_b[i] -> Success, all concurrent.
    let mut reqs: Vec<(uuid::Uuid, serde_json::Value)> = Vec::new();
    for i in 0..N {
        reqs.push((
            by_name[&format!("pa-{i}")],
            json!({"status": "Failure", "failure_reason": "a9-mixed"}),
        ));
        reqs.push((by_name[&format!("pb-{i}")], json!({"status": "Success"})));
    }
    let futs = reqs.into_iter().map(|(id, body)| {
        let app = &app;
        async move {
            let resp = actix_web::test::call_service(app, patch_status_req(id, body)).await;
            resp.status()
        }
    });
    let statuses = futures_util::future::join_all(futs).await;

    for s in &statuses {
        assert_ne!(
            *s,
            StatusCode::INTERNAL_SERVER_ERROR,
            "no PATCH may 500 (deadlock) under concurrent mixed diamond propagation, got {s}"
        );
        assert_eq!(
            *s,
            StatusCode::OK,
            "each Running->terminal PATCH must be a 200 transition, got {s}"
        );
    }

    // Every child requires_success from the failed parent_a -> cascade-fail to Failure,
    // regardless of the interleaving with parent_b's success decrement.
    for i in 0..N {
        for j in 0..M {
            let cid = by_name[&format!("c-{i}-{j}")];
            assert_task_status(
                &app,
                cid,
                StatusKind::Failure,
                "child must cascade-fail from the required parent_a failure",
            )
            .await;
        }
    }
}

// ============================================================================
// Audit 2, A5 — SSRF: IPv6-literal bypass + DNS-rebinding
// ============================================================================
//
// # Original bug
// 1. IPv6 literals bypassed creation-time validation entirely: `url.host_str()`
//    returns the *bracketed* form (`"[::1]"`), so `host.parse::<IpAddr>()` always
//    failed and `is_internal_ip` was never consulted. In release, `http://[::1]/`,
//    `http://[fd00::1]/`, `http://[::ffff:10.0.0.1]/` all passed.
// 2. DNS rebinding: validation only ever saw the *name*; reqwest re-resolved it at
//    delivery time (possibly across retries hours later). Register a domain -> a
//    public IP to pass validation, then flip it to `169.254.169.254` before delivery.
//
// # Fix
// 1. `validate_webhook_url_with_config` now matches on `url.host()` (parsed
//    `Host::Ipv6`) and `is_internal_ip` unwraps IPv4-mapped v6 (`::ffff:a.b.c.d`).
// 2. `ActionExecutor`, when SSRF validation is active, installs a custom reqwest
//    DNS resolver (`SsrfGuardResolver`) that re-checks every resolved IP at request
//    time and refuses to connect if any is internal — closing the rebinding window.
//
// # What these tests assert
// * `test_audit2_a5_ipv6_literal_rejected_by_validation` — the creation-time gate
//   (the exact function `POST /task` runs) rejects IPv6 loopback / ULA / link-local
//   / IPv4-mapped-private literals under a strict config, and still accepts a public
//   IPv6. We call the config-injected validator directly because the integration
//   binary shares ONE process-global SSRF `OnceLock` across ~50 tests that webhook
//   to 127.0.0.1; flipping the global to strict would break all of them. This is the
//   same code path `add_task` invokes via `validate_action_params`.
// * `test_audit2_a5_resolver_blocks_rebinding_to_internal` — a REAL local mock
//   webhook server is reached via the hostname `localhost` (which resolves to a
//   loopback IP): the strict executor must fail the delivery WITHOUT ever hitting
//   the server (hits stays 0 — proves the block happened at resolution, not at
//   connect), while a skip-SSRF executor delivers the very same URL successfully
//   (hits == 1 — the control proving the resolver is the only difference). This
//   exercises the anti-rebinding resolver without any external network/DNS, and
//   goes red if the resolver is not installed (the strict delivery would succeed).

#[test]
fn test_audit2_a5_ipv6_literal_rejected_by_validation() {
    use arcrun::config::SecurityConfig;
    use arcrun::validation::validate_webhook_url_with_config;

    let strict = SecurityConfig {
        skip_ssrf_validation: false,
        ..SecurityConfig::default()
    };

    // Rejected: IPv6 internal/reserved literals that used to slip through in release.
    for url in [
        "http://[::1]:8085/hook",
        "http://[fd00::1]/hook",
        "http://[fe80::1]/hook",
        "http://[::ffff:10.0.0.1]/hook",
        "http://[::ffff:169.254.169.254]/hook",
    ] {
        assert!(
            validate_webhook_url_with_config(url, &strict).is_err(),
            "IPv6 literal must be rejected under strict SSRF: {url}"
        );
    }

    // Accepted: a genuine public IPv6 (Cloudflare DNS).
    assert!(
        validate_webhook_url_with_config("https://[2606:4700:4700::1111]/hook", &strict).is_ok(),
        "public IPv6 literal must pass strict SSRF"
    );

    // Control: with SSRF skipped, the literal parses fine and is allowed (the
    // historical debug/test behaviour every other integration test relies on).
    let skip = SecurityConfig {
        skip_ssrf_validation: true,
        ..SecurityConfig::default()
    };
    assert!(validate_webhook_url_with_config("http://[::1]:8085/hook", &skip).is_ok());
}

#[tokio::test]
async fn test_audit2_a5_resolver_blocks_rebinding_to_internal() {
    use arcrun::action::{ActionContext, ActionExecutor};
    use arcrun::config::SecurityConfig;
    use arcrun::dtos::NewActionDto;
    use arcrun::models::ActionKindEnum;

    let ctx = || ActionContext {
        host_address: "http://localhost:8080".to_string(),
        webhook_idempotency_timeout: std::time::Duration::from_secs(30),
    };

    // A real mock webhook server on 127.0.0.1, reached via the *hostname*
    // `localhost` (which the system resolver maps to a loopback IP). Using a
    // name — not an IP literal — is what routes the request through the DNS
    // resolver under test; the live server is what makes the assertions sharp:
    // a plain connection failure (e.g. a closed port) would be indistinguishable
    // from the resolver block.
    let hits = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (url, _shutdown) = spawn_webhook_server(hits.clone());
    let url_by_name = url.replace("127.0.0.1", "localhost");
    let action = NewActionDto {
        kind: ActionKindEnum::Webhook,
        params: serde_json::json!({ "url": url_by_name, "verb": "Post" }),
    };

    // Strict executor: the resolver sees `localhost` resolve to an internal IP
    // and refuses — the delivery errors and the server is NEVER contacted.
    let strict = SecurityConfig {
        skip_ssrf_validation: false,
        ..SecurityConfig::default()
    };
    let strict_exec = ActionExecutor::with_security_config(ctx(), &strict);
    let res = strict_exec
        .execute_batch_action(&action, None, serde_json::json!({}))
        .await;
    assert!(
        res.is_err(),
        "strict resolver must refuse delivery to a name resolving to an internal IP"
    );
    assert_eq!(
        hits.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "the block must happen at resolution time — the server must never be hit"
    );

    // Control: a skip-SSRF executor (stock resolver) delivers the very same URL
    // successfully — proving the resolver is the only thing blocking above.
    let skip = SecurityConfig {
        skip_ssrf_validation: true,
        ..SecurityConfig::default()
    };
    let relaxed_exec = ActionExecutor::with_security_config(ctx(), &skip);
    let res = relaxed_exec
        .execute_batch_action(&action, None, serde_json::json!({}))
        .await;
    assert!(
        res.is_ok(),
        "skip-SSRF executor must deliver to the same URL (control): {res:?}"
    );
    assert_eq!(
        hits.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the control delivery must actually reach the mock server"
    );
}

// -----------------------------------------------------------------------------
// A5 follow-up — SSRF allowlist (ALLOWED_HOSTNAMES / ALLOWED_CIDRS)
// -----------------------------------------------------------------------------
//
// # Original bug (prod incident 2026-09-30)
// Upgrading from v1.2.0 to an image carrying A5 made every on_start webhook that
// targets an internal service by name (`http://backend:8000/hook`, a docker/k8s
// service resolving to a private IP) fail at delivery: creation-time validation
// accepted the name (not on any blocklist), then `SsrfGuardResolver` refused the
// resolved private IP and the task went to Failure. Public https URLs kept working,
// which made it look like an http-vs-https problem. There was no way to trust an
// internal target short of `SKIP_SSRF_VALIDATION=1`, which drops every check.
//
// # Fix
// `SecurityConfig::allowed_hostnames` (exact or `.suffix`) and `allowed_cidrs`
// are honoured by BOTH gates: `validate_webhook_url_with_config` (creation) and
// the resolver (delivery). An allowlisted hostname is resolved without an IP
// check; a resolved IP inside an allowed CIDR is accepted although private.
// Everything not allowlisted keeps the A5 behaviour.
//
// # What the test asserts
// Same real mock server / `localhost` setup as the rebinding test above:
// * strict + `ALLOWED_HOSTNAMES=localhost` ⇒ creation-time validation accepts the
//   URL (it is on the default blocklist otherwise) and the delivery reaches the
//   server (hits == 1);
// * strict + `ALLOWED_CIDRS=127.0.0.0/8` ⇒ the resolver accepts the loopback IP
//   `localhost` resolves to and the delivery reaches the server (hits == 2);
// * strict with an allowlist that does NOT cover the target (another hostname,
//   another network) ⇒ still refused at resolution (hits stays 2).
// Goes red if either gate ignores the allowlist.

#[tokio::test]
async fn test_audit2_a5_allowlist_permits_trusted_internal_target() {
    use arcrun::action::{ActionContext, ActionExecutor};
    use arcrun::config::{SecurityConfig, parse_cidr_list};
    use arcrun::dtos::NewActionDto;
    use arcrun::models::ActionKindEnum;
    use arcrun::validation::validate_webhook_url_with_config;

    let ctx = || ActionContext {
        host_address: "http://localhost:8080".to_string(),
        webhook_idempotency_timeout: std::time::Duration::from_secs(30),
    };
    let hits = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (url, _shutdown) = spawn_webhook_server(hits.clone());
    let url_by_name = url.replace("127.0.0.1", "localhost");
    let action = NewActionDto {
        kind: ActionKindEnum::Webhook,
        params: serde_json::json!({ "url": url_by_name, "verb": "Post" }),
    };
    let strict = || SecurityConfig {
        skip_ssrf_validation: false,
        ..SecurityConfig::default()
    };

    // 1. Hostname allowlist: both gates open for the trusted name.
    let by_host = SecurityConfig {
        allowed_hostnames: vec!["LOCALHOST".to_string()],
        ..strict()
    };
    assert!(
        validate_webhook_url_with_config(&url_by_name, &by_host).is_ok(),
        "allowlisted hostname must pass creation-time validation despite the blocklist"
    );
    let res = ActionExecutor::with_security_config(ctx(), &by_host)
        .execute_batch_action(&action, None, serde_json::json!({}))
        .await;
    assert!(
        res.is_ok(),
        "allowlisted hostname must be delivered: {res:?}"
    );
    assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 1);

    // 2. CIDR allowlist: the resolver accepts the private IP the name resolves to.
    let by_cidr = SecurityConfig {
        allowed_cidrs: parse_cidr_list("ALLOWED_CIDRS", "127.0.0.0/8,::1").unwrap(),
        ..strict()
    };
    let res = ActionExecutor::with_security_config(ctx(), &by_cidr)
        .execute_batch_action(&action, None, serde_json::json!({}))
        .await;
    assert!(
        res.is_ok(),
        "IP inside an allowed CIDR must be delivered: {res:?}"
    );
    assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 2);

    // 3. An allowlist that does not cover the target changes nothing.
    let unrelated = SecurityConfig {
        allowed_hostnames: vec!["backend".to_string(), ".svc.cluster.local".to_string()],
        allowed_cidrs: parse_cidr_list("ALLOWED_CIDRS", "10.42.0.0/16").unwrap(),
        ..strict()
    };
    let res = ActionExecutor::with_security_config(ctx(), &unrelated)
        .execute_batch_action(&action, None, serde_json::json!({}))
        .await;
    assert!(
        res.is_err(),
        "non-allowlisted internal target must still be refused"
    );
    let err = res.unwrap_err();
    assert!(
        err.contains("SSRF") && err.contains("ALLOWED_HOSTNAMES"),
        "the delivery error must surface the resolver's reason, got: {err}"
    );
    assert_eq!(
        hits.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "the refused delivery must never reach the server"
    );
}

// =============================================================================
// Audit 2, A6 — static bearer-token authentication
// =============================================================================
//
// # Original gap
// The only middleware was the Prometheus recorder, so every endpoint was open:
// anyone reachable on the network could create tasks with arbitrary outbound
// webhooks (an SSRF/DoS launcher even with A5's delivery-time checks), cancel or
// stop any batch, read all task metadata, and scrape /metrics and Swagger.
//
// # Fix
// An optional static bearer token (`AUTH_TOKEN`). When set, an actix
// `from_fn(auth::authorize)` middleware requires `Authorization: Bearer <token>`
// on every request EXCEPT the `/health` and `/ready` probes, comparing the token
// in constant time. When unset the middleware is a total pass-through (the
// historical open behavior), so the 200+ existing tests — which build the app via
// `test_service!` with no middleware — keep passing.
//
// # What these tests assert
// * a business endpoint (GET /task) is 401 with no header and with a wrong token,
//   and 200 with the correct token;
// * `/health` and `/ready` stay reachable WITHOUT a token while auth is active;
// * with auth disabled (token `None`) the same endpoint is reachable with no
//   header (smoke test mirroring the `test_service!` path used everywhere else).

/// Build an actix test service wrapped with the A6 bearer-auth middleware.
/// `$token` is the `Option<String>` the middleware is configured with.
macro_rules! authed_service {
    ($state:expr, $token:expr) => {{
        let token: Option<String> = $token;
        actix_web::test::init_service(
            actix_web::App::new()
                .app_data(actix_web::web::Data::new($state.clone()))
                .wrap(actix_web::middleware::from_fn(move |req, next| {
                    arcrun::auth::authorize(token.clone(), req, next)
                }))
                .configure(arcrun::handlers::configure_routes),
        )
        .await
    }};
}

#[tokio::test]
async fn test_audit2_a6_business_endpoint_requires_token() {
    let (_g, state) = setup_test_app().await;
    let app = authed_service!(state, Some("secret-token".to_string()));

    // No Authorization header → 401.
    let req = actix_web::test::TestRequest::get()
        .uri("/task")
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(
        resp.status().as_u16(),
        401,
        "GET /task without a token must be rejected"
    );
    let body: serde_json::Value = actix_web::test::read_body_json(resp).await;
    assert_eq!(body["status"], 401);
    assert!(
        body["error"].as_str().is_some(),
        "401 body must carry an `error` field (consistent with ApiError)"
    );

    // Wrong token → 401.
    let req = actix_web::test::TestRequest::get()
        .uri("/task")
        .insert_header(("Authorization", "Bearer wrong-token"))
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(
        resp.status().as_u16(),
        401,
        "GET /task with a wrong token must be rejected"
    );

    // Malformed scheme → 401.
    let req = actix_web::test::TestRequest::get()
        .uri("/task")
        .insert_header(("Authorization", "secret-token"))
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(
        resp.status().as_u16(),
        401,
        "a non-Bearer Authorization header must be rejected"
    );

    // Correct token → 200.
    let req = actix_web::test::TestRequest::get()
        .uri("/task")
        .insert_header(("Authorization", "Bearer secret-token"))
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(
        resp.status().as_u16(),
        200,
        "GET /task with the correct token must succeed"
    );
}

#[tokio::test]
async fn test_audit2_a6_health_and_ready_exempt_when_auth_active() {
    let (_g, state) = setup_test_app().await;
    let app = authed_service!(state, Some("secret-token".to_string()));

    for path in ["/health", "/ready"] {
        let req = actix_web::test::TestRequest::get().uri(path).to_request();
        let resp = actix_web::test::call_service(&app, req).await;
        assert!(
            resp.status().is_success(),
            "{path} must remain reachable WITHOUT a token while auth is active (got {})",
            resp.status()
        );
    }
}

/// The Prometheus middleware SERVES `/metrics` itself, so the auth middleware
/// only gates it if it sits OUTSIDE prometheus. In actix the LAST registered
/// `.wrap()` is the outermost and runs first — main.rs therefore registers
/// `auth` after `prometheus`. This test mirrors that exact wrap order and
/// asserts `/metrics` is 401 without the token; with the order flipped (the
/// bug caught in review: auth registered first ⇒ prometheus answers before
/// auth runs) it goes red.
#[tokio::test]
async fn test_audit2_a6_metrics_endpoint_gated_by_auth() {
    let (_g, state) = setup_test_app().await;

    // Fresh registry: the global one (metrics::REGISTRY) must not be re-registered
    // into by parallel tests.
    let prometheus = actix_web_prom::PrometheusMetricsBuilder::new("test_a6_auth")
        .endpoint("/metrics")
        .registry(prometheus::Registry::new())
        .build()
        .unwrap();

    let token = Some("secret-token".to_string());
    let app = actix_web::test::init_service(
        actix_web::App::new()
            .app_data(actix_web::web::Data::new(state.clone()))
            // Same order as main.rs: prometheus first, auth LAST (⇒ outermost).
            .wrap(prometheus)
            .wrap(actix_web::middleware::from_fn(move |req, next| {
                arcrun::auth::authorize(token.clone(), req, next)
            }))
            .configure(arcrun::handlers::configure_routes),
    )
    .await;

    // Without a token, /metrics must be blocked BEFORE prometheus can serve it.
    let req = actix_web::test::TestRequest::get()
        .uri("/metrics")
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(
        resp.status().as_u16(),
        401,
        "/metrics must be gated by auth (prometheus serves it only past the auth wrap)"
    );

    // With the token it is served normally.
    let req = actix_web::test::TestRequest::get()
        .uri("/metrics")
        .insert_header(("Authorization", "Bearer secret-token"))
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(
        resp.status().as_u16(),
        200,
        "/metrics must be served with the correct token"
    );
}

#[tokio::test]
async fn test_audit2_a6_disabled_passes_through() {
    // Token None ⇒ middleware is a total pass-through: no header required.
    let (_g, state) = setup_test_app().await;
    let app = authed_service!(state, None);

    let req = actix_web::test::TestRequest::get()
        .uri("/task")
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(
        resp.status().as_u16(),
        200,
        "with auth disabled, GET /task must succeed without any token"
    );
}

// ---------------------------------------------------------------------------
// A10 — structural limits on POST /task (batch size, deps/task, actions/task),
// explicit JSON payload cap, chunked grouped INSERTs, redundant DFS removed.
// ---------------------------------------------------------------------------
//
// # Original gap (Audit 2, A10)
// POST /task had NO structural limits: neither a cap on tasks-per-batch, deps-per-task,
// nor actions-per-task, and only actix's implicit ~2 MiB JSON cap as a backstop.
// Consequences:
//   * ~5 000 dedupe-free tasks (or a smaller batch that multiplied out via links/actions)
//     exceeded PostgreSQL's 65 535 bind-parameter ceiling on the grouped INSERTs → 500.
//   * The recursive DFS cycle-detection was a stack-overflow DoS vector on a long chain
//     of tasks — and redundant, since the forward-reference rule ("a dependency must
//     appear before the task in the batch") already makes cycles impossible.
//
// # Fix
//   * `MAX_TASKS_PER_BATCH` (1000), `MAX_DEPS_PER_TASK` (100), `MAX_ACTIONS_PER_TASK`
//     (20) env-configurable limits, enforced in validation → 400 with the limit and the
//     received value in the message.
//   * Explicit `web::JsonConfig` limit (`PAYLOAD_MAX_BYTES`, default 2 MiB) → 413.
//   * The grouped multi-row INSERTs in `flush_run` are chunked so `rows * binds_per_row`
//     stays under a conservative budget (in the SAME transaction — atomicity unchanged).
//   * The recursive DFS is removed; the forward-reference check is the single guarantee
//     that rejects unknown-id and would-be-cyclic references.
//
// The tests use the DEFAULT limits (the test app never calls `init_limits_config`, so
// `get_limits_config()` returns the defaults — above anything the rest of the suite
// constructs), building payloads just over each threshold.

/// A minimal task JSON with only the mandatory fields (keeps a 1000+ task batch cheap).
fn min_task(id: &str) -> serde_json::Value {
    json!({
        "id": id,
        "name": id,
        "kind": "a10-limits",
        "timeout": 60,
        "on_start": webhook_action(),
    })
}

/// POST /task with a raw body, returning the response status code.
async fn post_task_status<S, B>(app: &S, body: &serde_json::Value) -> StatusCode
where
    S: ActixService<ActixRequest, Response = ActixServiceResponse<B>, Error = actix_web::Error>,
    B: ActixMessageBody,
{
    let req = actix_web::test::TestRequest::post()
        .uri("/task")
        .set_json(body)
        .to_request();
    actix_web::test::call_service(app, req).await.status()
}

/// A batch of exactly `MAX_TASKS_PER_BATCH + 1` (= 1001) tasks must be rejected 400.
/// Exactly at the limit (1000) is exercised by the bind-params test below.
#[tokio::test]
async fn test_audit2_a10_limits_batch_size_rejected() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let tasks: Vec<serde_json::Value> = (0..1001).map(|i| min_task(&format!("t{i}"))).collect();
    let body = serde_json::Value::Array(tasks);

    assert_eq!(
        post_task_status(&app, &body).await,
        StatusCode::BAD_REQUEST,
        "a batch over MAX_TASKS_PER_BATCH (1001 > 1000) must be rejected with 400"
    );
}

/// A single task declaring `MAX_DEPS_PER_TASK + 1` (= 101) dependencies → 400.
#[tokio::test]
async fn test_audit2_a10_limits_deps_per_task_rejected() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // 101 real parents defined before the child (so the ONLY violation is the deps cap,
    // not a forward/unknown reference).
    let mut tasks: Vec<serde_json::Value> = (0..101).map(|i| min_task(&format!("p{i}"))).collect();
    let deps: Vec<serde_json::Value> = (0..101)
        .map(|i| json!({"id": format!("p{i}"), "requires_success": true}))
        .collect();
    let mut child = min_task("child");
    child["dependencies"] = serde_json::Value::Array(deps);
    tasks.push(child);

    assert_eq!(
        post_task_status(&app, &serde_json::Value::Array(tasks)).await,
        StatusCode::BAD_REQUEST,
        "a task with 101 dependencies (> MAX_DEPS_PER_TASK = 100) must be rejected with 400"
    );
}

/// A single task with `MAX_ACTIONS_PER_TASK + 1` total actions → 400.
/// on_start (1) + 20 on_success = 21 > 20.
#[tokio::test]
async fn test_audit2_a10_limits_actions_per_task_rejected() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let on_success: Vec<serde_json::Value> = (0..20)
        .map(|_| json!({"kind": "Webhook", "params": {"url": "https://ex.co/w", "verb": "Post"}}))
        .collect();
    let mut task = min_task("many-actions");
    task["on_success"] = serde_json::Value::Array(on_success);

    assert_eq!(
        post_task_status(&app, &serde_json::Value::Array(vec![task])).await,
        StatusCode::BAD_REQUEST,
        "a task with 21 total actions (> MAX_ACTIONS_PER_TASK = 20) must be rejected with 400"
    );
}

/// CRITICAL bind-params test — a dedupe-free batch of exactly 1000 tasks, each carrying
/// on_start + 13 on_success actions = 14 actions/task = 14 000 action rows. At 5 bind
/// params per action row that is 70 000 binds in the grouped `action` INSERT, well over
/// PostgreSQL's 65 535 ceiling. WITHOUT chunking this INSERT fails and the request 500s;
/// WITH chunking (the fix) the batch commits: all 1000 tasks are created and a sampled
/// task has all 14 actions. (Task count 1000 is exactly at MAX_TASKS_PER_BATCH; 14
/// actions is under MAX_ACTIONS_PER_TASK = 20.)
///
/// Reverting the chunking in `flush_run` (single `.values(all_new_actions)`) makes this
/// test fail: the oversized INSERT errors and `create_tasks_ok`'s 201 assertion trips.
#[tokio::test]
async fn test_audit2_a10_limits_bind_param_ceiling_chunked_insert_succeeds() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let on_success: Vec<serde_json::Value> = (0..13)
        .map(|_| json!({"kind": "Webhook", "params": {"url": "https://ex.co/w", "verb": "Post"}}))
        .collect();
    let tasks: Vec<serde_json::Value> = (0..1000)
        .map(|i| {
            let mut t = min_task(&format!("bind{i}"));
            t["on_success"] = serde_json::Value::Array(on_success.clone());
            t
        })
        .collect();

    // create_tasks_ok asserts 201 and returns BasicTaskDtos (no actions field).
    let created = create_tasks_ok(&app, &tasks).await;
    assert_eq!(
        created.len(),
        1000,
        "all 1000 tasks must be created despite > 65535 total action binds (chunked INSERT)"
    );
    // Verify actions via GET /task/{id} (BasicTaskDto doesn't carry actions).
    let first = get_task_ok(&app, created[0].id).await;
    assert_eq!(
        first.actions.len(),
        14,
        "each task must have its 14 actions (1 on_start + 13 on_success) persisted"
    );
    let last = get_task_ok(&app, created[999].id).await;
    assert_eq!(
        last.actions.len(),
        14,
        "the last task in the batch must also have all 14 actions"
    );
}

/// A JSON body larger than the configured `web::JsonConfig` limit → 413.
/// Built as an inline app with a tiny (512-byte) limit so the assertion is deterministic
/// and cheap; production wires the same `web::JsonConfig::default().limit(...)` in main.rs.
#[tokio::test]
async fn test_audit2_a10_limits_oversized_payload_rejected() {
    use actix_web::{App, web};

    let (_g, state) = setup_test_app().await;
    let app = actix_web::test::init_service(
        App::new()
            .app_data(web::Data::new(state.clone()))
            .app_data(web::JsonConfig::default().limit(512))
            .configure(arcrun::handlers::configure_routes),
    )
    .await;

    // A single task whose serialized JSON far exceeds 512 bytes (huge name).
    let mut task = min_task("oversized");
    task["name"] = json!("x".repeat(4096));
    let body = serde_json::Value::Array(vec![task]);

    let req = actix_web::test::TestRequest::post()
        .uri("/task")
        .set_json(&body)
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(
        resp.status(),
        StatusCode::PAYLOAD_TOO_LARGE,
        "a body over the JsonConfig limit must be rejected with 413"
    );
}

/// The contract that makes cycles impossible by construction (so the recursive DFS could
/// be removed): a dependency referencing a task defined LATER in the batch is rejected
/// with 400. A forward reference is the only way a cycle could form, so forbidding it
/// forbids cycles — no cycle-detection pass needed.
#[tokio::test]
async fn test_audit2_a10_limits_forward_reference_rejected() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // "a" depends on "b", but "b" is defined AFTER "a" → forward reference.
    let mut a = min_task("a");
    a["dependencies"] = json!([{"id": "b", "requires_success": true}]);
    let b = min_task("b");

    assert_eq!(
        post_task_status(&app, &serde_json::Value::Array(vec![a, b])).await,
        StatusCode::BAD_REQUEST,
        "a forward dependency reference must be rejected with 400 (this is what excludes cycles)"
    );
}

// =============================================================================
// B1 — on_start no longer holds a DB connection during the HTTP call (Lot 6.1)
// =============================================================================

/// B1 (perf, HIGH): before the fix, `execute_webhook_for_task` acquired a pool
/// connection and held it across the entire on_start HTTP call (up to the webhook
/// timeout, ~10 s each). With `WORKER_WEBHOOK_CONCURRENCY == POOL_MAX_SIZE` (the
/// default 10 == 10), a burst of slow on_start webhooks pinned every pool
/// connection, starving HTTP handlers and the four other worker loops — exactly the
/// pathology the Lot 2 outbox eliminated for end/cancel deliveries.
///
/// # Fix
/// `execute_webhook_for_task` is split into phases, mirroring the delivery loop:
/// phase A borrows a connection for the claim + A4 re-check + action load then
/// **drops** it, phase B runs the on_start HTTP with **no connection held**, phase C
/// re-acquires a connection only for the A2/A4 running-transition transaction.
///
/// # What this test asserts
/// Running the real start loop against a **size-1 pool** with a deliberately slow
/// (~2 s) on_start webhook: while that HTTP is in flight (the start loop is parked
/// inside `webhook_phase` awaiting it), an independent `pool.get()` + `SELECT 1`
/// over the SAME pool completes in well under the webhook delay (< 1 s). Pre-fix the
/// single connection was pinned for the full ~2 s, so the concurrent request would
/// block until the webhook returned and the `< 1 s` assertion fails. It also asserts
/// the flow is unchanged: the task reaches Running, its start row is `success`, and
/// the webhook was received exactly once.
#[tokio::test]
async fn test_audit2_b1_on_start_http_does_not_hold_db_connection() {
    let test_app = setup_test_db_with_pool_size(1).await;
    let state = create_test_state(test_app.pool.clone());
    let app = test_service!(state);

    let webhook_delay = Duration::from_secs(2);
    let hits = Arc::new(AtomicUsize::new(0));
    let (webhook_url, shutdown_server) = spawn_slow_200_webhook_server(hits.clone(), webhook_delay);

    let task_payload = json!({
        "id": "b1-slow",
        "name": "B1 Slow on_start",
        "kind": "b1-kind",
        "timeout": 60,
        "metadata": {"test": true},
        "on_start": { "kind": "Webhook", "params": { "url": webhook_url, "verb": "Post" } }
    });
    let created = create_tasks_ok(&app, &[task_payload]).await;
    let task_id = created[0].id;

    // Spawn the real start loop over the size-1 pool. It claims the task, then enters
    // the on_start HTTP (phase B), which — with the fix — holds no connection.
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let evaluator = state.action_executor.clone();
    let pool = state.pool.clone();
    let handle = tokio::spawn(async move {
        arcrun::workers::start_loop(
            &evaluator,
            pool,
            Duration::from_millis(50),
            true,
            50,
            10,
            shutdown_rx,
            arcrun::workers::WorkerNudges::new(),
            std::time::Duration::from_secs(30),
        )
        .await;
    });

    // Wait until the on_start HTTP is in flight. The slow server bumps `hits` BEFORE
    // sleeping, so `hits >= 1` means we are inside the ~2 s HTTP window.
    let mut waited = Duration::ZERO;
    while hits.load(Ordering::SeqCst) == 0 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        waited += Duration::from_millis(10);
        assert!(
            waited < Duration::from_secs(5),
            "start loop never fired the on_start webhook"
        );
    }

    // The HTTP will keep the mock server busy for ~2 s. Time an independent DB
    // request over the SAME size-1 pool: with the fix the connection is free during
    // phase B, so this returns near-instantly.
    let probe_start = std::time::Instant::now();
    {
        let mut conn = tokio::time::timeout(Duration::from_secs(5), state.pool.get())
            .await
            .expect("pool.get() timed out — connection still pinned by the on_start HTTP")
            .expect("failed to acquire connection");
        diesel_async::RunQueryDsl::execute(diesel::sql_query("SELECT 1"), &mut *conn)
            .await
            .expect("SELECT 1 failed");
    }
    let probe_elapsed = probe_start.elapsed();

    assert!(
        probe_elapsed < Duration::from_secs(1),
        "concurrent DB request took {:?}, expected < 1s — the on_start HTTP is holding \
         the pool connection (B1 regression). webhook delay = {:?}",
        probe_elapsed,
        webhook_delay
    );

    // Flow non-regression: the slow webhook still drives the task to Running.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let t = get_task_ok(&app, task_id).await;
            if t.status == StatusKind::Running {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("task never reached Running after slow on_start");

    let _ = shutdown_tx.send(true);
    let _ = handle.await;
    let _ = shutdown_server.send(());

    assert_task_status(&app, task_id, StatusKind::Running, "task should be Running").await;
    assert_eq!(
        start_row_status(&state.pool, task_id).await.as_deref(),
        Some("success"),
        "start outbox row should be completed as success"
    );
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "on_start webhook should be received exactly once"
    );
}

/// B1 flow non-regression (clean E2E, default pool): a normal-speed on_start webhook
/// still drives Pending -> Claimed -> Running, completes the `start` outbox row as
/// `success`, and is received exactly once — the phase split changes only WHERE the
/// connection is held, never the semantics.
#[tokio::test]
async fn test_audit2_b1_flow_unchanged_after_phase_split() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let hits = Arc::new(AtomicUsize::new(0));
    let (webhook_url, shutdown_server) = spawn_webhook_server(hits.clone());

    let task_payload = json!({
        "id": "b1-flow",
        "name": "B1 Flow",
        "kind": "b1-flow-kind",
        "timeout": 60,
        "metadata": {"test": true},
        "on_start": { "kind": "Webhook", "params": { "url": webhook_url, "verb": "Post" } }
    });
    let created = create_tasks_ok(&app, &[task_payload]).await;
    let task_id = created[0].id;
    assert_eq!(created[0].status, StatusKind::Pending);

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let evaluator = state.action_executor.clone();
    let pool = state.pool.clone();
    let handle = tokio::spawn(async move {
        arcrun::workers::start_loop(
            &evaluator,
            pool,
            Duration::from_millis(50),
            true,
            50,
            10,
            shutdown_rx,
            arcrun::workers::WorkerNudges::new(),
            std::time::Duration::from_secs(30),
        )
        .await;
    });

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let t = get_task_ok(&app, task_id).await;
            if t.status == StatusKind::Running {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("task never reached Running");

    let _ = shutdown_tx.send(true);
    let _ = handle.await;
    let _ = shutdown_server.send(());

    assert_task_status(&app, task_id, StatusKind::Running, "task should be Running").await;
    assert_eq!(
        start_row_status(&state.pool, task_id).await.as_deref(),
        Some("success"),
        "start outbox row should be completed as success"
    );
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "on_start webhook should be received exactly once"
    );
}

// ===========================================================================
// Audit 2, B3 — batch-complete detection: O(1) per transition, no lock on
//               no-webhook batches.
// ===========================================================================
//
// # Original problem
// `maybe_enqueue_batch_complete` (src/db/webhook_execution.rs) ran, on EVERY
// terminal transition of a batched task, a `NOT EXISTS (task WHERE batch_id=$1
// AND status NOT IN (terminal))` probe, serialised per batch by the `batch` row
// lock. With only `idx_task_batch_id` (every row of the batch), near the end of a
// large batch's life the probe scanned almost all of the batch's already-terminal
// rows: O(N) per transition ⇒ O(N^2) per batch (a 50k batch ≈ 1.25 billion
// cumulative row visits). Two defects:
//   1. No index qualified the probe's exact predicate.
//   2. The locking `SELECT ... FOR UPDATE` locked EVERY batch row — including
//      scope/metadata-only batches (`on_complete = '[]'`, #601) that carry no
//      completion signal — so their terminal transitions serialised for nothing.
//
// # Fix
//   1. A PARTIAL index `idx_task_batch_active ON task(batch_id) WHERE status NOT IN
//      ('success','failure','canceled')` — byte-for-byte the probe's predicate — so
//      the `NOT EXISTS` becomes an index existence check whose cost is independent of
//      the number of terminal rows (O(1) as the batch drains).
//   2. The non-vacuity predicate is pushed INTO the locking statement
//      (`on_complete <> '[]'::jsonb`): a no-webhook batch no longer matches, so the
//      `SELECT ... FOR UPDATE` returns no row, takes no lock, and early-returns.
//
// # SUPERSEDED BY D2 (batch.remaining counter)
// D2 replaced the probe + `batch` row lock entirely with a denormalized
// `batch.remaining` counter (each terminal transition does
// `UPDATE batch SET remaining = GREATEST(remaining - N, 0) … RETURNING remaining`;
// `remaining = 0` is the completion signal). The B3 partial index was the probe's
// only consumer, so migration `2026-07-09-000004_batch_remaining` drops it. See
// `tests/integration/test_batch_remaining.rs` for the D2 behaviour coverage.
//
// # What these tests assert (post-D2)
//   * `idx_task_batch_active` no longer exists (the old EXPLAIN-plan test is retired;
//     D2 dropped the index — this asserts the migration cleanup landed).
//   * A scope-only batch (`on_complete = '[]'`) reaches full termination WITHOUT
//     enqueuing any `batch_complete` outbox row (behaviour unchanged under D2).
//   * A webhook batch still fires its `on_batch_complete` exactly once, on the last
//     task's terminal transition (non-regression of the counter path).

/// Total `batch_complete` outbox rows for a batch (any status).
async fn b3_batch_complete_total(pool: &arcrun::DbPool, batch_id: uuid::Uuid) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Cnt {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        c: i64,
    }
    let mut conn = pool.get().await.unwrap();
    let r: Cnt = diesel_async::RunQueryDsl::get_result(
        diesel::sql_query(
            // D3: the batch_complete signal is in the queue (webhook_outbox) until
            // delivered, then in the history (webhook_execution) — count both.
            "SELECT (\
                (SELECT count(*) FROM webhook_execution WHERE batch_id = $1 AND trigger = 'batch_complete') + \
                (SELECT count(*) FROM webhook_outbox    WHERE batch_id = $1 AND trigger = 'batch_complete')\
             ) AS c",
        )
        .bind::<diesel::sql_types::Uuid, _>(batch_id),
        &mut *conn,
    )
    .await
    .unwrap();
    r.c
}

/// A batch row's `on_complete` payload as JSON (panics if the row is absent).
async fn b3_batch_on_complete(pool: &arcrun::DbPool, batch_id: uuid::Uuid) -> serde_json::Value {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Jsonb)]
        on_complete: serde_json::Value,
    }
    let mut conn = pool.get().await.unwrap();
    let r: Row = diesel_async::RunQueryDsl::get_result(
        diesel::sql_query("SELECT on_complete FROM batch WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(batch_id),
        &mut *conn,
    )
    .await
    .expect("batch row should exist");
    r.on_complete
}

/// POST an object-form body, assert 201, return the `X-Batch-ID`.
async fn b3_create_batch<S, B>(app: &S, body: &serde_json::Value) -> uuid::Uuid
where
    S: ActixService<ActixRequest, Response = ActixServiceResponse<B>, Error = actix_web::Error>,
    B: ActixMessageBody,
{
    let req = actix_web::test::TestRequest::post()
        .uri("/task")
        .insert_header(("requester", "test"))
        .set_json(body)
        .to_request();
    let resp = actix_web::test::call_service(app, req).await;
    assert_eq!(
        resp.status(),
        StatusCode::CREATED,
        "POST /task (object form) should return 201 Created"
    );
    resp.headers()
        .get("X-Batch-ID")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| uuid::Uuid::parse_str(s).ok())
        .expect("X-Batch-ID header")
}

/// B3/D2 — the partial index `idx_task_batch_active` no longer exists.
///
/// SUPERSEDED BY D2: the B3 partial index existed SOLELY to serve the batch-complete
/// `NOT EXISTS (task active)` probe. D2 (`batch.remaining` counter) retired that probe
/// entirely, so the index is dead weight and migration `2026-07-09-000004_batch_remaining`
/// drops it. The old test EXPLAINed the probe and asserted the plan used the index; both
/// the probe and the index are gone, making that assertion meaningless. This replacement
/// asserts the migration actually dropped the index — a regression guard that D2's
/// cleanup landed (reverting the DROP in the migration makes this fail).
#[tokio::test]
async fn test_audit2_b3_partial_index_dropped_by_d2() {
    use diesel_async::RunQueryDsl;
    let (_g, state) = setup_test_app().await;

    #[derive(diesel::QueryableByName)]
    struct Cnt {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        c: i64,
    }
    let mut conn = state.pool.get().await.unwrap();
    let r: Cnt = diesel::sql_query(
        "SELECT count(*) AS c FROM pg_indexes WHERE indexname = 'idx_task_batch_active'",
    )
    .get_result(&mut conn)
    .await
    .unwrap();
    assert_eq!(
        r.c, 0,
        "idx_task_batch_active must be dropped by D2 (its only consumer, the \
         batch-complete NOT EXISTS probe, was replaced by the batch.remaining counter)"
    );
}

/// B3 — a scope-only batch (`on_complete = '[]'`) reaches full termination WITHOUT
/// enqueuing a `batch_complete` outbox row.
///
/// Since #601 a scope/metadata-only batch has a `batch` row but an empty `on_complete`.
/// The B3 fix pushes `on_complete <> '[]'::jsonb` into the locking statement so such a
/// batch is neither locked nor signalled. This asserts the observable half: after the
/// only task becomes terminal, no `batch_complete` outbox row exists for the batch
/// (behaviour unchanged; the lock is simply not taken).
#[tokio::test]
async fn test_audit2_b3_scope_only_batch_no_batch_complete_enqueue() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let body = json!({
        "tasks": [task_json("b3-scope-1", "B3 scope only", "b3")],
        "scope": "b3-scope-only",
        "metadata": {"env": "test"}
    });
    let batch_id = b3_create_batch(&app, &body).await;

    // Precondition: the batch row exists with an EMPTY on_complete (scope-only).
    assert_eq!(
        b3_batch_on_complete(&state.pool, batch_id).await,
        json!([]),
        "a scope-only batch must store on_complete = '[]'"
    );

    let created = get_task_ok(&app, uuid_of_only_task(&state, batch_id).await).await;
    succeed_task(&state, created.id).await;

    assert_eq!(
        b3_batch_complete_total(&state.pool, batch_id).await,
        0,
        "a scope-only batch (empty on_complete) must NOT enqueue a batch_complete row"
    );
}

/// Resolve the single task id of a batch (test helper for the scope-only batch, whose
/// POST body returns tasks but we re-fetch to stay independent of body shape).
async fn uuid_of_only_task(state: &arcrun::handlers::AppState, batch_id: uuid::Uuid) -> uuid::Uuid {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: uuid::Uuid,
    }
    let mut conn = state.pool.get().await.unwrap();
    let r: Row = diesel_async::RunQueryDsl::get_result(
        diesel::sql_query("SELECT id FROM task WHERE batch_id = $1")
            .bind::<diesel::sql_types::Uuid, _>(batch_id),
        &mut *conn,
    )
    .await
    .unwrap();
    r.id
}

/// B3 — a webhook batch still fires `on_batch_complete` exactly once, on the last
/// task's terminal transition (non-regression of the modified locking path).
#[tokio::test]
async fn test_audit2_b3_webhook_batch_fires_once_on_last_terminal() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let hits = Arc::new(AtomicUsize::new(0));
    let (webhook_url, shutdown_server) = spawn_webhook_server(hits.clone());

    let body = json!({
        "tasks": [
            task_json("b3-wh-1", "B3 wh 1", "b3"),
            task_json("b3-wh-2", "B3 wh 2", "b3"),
        ],
        "on_batch_complete": [
            {"kind": "Webhook", "params": {"url": webhook_url, "verb": "Post"}}
        ]
    });
    let batch_id = b3_create_batch(&app, &body).await;

    // Collect the two task ids (order-independent).
    let ids = {
        #[derive(diesel::QueryableByName)]
        struct Row {
            #[diesel(sql_type = diesel::sql_types::Uuid)]
            id: uuid::Uuid,
        }
        let mut conn = state.pool.get().await.unwrap();
        let rows: Vec<Row> = diesel_async::RunQueryDsl::get_results(
            diesel::sql_query("SELECT id FROM task WHERE batch_id = $1 ORDER BY name")
                .bind::<diesel::sql_types::Uuid, _>(batch_id),
            &mut *conn,
        )
        .await
        .unwrap();
        rows.into_iter().map(|r| r.id).collect::<Vec<_>>()
    };
    assert_eq!(ids.len(), 2, "batch should have two tasks");

    // First task terminal: batch not yet complete -> no signal.
    succeed_task(&state, ids[0]).await;
    assert_eq!(
        b3_batch_complete_total(&state.pool, batch_id).await,
        0,
        "batch_complete must NOT fire before the last task is terminal"
    );

    // Last task terminal: exactly one batch_complete row is enqueued.
    succeed_task(&state, ids[1]).await;
    assert_eq!(
        b3_batch_complete_total(&state.pool, batch_id).await,
        1,
        "exactly one batch_complete row on the last terminal transition"
    );

    // Delivery fires the webhook exactly once.
    drain_outbox(&state).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "on_batch_complete webhook must be delivered exactly once"
    );

    let _ = shutdown_server.send(());
}

// =============================================================================
// B5 — failure cascade walked level-by-level (frontier BFS), not per-child recursion
// =============================================================================
//
// `propagate_to_children`'s failure cascade used to recurse once per failed child
// (`Box::pin(propagate_to_children(fid, Failure))`), so a root failure over N
// descendants ran O(N) sequential SELECT/UPDATE round-trips inside the PATCH
// transaction — locks held, latency in seconds. B5 replaces that with a
// level-by-level frontier walk (`cascade_failure_frontier`): one links SELECT +
// one A9 pre-lock + one cascade-fail UPDATE + one decrement UPDATE + one unblock
// UPDATE per DAG level, i.e. O(depth) statements. These tests assert the rewrite
// is behaviorally faithful, not the timing.

/// B5 — DEEP linear chain: a root failure must cascade to every descendant.
///
/// A chain of 60 `requires_success` tasks (t0 -> t1 -> ... -> t60): the root is
/// PATCHed Failure and EVERY descendant must end `Failure` with a populated
/// `failure_reason`, and an on_failure (trigger='end') outbox row must exist for
/// each. This is the correctness proof of the frontier loop replacing the
/// per-child recursion.
#[tokio::test]
async fn test_audit2_b5_deep_chain_all_descendants_fail() {
    const DEPTH: usize = 60;
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);
    let kind = "b5-chain";

    let mut tasks: Vec<serde_json::Value> = Vec::new();
    tasks.push(task_json("t0", "t0", kind));
    for i in 1..=DEPTH {
        let id = format!("t{i}");
        let parent = format!("t{}", i - 1);
        tasks.push(task_with_deps(
            &id,
            &id,
            kind,
            vec![(parent.as_str(), true)],
        ));
    }
    let created = create_tasks_ok(&app, &tasks).await;
    let by_name: std::collections::HashMap<String, uuid::Uuid> =
        created.iter().map(|t| (t.name.clone(), t.id)).collect();

    // Root -> Running, then PATCH Failure.
    let root = by_name["t0"];
    force_running(&state, root).await;
    let resp = actix_web::test::call_service(
        &app,
        patch_status_req(
            root,
            json!({"status": "Failure", "failure_reason": "root boom"}),
        ),
    )
    .await;
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "root PATCH Failure must be a 200 transition"
    );

    // Every descendant ends Failure, with a populated reason + exactly one end row.
    for i in 1..=DEPTH {
        let id = by_name[&format!("t{i}")];
        let t = get_task_ok(&app, id).await;
        assert_eq!(
            t.status,
            StatusKind::Failure,
            "t{i} must cascade to Failure via the frontier walk"
        );
        assert!(
            !t.failure_reason.unwrap_or_default().is_empty(),
            "t{i} must have a populated failure_reason"
        );
        assert_eq!(
            outbox_count_by_trigger(&state.pool, id, "end").await,
            1,
            "t{i} must have exactly one on_failure (end) outbox row"
        );
    }
}

/// B5 — mixed cascade tree exercises fail set, decrement set, and unblock across
/// two frontier levels.
///
/// root -> A (requires_success), B (not required); A -> C (requires_success from
/// A), D (not required from A). root fails =>
/// * A and C cascade to `Failure` (required-parent failure, one per level),
/// * B is `wait_finished`-decremented (root not required) and, its only dep gone,
///   unblocks to `Pending`,
/// * D is decremented VIA THE FRONTIER (A failed, D not required from A) and,
///   its only dep gone, unblocks to `Pending`.
/// Asserts both statuses and the drained (0,0) wait counters.
#[tokio::test]
async fn test_audit2_b5_mixed_tree_fail_decrement_unblock() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);
    let kind = "b5-mixed";

    let tasks = vec![
        task_json("root", "root", kind),
        task_with_deps("A", "A", kind, vec![("root", true)]),
        task_with_deps("B", "B", kind, vec![("root", false)]),
        task_with_deps("C", "C", kind, vec![("A", true)]),
        task_with_deps("D", "D", kind, vec![("A", false)]),
    ];
    let created = create_tasks_ok(&app, &tasks).await;
    let by = |n: &str| created.iter().find(|t| t.name == n).unwrap().id;

    force_running(&state, by("root")).await;
    let resp = actix_web::test::call_service(
        &app,
        patch_status_req(
            by("root"),
            json!({"status": "Failure", "failure_reason": "boom"}),
        ),
    )
    .await;
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "root PATCH Failure must be 200"
    );

    assert_task_status(
        &app,
        by("A"),
        StatusKind::Failure,
        "A fails (required parent root failed)",
    )
    .await;
    assert_task_status(
        &app,
        by("C"),
        StatusKind::Failure,
        "C fails (required parent A failed, via the frontier)",
    )
    .await;
    assert_task_status(
        &app,
        by("B"),
        StatusKind::Pending,
        "B unblocks to Pending (root not required, only dep drained)",
    )
    .await;
    assert_eq!(
        read_wait_counters(&state.pool, by("B")).await,
        (0, 0),
        "B wait counters drained"
    );
    assert_task_status(
        &app,
        by("D"),
        StatusKind::Pending,
        "D unblocks to Pending via the frontier decrement (A not required)",
    )
    .await;
    assert_eq!(
        read_wait_counters(&state.pool, by("D")).await,
        (0, 0),
        "D wait counters drained"
    );
}

/// B5 — diamond INSIDE the cascade: a grandchild required by two parents that
/// fail in the SAME frontier level is failed exactly once.
///
/// root -> A, B (both requires_success); A -> C, B -> C (C requires_success from
/// both). root fails => A and B fail (level 1), then the next frontier is {A, B}
/// and C appears twice in the level's links. The frontier walk dedups the fail
/// set, so C is failed once, appears once in the return set, and gets exactly ONE
/// on_failure outbox row (no double delivery).
#[tokio::test]
async fn test_audit2_b5_diamond_in_cascade_single_outbox() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);
    let kind = "b5-diamond";

    let tasks = vec![
        task_json("root", "root", kind),
        task_with_deps("A", "A", kind, vec![("root", true)]),
        task_with_deps("B", "B", kind, vec![("root", true)]),
        task_with_deps("C", "C", kind, vec![("A", true), ("B", true)]),
    ];
    let created = create_tasks_ok(&app, &tasks).await;
    let by = |n: &str| created.iter().find(|t| t.name == n).unwrap().id;

    force_running(&state, by("root")).await;
    let resp = actix_web::test::call_service(
        &app,
        patch_status_req(
            by("root"),
            json!({"status": "Failure", "failure_reason": "boom"}),
        ),
    )
    .await;
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "root PATCH Failure must be 200"
    );

    assert_task_status(&app, by("A"), StatusKind::Failure, "A fails").await;
    assert_task_status(&app, by("B"), StatusKind::Failure, "B fails").await;
    assert_task_status(
        &app,
        by("C"),
        StatusKind::Failure,
        "C fails once via the deduped {A,B} frontier",
    )
    .await;
    assert_eq!(
        outbox_count_by_trigger(&state.pool, by("C"), "end").await,
        1,
        "C must have exactly ONE on_failure outbox row (deduped frontier, no double)"
    );
}

/// A non-required child whose TWO parents fail in the SAME cascade frontier must
/// receive TWO `wait_finished` decrements — one per failed parent.
///
/// # Original bug (caught in the B5 review)
/// The first frontier implementation deduplicated the level's decrement set and
/// applied a single `wait_finished - 1` UPDATE. Pre-B5, the per-failed-child
/// recursion ran one `propagate_to_children` call per failed parent, so a child
/// with N failed (non-required) parents in the cascade was decremented N times.
/// With the dedup, such a child was decremented ONCE, leaving `wait_finished`
/// stranded ≥ 1 forever — the task could never unblock to Pending.
///
/// # Fix
/// `cascade_failure_frontier` counts each child's multiplicity in the level's
/// links and decrements by that delta (children grouped by delta, one UPDATE per
/// distinct delta).
///
/// # What this test asserts
/// root → A (required), B (required); D depends on A AND B, both non-required.
/// root fails ⇒ frontier {A, B} both cascade-fail in ONE level ⇒ D must be
/// decremented twice (wait_finished 2 → 0) and unblock to Pending.
#[tokio::test]
async fn test_audit2_b5_multi_failed_parents_decrement_multiplicity() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);
    let kind = "b5-multi";

    let tasks = vec![
        task_json("root", "root", kind),
        task_with_deps("A", "A", kind, vec![("root", true)]),
        task_with_deps("B", "B", kind, vec![("root", true)]),
        task_with_deps("D", "D", kind, vec![("A", false), ("B", false)]),
    ];
    let created = create_tasks_ok(&app, &tasks).await;
    let by = |n: &str| created.iter().find(|t| t.name == n).unwrap().id;

    assert_eq!(
        read_wait_counters(&state.pool, by("D")).await,
        (2, 0),
        "D starts waiting on both A and B (non-required)"
    );

    force_running(&state, by("root")).await;
    let resp = actix_web::test::call_service(
        &app,
        patch_status_req(
            by("root"),
            json!({"status": "Failure", "failure_reason": "boom"}),
        ),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);

    assert_task_status(&app, by("A"), StatusKind::Failure, "A cascade-fails").await;
    assert_task_status(&app, by("B"), StatusKind::Failure, "B cascade-fails").await;
    assert_eq!(
        read_wait_counters(&state.pool, by("D")).await,
        (0, 0),
        "D must be decremented once PER failed parent (A and B are in the same frontier)"
    );
    assert_task_status(
        &app,
        by("D"),
        StatusKind::Pending,
        "D unblocks to Pending — both its (non-required) deps are terminal",
    )
    .await;
}

// ============================================================================
// B4 — polling-floor latency: in-process worker nudges (WorkerNudges)
// ============================================================================
//
// The start/delivery loops poll on a fixed interval. Before B4 every DAG edge of
// instantaneous tasks paid up to one full tick of scheduling latency. `WorkerNudges`
// (a pair of `tokio::sync::Notify` shared between the handlers and the loops) lets a
// committing transition wake the relevant loop immediately; the poll stays as the
// correctness/fallback path.
//
// Both tests spawn the REAL loop with a deliberately LONG poll interval (8 s) sharing
// the AppState's nudges, let the loop park in its `select!`, then drive the HTTP path
// and assert the effect lands in < 3 s — impossible via the 8 s poll alone, so the
// nudge is what is being measured.

/// B4 — `POST /task` nudges the start loop.
///
/// # Setup
/// `start_loop` runs with an 8 s interval but shares `state.nudges`. We let it finish
/// its immediate first (empty) iteration and park asleep, THEN POST a dependency-free
/// task through the handler, which calls `nudges.nudge_start()` after commit.
///
/// # Assertion
/// The task reaches `Running` in < 3 s (timed from the POST). Contre-épreuve: with the
/// `state.nudges.nudge_start()` call removed from `add_task`, the loop stays asleep for
/// the rest of the 8 s interval and the task is still `Pending` at the 3 s deadline —
/// the test then panics (RED), confirming it exercises the nudge and not the poll.
#[tokio::test]
async fn test_audit2_b4_post_task_nudges_start_loop() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let hits = Arc::new(AtomicUsize::new(0));
    let (webhook_url, shutdown_server) = spawn_webhook_server(hits.clone());

    // Long poll interval: only a nudge can produce sub-3s latency.
    const LONG: Duration = Duration::from_secs(8);
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let evaluator = state.action_executor.clone();
    let pool = state.pool.clone();
    let nudges = state.nudges.clone();
    let handle = tokio::spawn(async move {
        arcrun::workers::start_loop(
            &evaluator,
            pool,
            LONG,
            true,
            50,
            10,
            shutdown_rx,
            nudges,
            std::time::Duration::from_secs(30),
        )
        .await;
    });

    // Let the loop run its immediate first iteration (no Pending work) and park asleep
    // in its `select!` — so, absent a nudge, the next scan is ~8 s away.
    tokio::time::sleep(Duration::from_millis(400)).await;

    let task_payload = json!({
        "id": "b4-start",
        "name": "B4 start latency",
        "kind": "b4-start-kind",
        "timeout": 60,
        "metadata": {"test": true},
        "on_start": { "kind": "Webhook", "params": { "url": webhook_url, "verb": "Post" } }
    });

    let t0 = std::time::Instant::now();
    let created = create_tasks_ok(&app, &[task_payload]).await;
    let task_id = created[0].id;
    assert_eq!(created[0].status, StatusKind::Pending);

    // Poll until Running with a 3 s deadline.
    let mut became_running: Option<Duration> = None;
    while t0.elapsed() < Duration::from_secs(3) {
        if get_task_ok(&app, task_id).await.status == StatusKind::Running {
            became_running = Some(t0.elapsed());
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let _ = shutdown_tx.send(true);
    let _ = handle.await;
    let _ = shutdown_server.send(());

    let elapsed = became_running.expect(
        "task did not reach Running within 3s — the poll interval is 8s, so only the \
         start-loop nudge from add_task can explain sub-3s latency (nudge missing/broken?)",
    );
    assert!(
        elapsed < Duration::from_secs(3),
        "task reached Running in {:?}, expected < 3s via the start nudge",
        elapsed
    );
}

/// B4 — a status `PATCH` nudges the delivery loop.
///
/// # Setup
/// A short-interval start loop brings the task to `Running` (its `start` outbox row is
/// completed in the same tx, so the start-before-end gate is already open). The REAL
/// `delivery_loop` runs with an 8 s interval sharing `state.nudges`, and is given time
/// to park asleep. We then `PATCH` the task `Success` through the handler, which
/// enqueues the `end/success` outbox row and calls `nudges.nudge_delivery()`.
///
/// # Assertion
/// The `on_success` webhook is received in < 3 s (timed from the PATCH). Absent the
/// nudge the delivery loop would sleep out the ~8 s interval before draining the row.
#[tokio::test]
async fn test_audit2_b4_patch_nudges_delivery_loop() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let hits = Arc::new(AtomicUsize::new(0));
    let (webhook_url, shutdown_server) = spawn_webhook_server(hits.clone());

    // Start loop: short interval (we only measure delivery latency here).
    let (start_shutdown_tx, start_shutdown_rx) = tokio::sync::watch::channel(false);
    {
        let evaluator = state.action_executor.clone();
        let pool = state.pool.clone();
        let nudges = state.nudges.clone();
        tokio::spawn(async move {
            arcrun::workers::start_loop(
                &evaluator,
                pool,
                Duration::from_millis(50),
                true,
                50,
                10,
                start_shutdown_rx,
                nudges,
                std::time::Duration::from_secs(30),
            )
            .await;
        });
    }

    // Delivery loop: LONG interval so only a nudge yields sub-3s delivery.
    const LONG: Duration = Duration::from_secs(8);
    let (del_shutdown_tx, del_shutdown_rx) = tokio::sync::watch::channel(false);
    {
        let evaluator = Arc::new(state.action_executor.clone());
        let pool = state.pool.clone();
        let nudges = state.nudges.clone();
        let cfg = default_delivery_cfg();
        tokio::spawn(async move {
            arcrun::workers::delivery_loop(evaluator, pool, LONG, cfg, del_shutdown_rx, nudges)
                .await;
        });
    }

    let task_payload = json!({
        "id": "b4-del",
        "name": "B4 delivery latency",
        "kind": "b4-del-kind",
        "timeout": 60,
        "metadata": {"test": true},
        "on_start": { "kind": "Webhook", "params": { "url": webhook_url, "verb": "Post" } },
        "on_success": [{ "kind": "Webhook", "params": { "url": webhook_url, "verb": "Post" } }]
    });
    let created = create_tasks_ok(&app, &[task_payload]).await;
    let task_id = created[0].id;

    // Wait for the start loop to bring it to Running (on_start delivered => hits >= 1).
    let mut running = false;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if get_task_ok(&app, task_id).await.status == StatusKind::Running {
            running = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(running, "task should reach Running via the start loop");

    // Stop the start loop so it no longer contends for pool connections; the delivery
    // measurement below is then clean.
    let _ = start_shutdown_tx.send(true);

    // Let the delivery loop finish its immediate (empty — no mature end rows yet) first
    // iteration and park asleep.
    tokio::time::sleep(Duration::from_millis(400)).await;
    let hits_before = hits.load(Ordering::SeqCst);

    // PATCH Success via the HTTP handler → enqueues the end/success outbox row AND
    // calls nudges.nudge_delivery().
    let t0 = std::time::Instant::now();
    let patch = actix_web::test::TestRequest::patch()
        .uri(&format!("/task/{}", task_id))
        .set_json(&json!({"status": "Success"}))
        .to_request();
    let resp = actix_web::test::call_service(&app, patch).await;
    assert!(resp.status().is_success(), "PATCH Success should succeed");

    // Poll until the on_success webhook fires (hits increments) with a 3 s deadline.
    let mut delivered: Option<Duration> = None;
    while t0.elapsed() < Duration::from_secs(3) {
        if hits.load(Ordering::SeqCst) > hits_before {
            delivered = Some(t0.elapsed());
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let _ = del_shutdown_tx.send(true);
    let _ = shutdown_server.send(());

    let elapsed = delivered.expect(
        "on_success webhook was not delivered within 3s — the delivery interval is 8s, so \
         only the delivery-loop nudge from update_task can explain sub-3s delivery",
    );
    assert!(
        elapsed < Duration::from_secs(3),
        "on_success delivered in {:?}, expected < 3s via the delivery nudge",
        elapsed
    );
}

// =============================================================================
// B2 — heartbeat `last_updated` while waiting for the semaphore permit
// =============================================================================

/// B2 — a claimed task waiting for a permit is heartbeated and not requeued.
///
/// # Setup
/// `webhook_concurrency = 1`, `claim_timeout = 3 s`, heartbeat interval = 1 s.
/// Two tasks are created. The first task's on_start webhook takes ~4 s (slow server),
/// saturating the single permit. The second task is claimed and parked on the
/// semaphore. Without the heartbeat, its `last_updated` goes stale after 3 s and
/// requeue-stale would reclaim it (Claimed → Pending churn). With the heartbeat,
/// `last_updated` is bumped every 1 s and the task stays Claimed until the permit
/// becomes available.
///
/// # Assertion
/// After the slow on_start finishes (~4 s), both tasks reach Running — the second
/// one was NOT requeued. Both on_start webhooks fire exactly once (no churn).
#[tokio::test]
async fn test_audit2_b2_heartbeat_prevents_requeue_during_permit_wait() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let hits = Arc::new(AtomicUsize::new(0));
    let (webhook_url, shutdown_server) =
        spawn_slow_200_webhook_server(hits.clone(), Duration::from_secs(4));

    // Short claim_timeout (3s) + concurrency=1 → the second task blocks on the permit.
    let claim_timeout = Duration::from_secs(3);
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let evaluator = state.action_executor.clone();
    let pool = state.pool.clone();
    let handle = tokio::spawn(async move {
        arcrun::workers::start_loop(
            &evaluator,
            pool,
            Duration::from_millis(200),
            false,
            50,
            1, // concurrency = 1
            shutdown_rx,
            arcrun::workers::WorkerNudges::new(),
            claim_timeout,
        )
        .await;
    });

    // Create two tasks with on_start webhooks.
    let t1 = json!({
        "id": "b2-t1", "name": "b2-t1", "kind": "b2-kind", "timeout": 60,
        "on_start": { "kind": "Webhook", "params": { "url": webhook_url, "verb": "Post" } }
    });
    let t2 = json!({
        "id": "b2-t2", "name": "b2-t2", "kind": "b2-kind", "timeout": 60,
        "on_start": { "kind": "Webhook", "params": { "url": webhook_url, "verb": "Post" } }
    });
    let created = create_tasks_ok(&app, &[t1, t2]).await;
    let id1 = created.iter().find(|t| t.name == "b2-t1").unwrap().id;
    let id2 = created.iter().find(|t| t.name == "b2-t2").unwrap().id;

    // Wait for both to reach Running (deadline 12s — each slow webhook takes ~4s
    // sequentially with concurrency=1, so the second should be Running by ~8-9s).
    let mut both_running = false;
    let deadline = std::time::Instant::now() + Duration::from_secs(12);
    while std::time::Instant::now() < deadline {
        let s1 = get_task_ok(&app, id1).await.status;
        let s2 = get_task_ok(&app, id2).await.status;
        if s1 == StatusKind::Running && s2 == StatusKind::Running {
            both_running = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let _ = shutdown_tx.send(true);
    let _ = handle.await;
    let _ = shutdown_server.send(());

    assert!(
        both_running,
        "both tasks must reach Running — the heartbeat should prevent requeue of the second \
         task while it waits for the permit (claim_timeout=3s, slow webhook=4s)"
    );
    assert_eq!(
        hits.load(Ordering::SeqCst),
        2,
        "both on_start webhooks must have fired (no requeue churn)"
    );
}

// =============================================================================
// Audit 2, item 6.6b (B6/B7) — dead-index drop, index tiebreaker, timeout drain,
// and probe timeout regressions.
// =============================================================================

#[derive(diesel::QueryableByName)]
struct B6bPlanText {
    #[diesel(sql_type = diesel::sql_types::Text)]
    plan: String,
}

/// EXPLAIN an arbitrary read-only SQL statement and return its plan text.
///
/// EXPLAIN's output column is literally `QUERY PLAN` (a space diesel cannot bind
/// by name), so the statement is wrapped in a temp plpgsql function whose
/// SETOF-text result is aliased to `plan` and aggregated into one string — same
/// trick as the B3 probe helper. `disable_seqscan` forces the planner to prefer
/// a usable index (a tiny test relation is otherwise cheapest to seqscan).
async fn b6b_explain_plan(pool: &arcrun::DbPool, query_sql: &str, disable_seqscan: bool) -> String {
    use diesel_async::RunQueryDsl;
    let mut conn = pool.get().await.unwrap();

    diesel::sql_query(if disable_seqscan {
        "SET enable_seqscan = off"
    } else {
        "SET enable_seqscan = on"
    })
    .execute(&mut conn)
    .await
    .unwrap();

    let create = format!(
        "CREATE OR REPLACE FUNCTION pg_temp.b6b_explain() RETURNS SETOF text AS $$ \
         BEGIN RETURN QUERY EXECUTE 'EXPLAIN (FORMAT TEXT) {}'; END $$ LANGUAGE plpgsql;",
        query_sql.replace('\'', "''")
    );
    diesel::sql_query(create).execute(&mut conn).await.unwrap();

    let rows: Vec<B6bPlanText> = diesel::sql_query(
        "SELECT string_agg(line, E'\n') AS plan FROM pg_temp.b6b_explain() AS t(line)",
    )
    .get_results(&mut conn)
    .await
    .unwrap();

    let _ = diesel::sql_query("RESET enable_seqscan")
        .execute(&mut conn)
        .await;

    rows.into_iter().next().map(|r| r.plan).unwrap_or_default()
}

/// B7 — after dropping the redundant `idx_action_task_id`, an action lookup by
/// `task_id` is still index-served, by the composite `idx_action_task_id_trigger`
/// (task_id is its leading column). Guards that the drop did not force a seqscan
/// on the hot `action` table.
#[tokio::test]
async fn test_audit2_b6b_action_by_task_id_uses_composite_index() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // Each task_json carries an on_start webhook => one `action` row per task.
    let created = create_tasks_ok(
        &app,
        &[
            task_json("b6b-a-1", "B6b action 1", "b6b"),
            task_json("b6b-a-2", "B6b action 2", "b6b"),
            task_json("b6b-a-3", "B6b action 3", "b6b"),
        ],
    )
    .await;
    let task_id = created[0].id;

    {
        use diesel_async::RunQueryDsl;
        let mut conn = state.pool.get().await.unwrap();
        diesel::sql_query("ANALYZE action")
            .execute(&mut conn)
            .await
            .unwrap();
    }

    // The task_id-only lookup (the case the dropped single-column index used to
    // serve) must now be served by the composite index via its leading column.
    let sql = format!("SELECT id FROM action WHERE task_id = '{}'::uuid", task_id);
    let plan = b6b_explain_plan(&state.pool, &sql, true).await;
    assert!(
        plan.contains("idx_action_task_id_trigger"),
        "action lookup by task_id must be served by idx_action_task_id_trigger \
         after idx_action_task_id was dropped; plan was:\n{plan}"
    );
    assert!(
        !plan.contains("Seq Scan"),
        "action lookup by task_id must not fall back to a seqscan; plan was:\n{plan}"
    );
}

/// B7 — with the `id` tiebreaker added to `idx_task_priority`, the start_loop
/// Pending scan (ORDER BY priority DESC, created_at ASC, id ASC) is a pure Index
/// Scan with NO Incremental Sort node. The pre-fix index (without `id`) forced an
/// Incremental Sort on every keyset page.
#[tokio::test]
async fn test_audit2_b6b_pending_scan_no_incremental_sort() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // A handful of Pending tasks so the index has live entries.
    create_tasks_ok(
        &app,
        &[
            task_json("b6b-p-1", "B6b pending 1", "b6b"),
            task_json("b6b-p-2", "B6b pending 2", "b6b"),
            task_json("b6b-p-3", "B6b pending 3", "b6b"),
            task_json("b6b-p-4", "B6b pending 4", "b6b"),
        ],
    )
    .await;

    {
        use diesel_async::RunQueryDsl;
        let mut conn = state.pool.get().await.unwrap();
        diesel::sql_query("ANALYZE task")
            .execute(&mut conn)
            .await
            .unwrap();
    }

    // Byte-for-byte the ordering of list_pending_page.
    let sql = "SELECT id FROM task WHERE status = 'pending' \
               ORDER BY priority DESC, created_at ASC, id ASC LIMIT 500";
    let plan = b6b_explain_plan(&state.pool, sql, true).await;

    assert!(
        plan.contains("idx_task_priority"),
        "the Pending claim scan must be served by idx_task_priority; plan was:\n{plan}"
    );
    assert!(
        !plan.contains("Incremental Sort") && !plan.contains("Sort Key"),
        "the Pending claim scan must not need a Sort/Incremental Sort node once the \
         index carries the id tiebreaker; plan was:\n{plan}"
    );
}

/// B7 — the timeout loop drains a mass-timeout in bounded passes: with a small
/// `timeout_batch_size`, more tasks than one batch still all get timed out within
/// a single loop iteration (the drain re-fetches until exhausted).
#[tokio::test]
async fn test_audit2_b6b_timeout_loop_drains_more_than_batch() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // 15 tasks, timeout=1s each.
    let mut payloads = Vec::new();
    for i in 0..15 {
        payloads.push(json!({
            "id": format!("b6b-to-{}", i),
            "name": format!("B6b timeout {}", i),
            "kind": "b6b-timeout",
            "timeout": 1,
            "metadata": {"test": true},
            "on_start": webhook_action()
        }));
    }
    let created = create_tasks_ok(&app, &payloads).await;
    let ids: Vec<uuid::Uuid> = created.iter().map(|t| t.id).collect();

    // Force all to Running with last_updated well in the past so they are overdue.
    {
        use diesel::sql_query;
        use diesel::sql_types::Timestamptz;
        use diesel_async::RunQueryDsl;
        let mut conn = state.pool.get().await.unwrap();
        let past = chrono::Utc::now() - chrono::Duration::seconds(120);
        sql_query(
            "UPDATE task SET status = 'running', started_at = $1, last_updated = $1 \
             WHERE kind = 'b6b-timeout'",
        )
        .bind::<Timestamptz, _>(past)
        .execute(&mut conn)
        .await
        .unwrap();
    }

    // Run the loop with a deliberately tiny batch size (5) so 15 tasks require a
    // multi-pass drain within a single iteration.
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let pool = state.pool.clone();
    let handle = tokio::spawn(async move {
        arcrun::workers::timeout_loop(
            pool,
            std::time::Duration::from_millis(50),
            std::time::Duration::from_secs(30),
            false,
            5,
            shutdown_rx,
            arcrun::workers::WorkerNudges::new(),
        )
        .await;
    });

    // Poll until all are Failure (or bail out).
    let mut all_failed = false;
    for _ in 0..40 {
        let mut n = 0;
        for &id in &ids {
            let t = get_task_ok(&app, id).await;
            if t.status == StatusKind::Failure {
                n += 1;
            }
        }
        if n == ids.len() {
            all_failed = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    let _ = shutdown_tx.send(true);
    let _ = handle.await;

    assert!(
        all_failed,
        "all 15 timed-out tasks must be Failure after the bounded drain (batch size 5)"
    );
}

/// B7 — probe endpoints answer fast under pool exhaustion (bounded acquire, no 30s
/// hang): `/ready` returns 503 (shed traffic), `/health` returns 200 degraded
/// (liveness must not restart the pod). A pool of size 1 with its only connection
/// held out makes every probe acquisition block until the short probe timeout.
#[tokio::test]
async fn test_audit2_b6b_probes_fast_under_pool_exhaustion() {
    let test_app = setup_test_db_with_pool_size(1).await;
    let state = create_test_state(test_app.pool.clone());
    let app = test_service!(state);

    // Hold the pool's only connection so every probe acquisition blocks.
    let _held = state.pool.get().await.unwrap();

    // /ready must return 503 quickly (well under the pool's 30s connection timeout).
    let start = std::time::Instant::now();
    let req = actix_web::test::TestRequest::get()
        .uri("/ready")
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    let elapsed = start.elapsed();
    assert_eq!(
        resp.status(),
        actix_web::http::StatusCode::SERVICE_UNAVAILABLE,
        "/ready must be 503 when the pool is exhausted"
    );
    assert!(
        elapsed < std::time::Duration::from_secs(5),
        "/ready must answer fast (bounded acquire), took {:?}",
        elapsed
    );

    // /health must return 200 quickly with a degraded body (liveness stays up).
    let start = std::time::Instant::now();
    let req = actix_web::test::TestRequest::get()
        .uri("/health")
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    let elapsed = start.elapsed();
    assert_eq!(
        resp.status(),
        actix_web::http::StatusCode::OK,
        "/health must be 200 even under pool exhaustion (liveness must not restart the pod)"
    );
    assert!(
        elapsed < std::time::Duration::from_secs(5),
        "/health must answer fast (bounded acquire), took {:?}",
        elapsed
    );

    let body: serde_json::Value = actix_web::test::read_body_json(resp).await;
    assert_eq!(
        body["status"], "degraded",
        "/health body must report degraded under pool exhaustion; got {body}"
    );

    drop(_held);
}
