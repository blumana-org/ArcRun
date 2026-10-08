use crate::common::*;

use arcrun::models::StatusKind;

/// Priority 1 — Requeue of stale Claimed tasks (timeout loop).
/// Simulates worker crash leaving tasks stuck in Claimed state.
///
/// Test that a Claimed task older than the threshold is requeued to Pending.
#[tokio::test]
async fn test_stale_claimed_task_requeued_to_pending() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let tasks = vec![task_json("stale-claim", "Stale Claimed Task", "stale-test")];
    let created = create_tasks_ok(&app, &tasks).await;
    let task_id = created[0].id;

    // Claim the task (Pending -> Claimed)
    let mut conn = state.pool.get().await.unwrap();
    let claimed = arcrun::db_operation::claim_task(&mut conn, &task_id)
        .await
        .unwrap();
    assert!(claimed);

    // Force last_updated to be far in the past (simulates crash)
    {
        use diesel::sql_query;
        use diesel::sql_types::{Timestamptz, Uuid as SqlUuid};
        use diesel_async::RunQueryDsl;

        let past = chrono::Utc::now() - chrono::Duration::seconds(120);
        sql_query("UPDATE task SET last_updated = $1 WHERE id = $2")
            .bind::<Timestamptz, _>(past)
            .bind::<SqlUuid, _>(task_id)
            .execute(&mut conn)
            .await
            .unwrap();
    }
    drop(conn);

    // Run timeout_loop which includes requeue_stale_claimed_tasks
    // claim_timeout = 30s, our task is 120s stale, so it should be requeued
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let pool = state.pool.clone();
    let handle = tokio::spawn(async move {
        arcrun::workers::timeout_loop(
            pool,
            std::time::Duration::from_millis(50),
            std::time::Duration::from_secs(30), // claim_timeout
            true,
            100,
            shutdown_rx,
            arcrun::workers::WorkerNudges::new(),
        )
        .await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let _ = shutdown_tx.send(true);
    let _ = handle.await;

    assert_task_status(
        &app,
        task_id,
        StatusKind::Pending,
        "stale claimed task should be requeued to Pending",
    )
    .await;
}

/// Test that a recently claimed task is NOT requeued.
#[tokio::test]
async fn test_recently_claimed_task_not_requeued() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let tasks = vec![task_json(
        "recent-claim",
        "Recent Claimed Task",
        "stale-test",
    )];
    let created = create_tasks_ok(&app, &tasks).await;
    let task_id = created[0].id;

    // Claim the task — last_updated is now()
    let mut conn = state.pool.get().await.unwrap();
    let claimed = arcrun::db_operation::claim_task(&mut conn, &task_id)
        .await
        .unwrap();
    assert!(claimed);
    drop(conn);

    // Run timeout loop with 30s claim_timeout — task was just claimed, should stay Claimed
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let pool = state.pool.clone();
    let handle = tokio::spawn(async move {
        arcrun::workers::timeout_loop(
            pool,
            std::time::Duration::from_millis(50),
            std::time::Duration::from_secs(30),
            true,
            100,
            shutdown_rx,
            arcrun::workers::WorkerNudges::new(),
        )
        .await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let _ = shutdown_tx.send(true);
    let _ = handle.await;

    // Task should still be Claimed (not requeued)
    let task = get_task_ok(&app, task_id).await;
    assert_eq!(
        task.status,
        StatusKind::Claimed,
        "recently claimed task should NOT be requeued"
    );
}

/// Test that a requeued task can be picked up again by start_loop.
#[tokio::test]
async fn test_requeued_task_picked_up_by_start_loop() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let tasks = vec![task_json("requeue-pickup", "Requeue Pickup", "stale-test")];
    let created = create_tasks_ok(&app, &tasks).await;
    let task_id = created[0].id;

    // Claim, then make it stale
    let mut conn = state.pool.get().await.unwrap();
    arcrun::db_operation::claim_task(&mut conn, &task_id)
        .await
        .unwrap();
    {
        use diesel::sql_query;
        use diesel::sql_types::{Timestamptz, Uuid as SqlUuid};
        use diesel_async::RunQueryDsl;

        let past = chrono::Utc::now() - chrono::Duration::seconds(120);
        sql_query("UPDATE task SET last_updated = $1 WHERE id = $2")
            .bind::<Timestamptz, _>(past)
            .bind::<SqlUuid, _>(task_id)
            .execute(&mut conn)
            .await
            .unwrap();
    }
    drop(conn);

    // Run timeout_loop to requeue
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let pool = state.pool.clone();
    let handle = tokio::spawn(async move {
        arcrun::workers::timeout_loop(
            pool,
            std::time::Duration::from_millis(50),
            std::time::Duration::from_secs(30),
            true,
            100,
            shutdown_rx,
            arcrun::workers::WorkerNudges::new(),
        )
        .await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let _ = shutdown_tx.send(true);
    let _ = handle.await;

    assert_task_status(
        &app,
        task_id,
        StatusKind::Pending,
        "should be requeued to Pending",
    )
    .await;

    // Now run start_loop — it should pick up the task (on_start will fail since webhook is example.com)
    let (shutdown_tx2, shutdown_rx2) = tokio::sync::watch::channel(false);
    let evaluator2 = state.action_executor.clone();
    let pool2 = state.pool.clone();
    let handle2 = tokio::spawn(async move {
        arcrun::workers::start_loop(
            &evaluator2,
            pool2,
            std::time::Duration::from_millis(50),
            true,
            50,
            10,
            shutdown_rx2,
            arcrun::workers::WorkerNudges::new(),
            std::time::Duration::from_secs(30),
        )
        .await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let _ = shutdown_tx2.send(true);
    let _ = handle2.await;

    // The task should no longer be Pending — it was picked up
    // (it will be Failed because example.com webhook fails, or Running if it somehow succeeds)
    let task = get_task_ok(&app, task_id).await;
    assert_ne!(
        task.status,
        StatusKind::Pending,
        "requeued task should have been picked up by start_loop"
    );
}
