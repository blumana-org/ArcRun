#![allow(clippy::redundant_field_names)]

//! Batch catalog lifecycle, transactional visibility and retention concurrency.

use crate::common::*;
use arcrun::DbPool;
use diesel::sql_types;
use diesel_async::{RunQueryDsl, SimpleAsyncConnection};
use serde_json::{Value, json};
use std::time::Duration;

const UP: &str = include_str!("../../migrations/2026-10-12-000001_batch_catalog/up.sql");
const DOWN: &str = include_str!("../../migrations/2026-10-12-000001_batch_catalog/down.sql");

#[derive(diesel::QueryableByName)]
struct Document {
    #[diesel(sql_type = sql_types::Jsonb)]
    value: Value,
}

async fn catalog(pool: &DbPool) -> Value {
    diesel::sql_query(
        "SELECT COALESCE(jsonb_agg(to_jsonb(c) ORDER BY batch_id), '[]') AS value
         FROM batch_catalog c",
    )
    .get_result::<Document>(&mut pool.get().await.unwrap())
    .await
    .unwrap()
    .value
}

async fn assert_catalog_matches_tasks(pool: &DbPool) {
    let expected = diesel::sql_query(
        "SELECT COALESCE(jsonb_agg(to_jsonb(c) ORDER BY batch_id), '[]') AS value
         FROM (SELECT batch_id, MIN(created_at) AS first_created_at,
                      MAX(created_at) AS last_created_at
               FROM task WHERE batch_id IS NOT NULL GROUP BY batch_id) c",
    )
    .get_result::<Document>(&mut pool.get().await.unwrap())
    .await
    .unwrap()
    .value;
    assert_eq!(catalog(pool).await, expected);
}

/// Bug 5: backfill includes bare batches and mixed dates, excludes empty optional
/// batch records, and installs maintenance atomically for old/new application code.
#[tokio::test]
async fn test_bug5_catalog_migration_backfills_existing_hot_batches() {
    let guard = setup_test_db().await;
    let mut conn = guard.pool.get().await.unwrap();
    conn.batch_execute(DOWN).await.unwrap();
    conn.batch_execute(
        "INSERT INTO task (batch_id,name,kind,status,timeout,metadata,start_condition,created_at)
         SELECT lpad(to_hex(i/3),32,'0')::uuid,'task','kind','success',60,'{}','[]',
                '2026-01-01Z'::timestamptz + i*interval '1 day'
         FROM generate_series(1,6) s(i);
         INSERT INTO batch (id,on_complete) VALUES (gen_random_uuid(),'[]');
         BEGIN;",
    )
    .await
    .unwrap();
    conn.batch_execute(UP).await.unwrap();
    conn.batch_execute("COMMIT").await.unwrap();
    assert_catalog_matches_tasks(&guard.pool).await;
    assert_eq!(catalog(&guard.pool).await.as_array().unwrap().len(), 3);
    // A failed insert transaction must leave neither tasks nor catalog entries.
    conn.batch_execute(
        "BEGIN;
         INSERT INTO task (batch_id,name,kind,status,timeout,metadata,start_condition)
         VALUES (gen_random_uuid(),'rollback','kind','pending',60,'{}','[]');
         ROLLBACK;",
    )
    .await
    .unwrap();
    assert_catalog_matches_tasks(&guard.pool).await;
    assert_eq!(catalog(&guard.pool).await.as_array().unwrap().len(), 3);
}

/// Bug 5: the registry must not activate batch.remaining or add lifecycle locks.
/// Hold the catalog row while the real claim/completion path executes. Its tuple
/// stays unchanged, and a bare-array POST still has no completion batch row.
#[tokio::test]
async fn test_bug5_catalog_does_not_lock_or_write_on_task_transitions() {
    let (_guard, state) = setup_test_app().await;
    let app = test_service!(state);
    let created = create_tasks_ok(&app, &[task_json("a", "a", "catalog")]).await;
    assert_catalog_matches_tasks(&state.pool).await;
    let before = catalog(&state.pool).await;
    let mut holder = state.pool.get().await.unwrap();
    holder
        .batch_execute("BEGIN; SELECT batch_id FROM batch_catalog FOR UPDATE;")
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), succeed_task(&state, created[0].id))
        .await
        .expect("terminal transition waited on catalog");
    holder.batch_execute("ROLLBACK").await.unwrap();
    assert_eq!(catalog(&state.pool).await, before);
    let batches: Document = diesel::sql_query(
        "SELECT jsonb_build_object('batch_rows', (SELECT COUNT(*) FROM batch),
                                  'catalog_rows', (SELECT COUNT(*) FROM batch_catalog)) AS value",
    )
    .get_result(&mut holder)
    .await
    .unwrap();
    assert_eq!(batches.value, json!({"batch_rows": 0, "catalog_rows": 1}));
}

/// Bug 5: multiple dedupe insertion runs must produce only one catalog tuple;
/// empty/all-skipped requests must not leave a phantom listing entry.
#[tokio::test]
async fn test_bug5_catalog_tracks_only_inserted_batches_without_rewrites() {
    let (_guard, state) = setup_test_app().await;
    let app = test_service!(state);
    let mut first = task_json("a", "a", "unique-a");
    first["dedupe_strategy"] = json!([{"kind":"unique-a","status":"Pending","fields":[]}]);
    let mut second = task_json("b", "b", "unique-b");
    second["dedupe_strategy"] = json!([{"kind":"unique-b","status":"Pending","fields":[]}]);
    let created = create_tasks_ok(&app, &[first.clone(), second.clone()]).await;
    assert_eq!(created.len(), 2);
    assert_catalog_matches_tasks(&state.pool).await;
    let before = catalog(&state.pool).await;
    assert_eq!(before.as_array().unwrap().len(), 1);
    for body in [json!([first, second]), json!([])] {
        let response = actix_web::test::call_service(
            &app,
            actix_web::test::TestRequest::post()
                .uri("/task")
                .set_json(body)
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), actix_web::http::StatusCode::NO_CONTENT);
    }
    assert_eq!(catalog(&state.pool).await, before);
    let mut conn = state.pool.get().await.unwrap();
    let tuple: Document = diesel::sql_query(
        "SELECT jsonb_build_object('ctid', ctid::text) AS value FROM batch_catalog",
    )
    .get_result(&mut conn)
    .await
    .unwrap();
    assert_eq!(
        tuple.value,
        json!({"ctid":"(0,1)"}),
        "catalog was rewritten"
    );
}

#[derive(diesel::QueryableByName)]
struct Pid {
    #[diesel(sql_type = sql_types::Integer)]
    pid: i32,
}

async fn wait_for_blocker(pool: &DbPool, blockers: &[i32], exclude: i32) -> i32 {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut observer = pool.get().await.unwrap();
        loop {
            let waiters: Vec<Pid> = diesel::sql_query(
                "SELECT pid FROM pg_stat_activity WHERE datname=current_database()
                 AND pg_blocking_pids(pid) && $1 AND pid <> $2",
            )
            .bind::<sql_types::Array<sql_types::Integer>, _>(blockers)
            .bind::<sql_types::Integer, _>(exclude)
            .load(&mut observer)
            .await
            .unwrap();
            if let Some(waiter) = waiters.first() {
                return waiter.pid;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("expected catalog lock wait did not appear")
}

/// Two retention passes delete disjoint tasks of the same batch. The second
/// waits on the catalog, then MUST re-read committed survivors on a fresh snapshot.
/// Otherwise it keeps a phantom batch (uniform case) or stale extrema (mixed).
async fn concurrent_retention(keep_survivor: bool) {
    let guard = setup_test_db().await;
    let mut holder = guard.pool.get().await.unwrap();
    holder
        .batch_execute(&format!(
            "INSERT INTO task (id,batch_id,name,kind,status,timeout,metadata,start_condition,
                               created_at,ended_at)
             SELECT lpad(to_hex(i),32,'0')::uuid, '00000000-0000-0000-0000-000000000099',
                    'task','kind','success',60,'{{}}','[]',
                    '2026-01-01Z'::timestamptz + {}*interval '1 day',
                    CASE WHEN i<3 THEN now()-interval '40 days' ELSE now() END
             FROM generate_series(1,{}) s(i);",
            if keep_survivor { "i" } else { "0" },
            if keep_survivor { 3 } else { 2 }
        ))
        .await
        .unwrap();
    // Commit the fixture before opening the lock-holder transaction: a single
    // simple-query message containing INSERT; BEGIN keeps that INSERT uncommitted.
    holder
        .batch_execute("BEGIN; SELECT batch_id FROM batch_catalog FOR UPDATE;")
        .await
        .unwrap();
    let pid = diesel::sql_query("SELECT pg_backend_pid() AS pid")
        .get_result::<Pid>(&mut holder)
        .await
        .unwrap()
        .pid;
    let (start_second, may_start) = tokio::sync::oneshot::channel();
    let cleanup = async {
        let mut conn = guard.pool.get().await.unwrap();
        arcrun::db_operation::cleanup_old_terminal_tasks(&mut conn, 30, 2)
            .await
            .unwrap()
    };
    // First pass sees only one eligible task because we explicitly lock task 2.
    let mut task_holder = guard.pool.get().await.unwrap();
    task_holder.batch_execute(
        "BEGIN; SELECT id FROM task WHERE id='00000000-0000-0000-0000-000000000002' FOR UPDATE;"
    ).await.unwrap();
    let second = async {
        may_start.await.unwrap();
        let mut conn = guard.pool.get().await.unwrap();
        arcrun::db_operation::cleanup_old_terminal_tasks(&mut conn, 30, 2)
            .await
            .unwrap()
    };
    let release = async {
        let first_pid = wait_for_blocker(&guard.pool, &[pid], 0).await;
        task_holder.batch_execute("ROLLBACK").await.unwrap();
        start_second.send(()).unwrap();
        // Observe both waiters, without relying on tuple-lock queue internals.
        wait_for_blocker(&guard.pool, &[pid, first_pid], first_pid).await;
        holder.batch_execute("ROLLBACK").await.unwrap();
    };
    let (first_count, second_count, ()) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(cleanup, second, release)
    })
    .await
    .expect("retention stalled");
    assert_eq!((first_count, second_count), (1, 1));
    assert_catalog_matches_tasks(&guard.pool).await;
    assert_eq!(
        catalog(&guard.pool).await.as_array().unwrap().len(),
        usize::from(keep_survivor)
    );
}

#[tokio::test]
async fn test_bug5_catalog_concurrent_retention_removes_last_tasks() {
    concurrent_retention(false).await;
}

#[tokio::test]
async fn test_bug5_catalog_concurrent_retention_rechecks_mixed_dates() {
    concurrent_retention(true).await;
}
