#![allow(clippy::redundant_field_names)]

//! Dedupe locks must be ordered across the whole insertion transaction.

use crate::common::*;
use actix_web::http::StatusCode;
use arcrun::{DbPool, models::StatusKind, rule::Matcher};
use diesel_async::RunQueryDsl;
use serde_json::json;
use std::time::Duration;

#[derive(diesel::QueryableByName)]
struct Pid {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    pid: i32,
}

#[derive(diesel::QueryableByName)]
struct Count {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    n: i64,
}

async fn wait_for_blocker(pool: &DbPool, blocker: i32) -> i32 {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut observer = pool.get().await.unwrap();
        loop {
            let waiters: Vec<Pid> = diesel::sql_query(
                "SELECT pid FROM pg_stat_activity WHERE datname=current_database()
                 AND $1=ANY(pg_blocking_pids(pid))",
            )
            .bind::<diesel::sql_types::Integer, _>(blocker)
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
    .expect("expected advisory-lock wait edge did not appear")
}

fn matcher(kind: &str) -> Matcher {
    Matcher {
        kind: kind.to_string(),
        status: StatusKind::Pending,
        fields: vec![],
    }
}

fn dedupe_task(id: &str, matcher: &Matcher) -> serde_json::Value {
    let mut task = task_json(id, id, &matcher.kind);
    task["dedupe_strategy"] = json!([matcher]);
    task
}

/// Bug 3: sorting each task's keys allowed H->L and L->H across two batches.
/// A third key G gates the first request after H (old code) or L,H (fixed code).
/// The second request then waits on the first; releasing G creates the old cycle
/// deterministically, without a FIFO assumption, timing sleep or production hook.
#[tokio::test]
async fn test_bug3_dedupe_opposite_batch_orders_do_not_deadlock() {
    let (_guard, state) = setup_test_app().await;
    let app = test_service!(state);
    let mut matchers = [
        matcher("audit9-a"),
        matcher("audit9-b"),
        matcher("audit9-c"),
    ];
    matchers.sort_unstable_by_key(|m| arcrun::rule::dedupe_lock_key(m, &json!({})));
    let [low, high, gate] = &matchers;
    let keys: Vec<_> = matchers
        .iter()
        .map(|m| arcrun::rule::dedupe_lock_key(m, &json!({})))
        .collect();
    assert!(keys.windows(2).all(|w| w[0] < w[1]));

    let mut holder = state.pool.get().await.unwrap();
    diesel::sql_query("BEGIN")
        .execute(&mut holder)
        .await
        .unwrap();
    diesel::sql_query("SELECT pg_advisory_xact_lock($1)")
        .bind::<diesel::sql_types::BigInt, _>(keys[2])
        .execute(&mut holder)
        .await
        .unwrap();
    let holder_pid = diesel::sql_query("SELECT pg_backend_pid() AS pid")
        .get_result::<Pid>(&mut holder)
        .await
        .unwrap()
        .pid;

    let mut first_task = dedupe_task("high", high);
    first_task["dedupe_strategy"] = json!([high, gate]);
    let first_body = json!({"scope":"first", "tasks":[first_task, dedupe_task("low", low)]});
    let second_body =
        json!({"scope":"second", "tasks":[dedupe_task("low", low), dedupe_task("high", high)]});
    let (start_second, second_can_start) = tokio::sync::oneshot::channel();
    let first = async {
        let request = actix_web::test::TestRequest::post()
            .uri("/task")
            .set_json(first_body)
            .to_request();
        let response = actix_web::test::call_service(&app, request).await;
        (
            response.status(),
            actix_web::test::read_body(response).await,
        )
    };
    let second = async {
        second_can_start.await.unwrap();
        let request = actix_web::test::TestRequest::post()
            .uri("/task")
            .set_json(second_body)
            .to_request();
        let response = actix_web::test::call_service(&app, request).await;
        (
            response.status(),
            actix_web::test::read_body(response).await,
        )
    };
    let release = async {
        let first_pid = wait_for_blocker(&state.pool, holder_pid).await;
        start_second.send(()).unwrap();
        wait_for_blocker(&state.pool, first_pid).await;
        diesel::sql_query("ROLLBACK")
            .execute(&mut holder)
            .await
            .unwrap();
    };
    let (first, second, ()) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(first, second, release)
    })
    .await
    .unwrap();
    assert_eq!(first.0, StatusCode::CREATED, "{first:?}");
    assert_eq!(second.0, StatusCode::NO_CONTENT, "{second:?}");
    for (table, expected) in [("task", 2), ("action", 2), ("batch", 2)] {
        let count: Count = diesel::sql_query(format!("SELECT count(*) AS n FROM {table}"))
            .get_result(&mut holder)
            .await
            .unwrap();
        assert_eq!(count.n, expected, "{table}");
    }
    let remaining: Vec<Count> =
        diesel::sql_query("SELECT remaining::bigint AS n FROM batch ORDER BY remaining")
            .load(&mut holder)
            .await
            .unwrap();
    assert_eq!(remaining.iter().map(|r| r.n).collect::<Vec<_>>(), [0, 2]);
}

/// Global lock collection must not move the dedupe checks before earlier inserts
/// or reorder tasks/dependencies. Three dedupe tasks use one locking statement.
#[tokio::test]
async fn test_bug3_dedupe_preserves_input_order_with_one_lock_query() {
    use diesel::connection::InstrumentationEvent;
    use diesel_async::AsyncConnection;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let guard = setup_test_db_with_pool_size(1).await;
    let state = create_test_state(guard.pool.clone());
    let app = test_service!(state);
    let lock_queries = Arc::new(AtomicUsize::new(0));
    let observed = lock_queries.clone();
    guard
        .pool
        .get()
        .await
        .unwrap()
        .set_instrumentation(move |event: InstrumentationEvent<'_>| {
            if let InstrumentationEvent::StartQuery { query, .. } = event
                && query.to_string().contains("pg_advisory_xact_lock")
            {
                observed.fetch_add(1, Ordering::Relaxed);
            }
        });

    let request = actix_web::test::TestRequest::post()
        .uri("/task")
        .set_json(json!({"scope":"ordered", "tasks":[
            task_json("prefix", "prefix", "audit9-prefix"),
            dedupe_task("skip-prefix", &matcher("audit9-prefix")),
            task_with_deps("child", "child", "audit9-child", vec![("prefix", true), ("skip-prefix", true)]),
            dedupe_task("fresh", &matcher("audit9-fresh")),
            dedupe_task("skip-fresh", &matcher("audit9-fresh"))
        ]}))
        .to_request();
    let response = actix_web::test::call_service(&app, request).await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let created: Vec<arcrun::dtos::BasicTaskDto> = actix_web::test::read_body_json(response).await;
    assert_eq!(
        created.iter().map(|t| t.name.as_str()).collect::<Vec<_>>(),
        ["prefix", "child", "fresh"]
    );
    let child = get_task_ok(&app, created[1].id).await;
    assert_eq!(child.status, StatusKind::Waiting);
    let mut conn = guard.pool.get().await.unwrap();
    let dependency: Count = diesel::sql_query(
        "SELECT count(*) AS n FROM task t JOIN link l ON l.child_id=t.id
         WHERE t.id=$1 AND l.parent_id=$2 AND t.wait_finished=1 AND t.wait_success=1",
    )
    .bind::<diesel::sql_types::Uuid, _>(created[1].id)
    .bind::<diesel::sql_types::Uuid, _>(created[0].id)
    .get_result(&mut conn)
    .await
    .unwrap();
    assert_eq!(
        dependency.n, 1,
        "only the inserted parent contributes a dependency"
    );
    let remaining: Count = diesel::sql_query("SELECT remaining::bigint AS n FROM batch")
        .get_result(&mut conn)
        .await
        .unwrap();
    assert_eq!(remaining.n, 3);
    assert_eq!(lock_queries.load(Ordering::Relaxed), 1);
    drop(conn);
    create_tasks_ok(&app, &[task_json("plain", "plain", "audit9-plain")]).await;
    assert_eq!(
        lock_queries.load(Ordering::Relaxed),
        1,
        "no lock for a rule-free batch"
    );
}

/// Missing metadata/fields make a matcher inapplicable. Collecting every declared
/// key would introduce spurious waits; those rules must take no advisory lock.
#[tokio::test]
async fn test_bug3_dedupe_skipped_matchers_do_not_lock() {
    let (_guard, state) = setup_test_app().await;
    let app = test_service!(state);
    let mut no_metadata_rule = matcher("audit9-no-metadata");
    no_metadata_rule.fields = vec!["missing".to_string()];
    let mut missing_field_rule = matcher("audit9-missing-field");
    missing_field_rule.fields = vec!["missing".to_string()];
    let mut no_metadata = dedupe_task("no-metadata", &no_metadata_rule);
    no_metadata.as_object_mut().unwrap().remove("metadata");
    let missing_field = dedupe_task("missing-field", &missing_field_rule);
    let mut holder = state.pool.get().await.unwrap();
    diesel::sql_query("BEGIN")
        .execute(&mut holder)
        .await
        .unwrap();
    for key in [
        arcrun::rule::dedupe_lock_key(&no_metadata_rule, &serde_json::Value::Null),
        arcrun::rule::dedupe_lock_key(&missing_field_rule, &missing_field["metadata"]),
    ] {
        diesel::sql_query("SELECT pg_advisory_xact_lock($1)")
            .bind::<diesel::sql_types::BigInt, _>(key)
            .execute(&mut holder)
            .await
            .unwrap();
    }
    let request = actix_web::test::TestRequest::post()
        .uri("/task")
        .set_json([no_metadata, missing_field])
        .to_request();
    let response = tokio::time::timeout(
        Duration::from_secs(2),
        actix_web::test::call_service(&app, request),
    )
    .await;
    diesel::sql_query("ROLLBACK")
        .execute(&mut holder)
        .await
        .unwrap();
    assert_eq!(response.unwrap().status(), StatusCode::CREATED);
}
