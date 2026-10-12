#![allow(clippy::redundant_field_names)]

//! Batch listing: page before aggregating kinds, without changing filter semantics.

use crate::common::*;
use arcrun::dtos::BatchSummaryDto;
use chrono::{DateTime, Utc};
use diesel::sql_types;
use diesel_async::{AsyncConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::{Value, json};

fn bid(n: u128) -> uuid::Uuid {
    uuid::Uuid::from_u128(n)
}

fn day(n: u32) -> DateTime<Utc> {
    format!("2026-01-{n:02}T00:00:00Z").parse().unwrap()
}

async fn seed(pool: &arcrun::DbPool) {
    // Deliberately different task timestamps: the page ranks matching tasks,
    // whereas its output dates/counts describe every hot task in each batch.
    // Batch 2 has separate matches for each filter, but no single complete match.
    let mut conn = pool.get().await.unwrap();
    conn.batch_execute(
        "INSERT INTO task (id,batch_id,name,kind,status,timeout,metadata,start_condition,
                           created_at,last_updated,ended_at)
         SELECT lpad(to_hex(id),32,'0')::uuid, lpad(to_hex(bid),32,'0')::uuid,
                name,kind,status::status_kind,60,'{}','[]',
                '2026-01-01'::timestamptz + (created_day-1)*interval '1 day',
                '2026-01-01'::timestamptz + (updated_day-1)*interval '1 day',
                CASE WHEN id IN (11,51) THEN now()-interval '40 days' END
         FROM (VALUES
           (11,1,'ordinary','unrelated','success',1,12),
           (12,1,'needle_%','kind_%','success',3,4),
           (13,1,'needle_%','kind_%extra','success',3,5),
           (14,1,'needle_%','kind_%','pending',4,11),
           (15,1,'needle_%','kind_%','failure',5,6),
           (16,1,'needle_%','other','success',3,4),
           (17,1,'other','kind_%','success',3,4),
           (18,1,'needle_%','kind_%','success',7,8),
           (21,2,'other','kind_%','success',2,2),
           (22,2,'needle_%','kind_%','pending',2,2),
           (23,2,'needle_%','other','success',2,2),
           (31,3,'needle_%','kind_%','success',2,2),
           (32,3,'ordinary','other','pending',2,3),
           (51,5,'archive','archived-only','success',4,4),
           (101,10,'tie','tie','paused',3,3),
           (111,11,'tie','tie','paused',3,3)
         ) AS fixture(id,bid,name,kind,status,created_day,updated_day);
         INSERT INTO batch (id,on_complete,scope,metadata,remaining)
         SELECT lpad(to_hex(id),32,'0')::uuid,'[]','team_%',
                '{\"env\":\"prod\",\"label\":\"literal_%\"}',1
         FROM (VALUES (1),(2)) AS fixture(id);
         INSERT INTO batch (id,on_complete,remaining)
         VALUES ('00000000-0000-0000-0000-000000000004','[]',0);",
    )
    .await
    .unwrap();
}

async fn listing<S, B>(app: &S, query: &str) -> Vec<BatchSummaryDto>
where
    S: actix_web::dev::Service<
            actix_http::Request,
            Response = actix_web::dev::ServiceResponse<B>,
            Error = actix_web::Error,
        >,
    B: actix_web::body::MessageBody,
{
    let request = actix_web::test::TestRequest::get()
        .uri(&format!("/batches?{query}"))
        .to_request();
    let response = actix_web::test::call_service(app, request).await;
    assert_eq!(response.status(), actix_web::http::StatusCode::OK);
    actix_web::test::read_body_json(response).await
}

const TASK_FILTERS: &str = "kind=kind_%25&status=Success&name=needle_%25";

/// Bug 5 optimization must not broaden AND filters to different tasks or make
/// counts/dates describe only the filtered subset. Literal %/_ and inclusive
/// timestamp bounds must still apply to the page's kinds as well as selection.
#[tokio::test]
async fn test_bug5_listing_filters_share_a_task_and_keep_full_counts() {
    let (_guard, state) = setup_test_app().await;
    seed(&state.pool).await;
    let app = test_service!(state);
    let query = format!(
        "{TASK_FILTERS}&created_after=2026-01-02T00:00:00Z&created_before=2026-01-03T00:00:00Z"
    );
    let summaries = listing(&app, &query).await;
    assert_eq!(
        summaries.iter().map(|b| b.batch_id).collect::<Vec<_>>(),
        [bid(1), bid(3)]
    );
    let summary = &summaries[0];
    assert_eq!(summary.total_tasks, 8);
    assert_eq!(summary.status_counts.success, 6);
    assert_eq!(summary.status_counts.pending, 1);
    assert_eq!(summary.status_counts.failure, 1);
    assert_eq!(summary.first_created_at, day(1));
    assert_eq!(summary.latest_updated_at, day(12));
    assert_eq!(summary.kinds, ["kind_%", "kind_%extra"]);
    assert_eq!(summary.remaining, Some(1));
    assert_eq!(summary.scope.as_deref(), Some("team_%"));
    assert_eq!(summary.metadata, json!({"env":"prod","label":"literal_%"}));

    let all_filters =
        format!("{query}&scope=team_%25&metadata=%7B%22env%22%3A%22prod%22%7D&search=LITERAL_%25");
    let filtered = listing(&app, &all_filters).await;
    assert_eq!(filtered.len(), 1);
    assert_eq!(
        serde_json::to_value(&filtered[0]).unwrap(),
        serde_json::to_value(summary).unwrap()
    );
    assert!(listing(&app, "kind=kindX").await.is_empty());
}

/// Bug 5 optimization must keep page membership based on the first matching
/// task, even when the displayed first-created timestamp belongs to another task.
/// Preserve the existing UUID tie behavior: DESC for page selection, ASC inside
/// the returned page. Changing that API behavior is outside this optimization.
#[tokio::test]
async fn test_bug5_listing_pagination_uses_matching_dates() {
    let (_guard, state) = setup_test_app().await;
    seed(&state.pool).await;
    let app = test_service!(state);
    for (page, expected) in [(0, 1), (1, 3)] {
        let summaries = listing(&app, &format!("{TASK_FILTERS}&page_size=1&page={page}")).await;
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].batch_id, bid(expected));
        assert_eq!(
            summaries[0].first_created_at,
            day(if page == 0 { 1 } else { 2 })
        );
    }
    assert!(
        listing(&app, &format!("{TASK_FILTERS}&page_size=1&page=2"))
            .await
            .is_empty()
    );
    let tied = listing(&app, "kind=tie&page_size=2").await;
    assert_eq!(
        tied.iter().map(|b| b.batch_id).collect::<Vec<_>>(),
        [bid(10), bid(11)]
    );
    for (page, expected) in [(0, 11), (1, 10)] {
        let summaries = listing(&app, &format!("kind=tie&page_size=1&page={page}")).await;
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].batch_id, bid(expected));
    }
}

/// Bug 5 optimization must retain batches represented only by hot tasks, omit
/// empty persisted batches, and recalculate dates/kinds/counts after retention.
#[tokio::test]
async fn test_bug5_listing_tracks_hot_rows_and_optional_batch_identity() {
    let (_guard, state) = setup_test_app().await;
    seed(&state.pool).await;
    let app = test_service!(state);
    let before = listing(&app, "").await;
    let legacy = before.iter().find(|b| b.batch_id == bid(3)).unwrap();
    assert!(legacy.remaining.is_none());
    assert!(legacy.scope.is_none());
    assert_eq!(legacy.metadata, json!({}));
    assert!(!before.iter().any(|b| b.batch_id == bid(4)));
    assert!(before.iter().any(|b| b.batch_id == bid(5)));
    let before = before.iter().find(|b| b.batch_id == bid(1)).unwrap();
    assert_eq!(
        before.kinds,
        ["kind_%", "kind_%extra", "other", "unrelated"]
    );

    let archived = arcrun::db_operation::cleanup_old_terminal_tasks(
        &mut state.pool.get().await.unwrap(),
        30,
        100,
    )
    .await
    .unwrap();
    assert_eq!(archived, 2);
    let after = listing(&app, "").await;
    assert!(!after.iter().any(|b| b.batch_id == bid(5)));
    let remaining = after.iter().find(|b| b.batch_id == bid(1)).unwrap();
    assert_eq!(remaining.total_tasks, 7);
    assert_eq!(remaining.status_counts.success, 5);
    assert_eq!(remaining.first_created_at, day(3));
    assert_eq!(remaining.latest_updated_at, day(11));
    assert_eq!(remaining.kinds, ["kind_%", "kind_%extra", "other"]);
    assert_eq!(remaining.remaining, Some(1));
}

#[derive(diesel::QueryableByName)]
struct QueryPlan {
    #[diesel(sql_type = sql_types::Json, column_name = "QUERY PLAN")]
    plan: Value,
}

fn kinds_aggregate_inputs(node: &Value) -> (usize, f64) {
    let carries_kinds = node["Node Type"] == "Aggregate"
        && node["Output"].as_array().is_some_and(|output| {
            output
                .iter()
                .any(|value| value.as_str().is_some_and(|s| s.contains("array_agg(")))
        });
    let mut count = usize::from(carries_kinds);
    let mut rows = if carries_kinds {
        let input = &node["Plans"][0];
        // PostgreSQL 18 reports fractional per-loop row counts. as_f64 also
        // accepts the integer JSON numbers emitted by older server versions.
        input["Actual Rows"].as_f64().unwrap() * input["Actual Loops"].as_f64().unwrap()
    } else {
        0.0
    };
    if let Some(children) = node["Plans"].as_array() {
        for child in children {
            let (child_count, child_rows) = kinds_aggregate_inputs(child);
            count += child_count;
            rows += child_rows;
        }
    }
    (count, rows)
}

fn task_rows_visited(node: &Value) -> f64 {
    let own = if node["Relation Name"] == "task" {
        (node["Actual Rows"].as_f64().unwrap()
            + node["Rows Removed by Filter"].as_f64().unwrap_or(0.0))
            * node["Actual Loops"].as_f64().unwrap()
    } else {
        0.0
    };
    own + node["Plans"]
        .as_array()
        .map_or(0.0, |children| children.iter().map(task_rows_visited).sum())
}

/// Bug 5: ARRAY_AGG(DISTINCT kind) used to consume the entire matching history
/// before LIMIT. Inspect the actual route's query, then EXPLAIN ANALYZE it: kinds
/// aggregation may consume only tasks on this page. Catalog selection must also
/// avoid reading the other tasks. No timing threshold, index choice or join
/// algorithm is prescribed. Reverting either listing optimization makes this fail.
#[tokio::test]
async fn test_bug5_listing_aggregates_kinds_only_on_the_page() {
    use diesel::connection::InstrumentationEvent;
    use std::sync::{Arc, Mutex};

    let guard = setup_test_db_with_pool_size(1).await;
    let state = create_test_state(guard.pool.clone());
    let app = test_service!(state);
    let mut conn = state.pool.get().await.unwrap();
    conn.batch_execute(
        "INSERT INTO task (batch_id,name,kind,status,timeout,metadata,start_condition,created_at)
         SELECT lpad(to_hex((i-1)/12),32,'0')::uuid,'task','kind-'||(i%7),
                (ARRAY['pending','success','failure'])[1+(i%3)]::status_kind,60,'{}','[]',
                '2026-01-01'::timestamptz + ((i-1)/12)*interval '1 second'
         FROM generate_series(1,3600) AS s(i);
         ANALYZE task; ANALYZE batch; ANALYZE batch_catalog;",
    )
    .await
    .unwrap();
    let queries = Arc::new(Mutex::new(Vec::new()));
    let captured = queries.clone();
    conn.set_instrumentation(move |event: InstrumentationEvent<'_>| {
        if let InstrumentationEvent::StartQuery { query, .. } = event {
            let query = query.to_string();
            if query.contains("paginated_batches") {
                captured.lock().unwrap().push(query);
            }
        }
    });
    drop(conn);
    let page = listing(&app, "page_size=2&page=1").await;
    assert_eq!(page.len(), 2);
    assert!(page.iter().all(|b| b.total_tasks == 12));
    let query = {
        let queries = queries.lock().unwrap();
        assert_eq!(queries.len(), 1);
        queries[0].split(" -- binds: ").next().unwrap().to_owned()
    };
    let mut conn = state.pool.get().await.unwrap();
    // Let PostgreSQL infer the route's parameter types (including status_kind).
    conn.batch_execute(&format!("PREPARE listing_plan AS {query}"))
        .await
        .unwrap();
    let plan: QueryPlan = diesel::sql_query(
        "EXPLAIN (ANALYZE, VERBOSE, TIMING OFF, FORMAT JSON)
         EXECUTE listing_plan(NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,2,2)",
    )
    .get_result(&mut conn)
    .await
    .unwrap();
    let (aggregates, input_rows) = kinds_aggregate_inputs(&plan.plan[0]["Plan"]);
    assert!(aggregates > 0, "no kinds aggregate found: {}", plan.plan);
    assert!(
        input_rows <= 24.0,
        "kinds aggregation consumed {input_rows} rows for a 24-task page"
    );
    assert!(
        task_rows_visited(&plan.plan[0]["Plan"]) <= 48.0,
        "catalog page selection still scans task history: {}",
        plan.plan
    );

    drop(conn);
    assert!(
        listing(&app, "status=Success&page=9223372036854775807")
            .await
            .is_empty()
    );
}
