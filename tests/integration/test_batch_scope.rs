// Diesel QueryableByName emits separate impls with explicit field initializers;
// an allow on the struct does not cover those generated impls.
#![allow(clippy::redundant_field_names)]

//! Tasker #601 — batch scope / metadata.
//!
//! A `POST /task` object body may carry a `scope` (text label) and/or `metadata`
//! (arbitrary JSON) on the batch. These are stored on the `batch` row and exposed by
//! `GET /batches` (and `GET /batch/{id}`), which can filter by them:
//!   * `?scope=` — exact match
//!   * `?metadata={...}` — JSONB containment (`@>`)
//!   * `?search=` — substring across scope + metadata text
//!
//! Critically, creating a batch row for a scope/metadata-only batch (no webhook) must
//! NOT enqueue a `batch_complete` signal — that's gated on a non-empty `on_complete`.

use crate::common::*;

use arcrun::dtos::{BasicTaskDto, BatchStatsDto, BatchSummaryDto};
use serde_json::json;

// ============================================================================
// Helpers
// ============================================================================

/// Percent-encode a query-parameter value (encodes everything that isn't an
/// unreserved URI char) so a JSON object can be passed in `?metadata=`.
fn enc(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}

/// POST an object-form body; assert 201 and return (batch_id, created tasks).
async fn create_batch<S, B>(app: &S, body: &serde_json::Value) -> (uuid::Uuid, Vec<BasicTaskDto>)
where
    S: actix_web::dev::Service<
            actix_http::Request,
            Response = actix_web::dev::ServiceResponse<B>,
            Error = actix_web::Error,
        >,
    B: actix_web::body::MessageBody,
{
    let req = actix_web::test::TestRequest::post()
        .uri("/task")
        .set_json(body)
        .to_request();
    let resp = actix_web::test::call_service(app, req).await;
    assert_eq!(
        resp.status(),
        actix_web::http::StatusCode::CREATED,
        "expected 201 Created"
    );
    let batch_id = resp
        .headers()
        .get("X-Batch-ID")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| uuid::Uuid::parse_str(s).ok())
        .expect("X-Batch-ID header");
    let tasks: Vec<BasicTaskDto> = actix_web::test::read_body_json(resp).await;
    (batch_id, tasks)
}

/// GET /batches with a raw query string; return the parsed summaries.
async fn list_batches<S, B>(app: &S, query: &str) -> Vec<BatchSummaryDto>
where
    S: actix_web::dev::Service<
            actix_http::Request,
            Response = actix_web::dev::ServiceResponse<B>,
            Error = actix_web::Error,
        >,
    B: actix_web::body::MessageBody,
{
    let uri = if query.is_empty() {
        "/batches".to_string()
    } else {
        format!("/batches?{}", query)
    };
    let req = actix_web::test::TestRequest::get().uri(&uri).to_request();
    let resp = actix_web::test::call_service(app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::OK);
    actix_web::test::read_body_json(resp).await
}

/// Total batch_complete outbox rows for a batch (any status).
async fn batch_complete_rows(pool: &arcrun::DbPool, batch_id: uuid::Uuid) -> i64 {
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

fn one_task(id: &str, kind: &str) -> serde_json::Value {
    json!({ "id": id, "name": id, "kind": kind, "timeout": 60, "on_start": webhook_action() })
}

// ============================================================================
// Storage + read-back
// ============================================================================

/// A batch created with scope + metadata stores them and surfaces them via
/// GET /batches and GET /batch/{id}.
#[tokio::test]
async fn test_batch_scope_metadata_stored() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let body = json!({
        "tasks": [one_task("a", "scoped")],
        "scope": "team-payments",
        "metadata": {"env": "prod", "region": "eu"}
    });
    let (batch_id, _) = create_batch(&app, &body).await;

    // GET /batches surfaces scope + metadata.
    let batches = list_batches(&app, &format!("scope={}", enc("team-payments"))).await;
    let b = batches
        .iter()
        .find(|b| b.batch_id == batch_id)
        .expect("batch present");
    assert_eq!(b.scope.as_deref(), Some("team-payments"));
    assert_eq!(b.metadata, json!({"env": "prod", "region": "eu"}));

    // GET /batch/{id} stats also carries them.
    let req = actix_web::test::TestRequest::get()
        .uri(&format!("/batch/{}", batch_id))
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    let stats: BatchStatsDto = actix_web::test::read_body_json(resp).await;
    assert_eq!(stats.scope.as_deref(), Some("team-payments"));
    assert_eq!(stats.metadata, json!({"env": "prod", "region": "eu"}));
}

// ============================================================================
// Filter: scope (exact)
// ============================================================================

#[tokio::test]
async fn test_batches_filter_by_scope_exact() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let (alpha, _) = create_batch(
        &app,
        &json!({"tasks": [one_task("a", "k")], "scope": "alpha"}),
    )
    .await;
    let (beta, _) = create_batch(
        &app,
        &json!({"tasks": [one_task("b", "k")], "scope": "beta"}),
    )
    .await;

    let batches = list_batches(&app, "scope=alpha").await;
    assert!(batches.iter().any(|b| b.batch_id == alpha));
    assert!(
        !batches.iter().any(|b| b.batch_id == beta),
        "scope filter is exact: beta must be excluded"
    );
    // Exact, not substring: a prefix must not match.
    let none = list_batches(&app, "scope=alph").await;
    assert!(!none.iter().any(|b| b.batch_id == alpha));
}

// ============================================================================
// Filter: metadata (JSONB containment)
// ============================================================================

#[tokio::test]
async fn test_batches_filter_by_metadata_containment() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let (prod, _) = create_batch(
        &app,
        &json!({"tasks": [one_task("a", "k")], "metadata": {"env": "prod", "team": "x"}}),
    )
    .await;
    let (staging, _) = create_batch(
        &app,
        &json!({"tasks": [one_task("b", "k")], "metadata": {"env": "staging"}}),
    )
    .await;

    let batches = list_batches(&app, &format!("metadata={}", enc(r#"{"env":"prod"}"#))).await;
    assert!(
        batches.iter().any(|b| b.batch_id == prod),
        "containment should match the prod batch"
    );
    assert!(
        !batches.iter().any(|b| b.batch_id == staging),
        "containment should exclude the staging batch"
    );
}

/// Invalid JSON must not silently turn into an unfiltered listing.
#[tokio::test]
async fn test_batches_reject_malformed_metadata_filter() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let req = actix_web::test::TestRequest::get()
        .uri("/batches?metadata=%7Bnot-json%7D")
        .to_request();
    let response = actix_web::test::call_service(&app, req).await;
    assert_eq!(response.status(), actix_web::http::StatusCode::BAD_REQUEST);
}

// ============================================================================
// Search: substring across scope + metadata text
// ============================================================================

#[tokio::test]
async fn test_batches_search_substring() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // Matches via scope substring.
    let (by_scope, _) = create_batch(
        &app,
        &json!({"tasks": [one_task("a", "k")], "scope": "nightly-build-42"}),
    )
    .await;
    // Matches via metadata value substring.
    let (by_meta, _) = create_batch(
        &app,
        &json!({"tasks": [one_task("b", "k")], "metadata": {"pipeline": "nightly-build-99"}}),
    )
    .await;
    // Should not match.
    let (other, _) = create_batch(
        &app,
        &json!({"tasks": [one_task("c", "k")], "scope": "release"}),
    )
    .await;

    let batches = list_batches(&app, &format!("search={}", enc("nightly-build"))).await;
    assert!(batches.iter().any(|b| b.batch_id == by_scope), "scope hit");
    assert!(
        batches.iter().any(|b| b.batch_id == by_meta),
        "metadata hit"
    );
    assert!(
        !batches.iter().any(|b| b.batch_id == other),
        "non-matching batch excluded"
    );
}

// ============================================================================
// Gating: scope/metadata-only batch must NOT signal batch_complete
// ============================================================================

/// A batch with scope/metadata but NO on_batch_complete creates a `batch` row, yet
/// completing all its tasks enqueues no batch_complete outbox row.
#[tokio::test]
async fn test_scope_only_batch_no_complete_signal() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let (batch_id, tasks) = create_batch(
        &app,
        &json!({"tasks": [one_task("a", "k")], "scope": "no-webhook", "metadata": {"x": 1}}),
    )
    .await;

    // Drive the only task terminal — this decrements batch.remaining (D2).
    succeed_task(&state, tasks[0].id).await;

    assert_eq!(
        batch_complete_rows(&state.pool, batch_id).await,
        0,
        "scope/metadata-only batch (empty on_complete) must not enqueue a batch_complete signal"
    );
    // The batch row still exists (scope is queryable).
    let batches = list_batches(&app, "scope=no-webhook").await;
    assert!(batches.iter().any(|b| b.batch_id == batch_id));
}

/// A batch with BOTH on_batch_complete AND scope/metadata still enqueues the
/// batch_complete signal (non-regression of Lot 3b) and stores the scope.
#[tokio::test]
async fn test_batch_complete_still_fires_with_scope() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let body = json!({
        "tasks": [one_task("a", "k")],
        "scope": "with-webhook",
        "metadata": {"env": "prod"},
        "on_batch_complete": [{"kind": "Webhook", "params": {"url": "https://example.com/done", "verb": "Post"}}]
    });
    let (batch_id, tasks) = create_batch(&app, &body).await;

    succeed_task(&state, tasks[0].id).await;

    assert_eq!(
        batch_complete_rows(&state.pool, batch_id).await,
        1,
        "a batch with on_batch_complete must enqueue exactly one batch_complete signal"
    );
    let batches = list_batches(&app, "scope=with-webhook").await;
    let b = batches.iter().find(|b| b.batch_id == batch_id).unwrap();
    assert_eq!(b.scope.as_deref(), Some("with-webhook"));
}

// ============================================================================
// Backwards compatibility
// ============================================================================

/// The legacy bare-array body still works and carries no scope (null) / empty metadata.
#[tokio::test]
async fn test_legacy_bare_array_has_no_scope() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let created = create_tasks_ok(&app, &[one_task("a", "legacy")]).await;
    let batch_id = created[0].batch_id.expect("task has a batch_id");

    let batches = list_batches(&app, "").await;
    let b = batches
        .iter()
        .find(|b| b.batch_id == batch_id)
        .expect("legacy batch listed");
    assert!(b.scope.is_none(), "bare-array batch has no scope");
    assert_eq!(b.metadata, json!({}), "bare-array batch has empty metadata");
}
