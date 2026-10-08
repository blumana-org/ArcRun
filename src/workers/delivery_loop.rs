// Diesel QueryableByName emits separate impls with explicit field initializers;
// an allow on the struct does not cover those generated impls.
#![allow(clippy::redundant_field_names)]

//! Webhook delivery loop (Lot 2 — transactional outbox).
//!
//! The 5th background worker. End/cancel webhooks are no longer fired inline in the
//! HTTP/worker call path: the status-change transaction enqueues a row in the dedicated
//! `webhook_outbox` queue (Audit 2 D3), and this loop drains it asynchronously with
//! at-least-once delivery semantics and exponential backoff. A terminated delivery leaves
//! the queue and is historised into `webhook_execution` as `success`/`exhausted`.
//!
//! Guarantees (see `docs/perf-correctness-plan.md`, "Contrat API cible"):
//! - **At-least-once**: a row survives crash/redeploy and is delivered on restart.
//! - **Start gate**: an `end`/`cancel` row waits while the task's `start` row is
//!   pending and fresh (enforced in `claim_due_outbox_leased`).
//! - **Idempotency**: the `Idempotency-Key` header (= `idempotency_key`) lets the
//!   consumer dedupe. A successful mark removes the row from the queue; a crash
//!   before that mark or an expired lease can still cause duplicate HTTP delivery.
//!
//! ## Bounded admission and streaming acknowledgements
//!
//! Each short claim transaction leases only as many rows as there are free slots.
//! Inputs are bulk-prefetched, then HTTP runs without retaining a connection.
//! Actions within an event remain sequential. Ready results are coalesced and
//! marked before refilling free slots, without waiting for slower events.
//!
//! Every result is fenced by its claim token. A transient DB failure retains the
//! result for retry and pauses admission; active jobs plus pending results never
//! exceed `concurrency`. After a process crash the durable leases expire and allow
//! redelivery. Shutdown drains admitted work; if its marks fail, leases recover it.

use std::{collections::HashMap, sync::Arc, time::Instant};

use actix_web::rt;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use tokio::sync::watch;

use crate::{
    Conn, DbPool,
    action::{ActionExecutor, WebhookEnrichment},
    db_operation, metrics,
    models::{Action, Task, TriggerCondition, TriggerKind, WebhookOutbox},
    workers::WorkerNudges,
};

/// Tunables for the delivery loop, resolved from config once at startup.
#[derive(Clone, Copy)]
pub struct DeliveryConfig {
    pub batch_size: i64,
    pub max_attempts: i32,
    pub backoff_base_secs: i64,
    pub backoff_cap_secs: i64,
    /// Lease (seconds) applied to a row at claim time: it stays "in-flight" (not
    /// re-claimable) until the lease expires. Must cover preparation, sequential
    /// actions for an event, and database acknowledgement latency.
    pub lease_secs: i64,
    /// Max admitted events, including results waiting for database acknowledgement.
    pub concurrency: usize,
    /// Freshness bound (seconds) for the start-before-end gate (Audit 2, A2). An
    /// `end`/`cancel` row is held back only while the task's `start` row is pending
    /// AND that start row was updated within the last `start_stale_secs`. A start row
    /// that never completes (crash between `mark_task_running` and the start-row
    /// completion, or a Claimed task canceled mid-webhook) eventually goes stale and
    /// stops blocking delivery. Mirror of the `webhook_idempotency_timeout` /
    /// `WORKER_CLAIM_TIMEOUT_SECS` staleness used by `try_claim_webhook_execution`.
    pub start_stale_secs: i64,
}

/// Continuously refill free delivery slots and persist ready results first.
pub async fn delivery_loop(
    evaluator: Arc<ActionExecutor>,
    pool: DbPool,
    interval: std::time::Duration,
    cfg: DeliveryConfig,
    mut shutdown: watch::Receiver<bool>,
    nudges: WorkerNudges,
) {
    const BACKLOG_SAMPLE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(15);
    let interval = interval.max(std::time::Duration::from_millis(1));
    let mut last_backlog_sample = Instant::now() - BACKLOG_SAMPLE_INTERVAL;
    let mut pump = DeliveryPump::default();
    let mut scan = true;
    let mut stopping = false;
    let mut retry_at = None;
    let mut poll_at = tokio::time::Instant::now() + interval;

    loop {
        stopping |= *shutdown.borrow() || shutdown.has_changed().is_err();
        pump.collect_ready();
        if stopping && pump.is_empty() {
            return;
        }

        let ready_to_retry = retry_at.is_none_or(|at| tokio::time::Instant::now() >= at);
        if ready_to_retry
            && (!pump.pending.is_empty() || (!stopping && scan && pump.free_slots(cfg) > 0))
        {
            let started = Instant::now();
            let sample = last_backlog_sample.elapsed() >= BACKLOG_SAMPLE_INTERVAL;
            let budget = if stopping || !scan { 0 } else { cfg.batch_size };
            match pump
                .advance(&evaluator, &pool, cfg, budget, Some(&shutdown), sample)
                .await
            {
                Ok(fill) => {
                    retry_at = None;
                    if budget > 0 {
                        scan = fill.filled;
                    }
                    if sample {
                        last_backlog_sample = Instant::now();
                    }
                }
                Err(e) => {
                    log::error!("Delivery worker: database step failed: {:?}", e);
                    stopping |= *shutdown.borrow() || shutdown.has_changed().is_err();
                    if stopping {
                        // Shutdown cannot wait indefinitely for DB recovery. The
                        // durable rows remain recoverable after their leases expire.
                        log::warn!(
                            "Delivery worker: leaving {} unrecorded results for lease recovery",
                            pump.pending.len()
                        );
                        pump.pending.clear();
                        retry_at = None;
                    } else {
                        // Keep ready outcomes, and stop admission, until their
                        // fenced marks can be written. No repeat HTTP is needed.
                        retry_at = Some(tokio::time::Instant::now() + interval);
                    }
                }
            }
            metrics::record_worker_loop_iteration("delivery", started.elapsed().as_secs_f64());
            poll_at = tokio::time::Instant::now() + interval;
            continue;
        }

        let wake_at = retry_at.map_or(poll_at, |retry| retry.min(poll_at));
        tokio::select! {
            biased;
            _ = shutdown.changed(), if !stopping => {
                stopping = true;
                retry_at = None;
            }
            Some(result) = pump.running.join_next(), if !pump.running.is_empty() => {
                pump.push_result(result);
                scan = true;
            }
            _ = nudges.delivery.notified(), if !stopping => { scan = true; }
            _ = rt::time::sleep_until(wake_at) => {
                scan = true;
                poll_at = tokio::time::Instant::now() + interval;
            }
        }
    }
}

/// Process at most `batch_size` due rows, recording each result as it completes.
/// The finite budget makes this helper deterministic for integration tests;
/// production uses the same pump continuously, without a barrier between batches.
pub async fn run_delivery_once(
    evaluator: &ActionExecutor,
    pool: &DbPool,
    cfg: DeliveryConfig,
) -> Result<usize, db_operation::DbError> {
    let mut pump = DeliveryPump::default();
    let mut remaining = cfg.batch_size.max(0);
    let mut processed = 0;
    loop {
        pump.collect_ready();
        if !pump.pending.is_empty() || (remaining > 0 && pump.free_slots(cfg) > 0) {
            let fill = pump
                .advance(evaluator, pool, cfg, remaining, None, false)
                .await?;
            processed += fill.claimed;
            remaining = if fill.filled {
                remaining - fill.claimed as i64
            } else {
                0
            };
            continue;
        }
        match pump.running.join_next().await {
            Some(result) => pump.push_result(result),
            None => return Ok(processed),
        }
    }
}

#[derive(Default)]
struct DeliveryPump {
    // JoinSet keeps HTTP progressing independently while the coordinator borrows
    // a connection. Completed jobs still occupy a slot until their mark is saved.
    running: tokio::task::JoinSet<DeliveryOutcome>,
    pending: std::collections::VecDeque<MarkAction>,
}

struct Fill {
    claimed: usize,
    filled: bool,
}

impl DeliveryPump {
    fn is_empty(&self) -> bool {
        self.running.is_empty() && self.pending.is_empty()
    }

    fn free_slots(&self, cfg: DeliveryConfig) -> usize {
        cfg.concurrency
            .max(1)
            .saturating_sub(self.running.len() + self.pending.len())
    }

    fn push_result(&mut self, result: Result<DeliveryOutcome, tokio::task::JoinError>) {
        match result {
            Ok(outcome) => self.pending.push_back(outcome.mark),
            Err(e) => log::error!(
                "Delivery worker: HTTP task failed; lease will recover it: {:?}",
                e
            ),
        }
    }

    fn collect_ready(&mut self) {
        while let Some(result) = self.running.try_join_next() {
            self.push_result(result);
        }
    }

    async fn flush(&mut self, conn: &mut Conn<'_>) -> Result<(), db_operation::DbError> {
        // A failed or ambiguous write keeps its outcome for retry. Every mark
        // checks the lease token, so an already-applied/stale mark is a safe no-op.
        self.collect_ready();
        while let Some(mark) = self.pending.front() {
            apply_mark(conn, mark).await?;
            self.pending.pop_front();
            self.collect_ready();
        }
        Ok(())
    }

    async fn advance(
        &mut self,
        evaluator: &ActionExecutor,
        pool: &DbPool,
        cfg: DeliveryConfig,
        budget: i64,
        shutdown: Option<&watch::Receiver<bool>>,
        sample_backlog: bool,
    ) -> Result<Fill, db_operation::DbError> {
        let mut conn = pool
            .get()
            .await
            .map_err(|e| db_operation::DbError::Pool(e.to_string()))?;
        // Include completions arriving during pool acquisition, and share this
        // checkout between their marks and the next bounded claim/prefetch.
        self.flush(&mut conn).await?;
        let stopping = shutdown.is_some_and(|rx| *rx.borrow() || rx.has_changed().is_err());
        let limit = if stopping {
            0
        } else {
            budget.min(cfg.batch_size).min(self.free_slots(cfg) as i64)
        };
        let mut claimed = 0;
        if limit > 0 {
            let rows = db_operation::run_in_transaction(&mut conn, |conn| {
                Box::pin(async move {
                    db_operation::claim_due_outbox_leased(
                        conn,
                        limit,
                        cfg.lease_secs,
                        cfg.start_stale_secs,
                    )
                    .await
                })
            })
            .await?;
            claimed = rows.len();
            if !rows.is_empty() {
                // If preparation fails, these durable claims recover by lease
                // expiry, as before. No HTTP has been sent for this group yet.
                for prepared in prepare_rows(&mut conn, rows).await? {
                    match prepared {
                        Prepared::Mark(mark) => self.pending.push_back(mark),
                        Prepared::Deliver(plan) => {
                            let evaluator = evaluator.clone();
                            self.running
                                .spawn(async move { deliver_plan(&evaluator, plan, cfg).await });
                        }
                    }
                }
            }
        }
        self.flush(&mut conn).await?;
        if sample_backlog {
            match db_operation::outbox_backlog_stats(&mut conn).await {
                Ok(stats) => metrics::set_webhook_outbox_backlog(
                    stats.ready,
                    stats.leased,
                    stats.oldest_ready_age_secs,
                ),
                Err(e) => log::warn!("Delivery worker: outbox backlog stats failed: {:?}", e),
            }
        }
        Ok(Fill {
            claimed,
            filled: limit > 0 && claimed as i64 == limit,
        })
        // conn drops here; never wait for an HTTP result holding a connection.
    }
}

/// Outcome of preparing a row for delivery: either an immediate mark (fast-path) or a
/// plan that requires HTTP delivery.
enum Prepared {
    Mark(MarkAction),
    Deliver(DeliveryPlan),
}

#[derive(diesel::QueryableByName)]
struct BatchStatsRow {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    batch_id: uuid::Uuid,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    success: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    failure: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    canceled: i64,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>)]
    completed_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// What to do with an outbox row's `status` after preparing/delivering it. Owns its
/// data so it can be applied without any borrow of the original row.
enum MarkAction {
    Success {
        key: String,
        lease_token: uuid::Uuid,
        lag: Option<f64>,
        label: &'static str,
    },
    Retry {
        key: String,
        lease_token: uuid::Uuid,
        error: String,
        backoff: i64,
        label: &'static str,
    },
    Exhausted {
        key: String,
        lease_token: uuid::Uuid,
        error: String,
        label: &'static str,
    },
}

/// Everything needed to deliver one outbox row's HTTP, owned (no DB borrow).
struct DeliveryPlan {
    key: String,
    lease_token: uuid::Uuid,
    trigger_label: &'static str,
    /// Prior failed-attempt count (for backoff/exhaustion decisions).
    attempts: i32,
    /// `created_at`, for the delivery-lag metric on success.
    created_at: chrono::DateTime<chrono::Utc>,
    kind: DeliveryKind,
}

/// Task-level vs batch-level delivery payload.
enum DeliveryKind {
    Task {
        task: Box<Task>,
        actions: Vec<Action>,
        enrichment: WebhookEnrichment,
    },
    Batch {
        actions: Vec<crate::dtos::NewActionDto>,
        enrichment: serde_json::Value,
    },
}

/// Result of delivering a plan: which mark to post.
struct DeliveryOutcome {
    mark: MarkAction,
}

/// Phase 2 helper: bulk-load all delivery inputs, then resolve each row's fast-path.
/// This keeps the DB work bounded to four queries per claimed page (tasks, actions,
/// batches, batch stats), instead of two queries per outbox row.
async fn prepare_rows<'a>(
    conn: &mut Conn<'a>,
    rows: Vec<WebhookOutbox>,
) -> Result<Vec<Prepared>, db_operation::DbError> {
    use crate::schema::action::dsl as action_dsl;
    use crate::schema::batch::dsl as batch_dsl;
    use crate::schema::task::dsl as task_dsl;

    let task_ids: Vec<uuid::Uuid> = rows
        .iter()
        .filter(|row| row.trigger != TriggerKind::BatchComplete)
        .filter_map(|row| row.task_id)
        .collect();
    let batch_ids: Vec<uuid::Uuid> = rows
        .iter()
        .filter(|row| row.trigger == TriggerKind::BatchComplete)
        .filter_map(|row| row.batch_id)
        .collect();

    let tasks = if task_ids.is_empty() {
        Vec::new()
    } else {
        task_dsl::task
            .filter(task_dsl::id.eq_any(&task_ids))
            .load::<Task>(conn)
            .await?
    };
    let actions = if task_ids.is_empty() {
        Vec::new()
    } else {
        action_dsl::action
            .filter(action_dsl::task_id.eq_any(&task_ids))
            .load::<Action>(conn)
            .await?
    };
    let batch_payloads: Vec<(uuid::Uuid, serde_json::Value)> = if batch_ids.is_empty() {
        Vec::new()
    } else {
        batch_dsl::batch
            .filter(batch_dsl::id.eq_any(&batch_ids))
            .select((batch_dsl::id, batch_dsl::on_complete))
            .load(conn)
            .await?
    };

    let batch_stats: Vec<BatchStatsRow> = if batch_ids.is_empty() {
        Vec::new()
    } else {
        diesel::sql_query(
            "SELECT batch_id,
                    COUNT(*) FILTER (WHERE status = 'success') AS success,
                    COUNT(*) FILTER (WHERE status = 'failure') AS failure,
                    COUNT(*) FILTER (WHERE status = 'canceled') AS canceled,
                    MAX(ended_at) AS completed_at
             FROM task
             WHERE batch_id = ANY($1)
             GROUP BY batch_id",
        )
        .bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(&batch_ids)
        .load(conn)
        .await?
    };

    let tasks_by_id: HashMap<_, _> = tasks.into_iter().map(|task| (task.id, task)).collect();
    let mut actions_by_task: HashMap<uuid::Uuid, Vec<Action>> = HashMap::new();
    for action in actions {
        actions_by_task
            .entry(action.task_id)
            .or_default()
            .push(action);
    }
    let payloads_by_batch: HashMap<_, _> = batch_payloads.into_iter().collect();
    let stats_by_batch: HashMap<_, _> = batch_stats
        .into_iter()
        .map(|stats| (stats.batch_id, stats))
        .collect();

    let mut prepared = Vec::with_capacity(rows.len());
    for row in rows {
        if row.trigger == TriggerKind::BatchComplete {
            let payload = row
                .batch_id
                .and_then(|id| payloads_by_batch.get(&id).cloned());
            let stats = row.batch_id.and_then(|id| stats_by_batch.get(&id));
            prepared.push(prepare_batch_complete_row(row, payload, stats)?);
        } else {
            let task = row.task_id.and_then(|id| tasks_by_id.get(&id).cloned());
            let actions = row
                .task_id
                .and_then(|id| actions_by_task.get(&id))
                .map(|actions| {
                    actions
                        .iter()
                        .filter(|action| {
                            action.trigger == row.trigger && action.condition == row.condition
                        })
                        .cloned()
                        .collect()
                })
                .unwrap_or_default();
            prepared.push(prepare_task_row(row, task, actions)?);
        }
    }
    Ok(prepared)
}

fn prepare_task_row(
    row: WebhookOutbox,
    task: Option<Task>,
    actions: Vec<Action>,
) -> Result<Prepared, db_operation::DbError> {
    let key = row.idempotency_key.clone();
    let lease_token = row.lease_token.ok_or_else(|| {
        crate::error::ArcRunError::Internal(format!(
            "claimed outbox row {} has no lease token",
            row.id
        ))
    })?;
    let label = trigger_label(row.trigger, row.condition);

    let Some(row_task_id) = row.task_id else {
        log::error!(
            "Delivery worker: task-level outbox key {} has NULL task_id; marking success",
            key
        );
        return Ok(Prepared::Mark(MarkAction::Success {
            key,
            lease_token,
            lag: None,
            label,
        }));
    };

    let Some(task) = task else {
        log::warn!(
            "Delivery worker: task {} for outbox key {} no longer exists; marking success",
            row_task_id,
            key
        );
        return Ok(Prepared::Mark(MarkAction::Success {
            key,
            lease_token,
            lag: None,
            label,
        }));
    };

    // No matching actions: nothing to deliver. Mark success immediately (documented
    // design choice — we enqueue unconditionally to keep the transition tx minimal).
    if actions.is_empty() {
        return Ok(Prepared::Mark(MarkAction::Success {
            key,
            lease_token,
            lag: Some(lag_secs(row.created_at)),
            label,
        }));
    }

    let enrichment = WebhookEnrichment {
        status: task.status,
        ended_at: task.ended_at,
        trigger: match row.trigger {
            TriggerKind::Cancel => "cancel".to_string(),
            _ => "end".to_string(),
        },
    };

    Ok(Prepared::Deliver(DeliveryPlan {
        key,
        lease_token,
        trigger_label: label,
        attempts: row.attempts,
        created_at: row.created_at,
        kind: DeliveryKind::Task {
            task: Box::new(task),
            actions,
            enrichment,
        },
    }))
}

fn prepare_batch_complete_row(
    row: WebhookOutbox,
    on_complete: Option<serde_json::Value>,
    stats: Option<&BatchStatsRow>,
) -> Result<Prepared, db_operation::DbError> {
    let key = row.idempotency_key.clone();
    let lease_token = row.lease_token.ok_or_else(|| {
        crate::error::ArcRunError::Internal(format!(
            "claimed outbox row {} has no lease token",
            row.id
        ))
    })?;
    let label = "batch_complete";

    let Some(batch_id) = row.batch_id else {
        log::error!(
            "Delivery worker: batch_complete outbox key {} has NULL batch_id; marking success",
            key
        );
        return Ok(Prepared::Mark(MarkAction::Success {
            key,
            lease_token,
            lag: None,
            label,
        }));
    };

    let Some(on_complete) = on_complete else {
        log::warn!(
            "Delivery worker: batch {} for outbox key {} no longer exists; marking success",
            batch_id,
            key
        );
        return Ok(Prepared::Mark(MarkAction::Success {
            key,
            lease_token,
            lag: None,
            label,
        }));
    };

    let actions: Vec<crate::dtos::NewActionDto> = match serde_json::from_value(on_complete) {
        Ok(a) => a,
        Err(e) => {
            // Malformed payload: this can never succeed, so exhaust immediately.
            // (The exhausted metric is emitted by apply_mark, the single emission point.)
            log::error!(
                "Delivery worker: batch {} on_complete payload is malformed: {}",
                batch_id,
                e
            );
            return Ok(Prepared::Mark(MarkAction::Exhausted {
                key,
                lease_token,
                error: format!("malformed payload: {}", e),
                label,
            }));
        }
    };

    if actions.is_empty() {
        return Ok(Prepared::Mark(MarkAction::Success {
            key,
            lease_token,
            lag: Some(lag_secs(row.created_at)),
            label,
        }));
    }

    // Compute counts + completed_at at delivery time.
    let completed_at = stats
        .and_then(|stats| stats.completed_at)
        .unwrap_or(row.updated_at);
    let enrichment = serde_json::json!({
        "batch_id": batch_id,
        "counts": {
            "success": stats.map_or(0, |stats| stats.success),
            "failure": stats.map_or(0, |stats| stats.failure),
            "canceled": stats.map_or(0, |stats| stats.canceled),
        },
        "completed_at": completed_at,
        "trigger": "batch_complete",
    });

    Ok(Prepared::Deliver(DeliveryPlan {
        key,
        lease_token,
        trigger_label: label,
        attempts: row.attempts,
        created_at: row.created_at,
        kind: DeliveryKind::Batch {
            actions,
            enrichment,
        },
    }))
}

/// Phase 3 helper: run a plan's HTTP (no DB) and decide the resulting mark.
async fn deliver_plan(
    evaluator: &ActionExecutor,
    plan: DeliveryPlan,
    cfg: DeliveryConfig,
) -> DeliveryOutcome {
    let DeliveryPlan {
        key,
        lease_token,
        trigger_label,
        attempts,
        created_at,
        kind,
    } = plan;

    // Track delivery-phase concurrency (separate from the start phase) so we can see
    // whether we plateau at WEBHOOK_DELIVERY_CONCURRENCY.
    let _in_flight = metrics::WebhooksInFlightGuard::new("delivery");

    let mut errors: Vec<String> = Vec::new();
    match kind {
        DeliveryKind::Task {
            task,
            actions,
            enrichment,
        } => {
            for act in actions.iter() {
                match evaluator
                    .execute_with_enrichment(act, &task, Some(&key), Some(&enrichment))
                    .await
                {
                    Ok(_) => log::debug!("Delivery worker: action {} delivered", act.id),
                    Err(e) => {
                        log::warn!("Delivery worker: action {} delivery failed: {}", act.id, e);
                        errors.push(e);
                    }
                }
            }
        }
        DeliveryKind::Batch {
            actions,
            enrichment,
        } => {
            for act in &actions {
                match evaluator
                    .execute_batch_action(act, Some(&key), enrichment.clone())
                    .await
                {
                    Ok(_) => log::debug!("Delivery worker: batch_complete action delivered"),
                    Err(e) => {
                        log::warn!("Delivery worker: batch_complete action failed: {}", e);
                        errors.push(e);
                    }
                }
            }
        }
    }

    if errors.is_empty() {
        return DeliveryOutcome {
            mark: MarkAction::Success {
                key,
                lease_token,
                lag: Some(lag_secs(created_at)),
                label: trigger_label,
            },
        };
    }

    // At least one action failed. Either reschedule with backoff or give up.
    let error_msg = errors.join("; ");
    // attempts is the count of prior failed attempts; this delivery makes it +1.
    let attempts_after = attempts + 1;
    if attempts_after >= cfg.max_attempts {
        log::error!(
            "Delivery worker: outbox key {} exhausted after {} attempts: {}",
            key,
            attempts_after,
            error_msg
        );
        DeliveryOutcome {
            mark: MarkAction::Exhausted {
                key,
                lease_token,
                error: error_msg,
                label: trigger_label,
            },
        }
    } else {
        let backoff = compute_backoff(attempts, cfg);
        DeliveryOutcome {
            mark: MarkAction::Retry {
                key,
                lease_token,
                error: error_msg,
                backoff,
                label: trigger_label,
            },
        }
    }
}

/// Apply a fenced mark without consuming it, so a transient DB failure can retry
/// the same outcome without repeating HTTP. Other marks remain independently committed.
async fn apply_mark(conn: &mut Conn<'_>, mark: &MarkAction) -> Result<(), db_operation::DbError> {
    let (result, mark_label) = match mark {
        MarkAction::Success {
            key,
            lease_token,
            lag,
            label,
        } => {
            let r = db_operation::mark_outbox_success(conn, key, *lease_token).await;
            if matches!(r, Ok(true)) {
                if let Some(lag) = lag {
                    metrics::record_webhook_delivery_lag(*lag);
                }
                metrics::record_webhook_delivery_success(label);
            }
            (r, "success")
        }
        MarkAction::Retry {
            key,
            lease_token,
            error,
            backoff,
            label,
        } => {
            let r = db_operation::mark_outbox_retry(conn, key, *lease_token, error, *backoff).await;
            if matches!(r, Ok(true)) {
                metrics::record_webhook_delivery_retry(label);
            }
            (r, "retry")
        }
        MarkAction::Exhausted {
            key,
            lease_token,
            error,
            label,
        } => {
            let r = db_operation::mark_outbox_exhausted(conn, key, *lease_token, error).await;
            if matches!(r, Ok(true)) {
                metrics::record_webhook_delivery_exhausted(label);
            }
            (r, "exhausted")
        }
    };

    let key = match mark {
        MarkAction::Success { key, .. }
        | MarkAction::Retry { key, .. }
        | MarkAction::Exhausted { key, .. } => key,
    };
    match &result {
        Err(e) => {
            // A DB failure leaves the leased row in place; it will mature and retry.
            metrics::record_webhook_mark_failure(mark_label);
            log::error!(
                "Delivery worker: failed to post outbox mark for key {} ({:?}); \
                 result retained for retry",
                key,
                e
            );
        }
        Ok(false) => log::debug!(
            "Delivery worker: ignored stale {} mark for key {} (lease was reclaimed)",
            mark_label,
            key
        ),
        Ok(true) => {}
    }
    result.map(|_| ())
}

/// Exponential backoff: `base^(prior_attempts + 1)` seconds, capped.
/// First failure (prior_attempts = 0) => base^1; second => base^2; etc.
fn compute_backoff(prior_attempts: i32, cfg: DeliveryConfig) -> i64 {
    let exp = (prior_attempts as u32).saturating_add(1);
    let mut delay = cfg.backoff_base_secs;
    for _ in 1..exp {
        delay = delay.saturating_mul(cfg.backoff_base_secs);
        if delay >= cfg.backoff_cap_secs {
            return cfg.backoff_cap_secs;
        }
    }
    delay.min(cfg.backoff_cap_secs)
}

fn lag_secs(created_at: chrono::DateTime<chrono::Utc>) -> f64 {
    (chrono::Utc::now() - created_at).num_milliseconds().max(0) as f64 / 1000.0
}

fn trigger_label(trigger: TriggerKind, condition: TriggerCondition) -> &'static str {
    match (trigger, condition) {
        (TriggerKind::End, TriggerCondition::Success) => "end_success",
        (TriggerKind::End, TriggerCondition::Failure) => "end_failure",
        (TriggerKind::Cancel, _) => "cancel",
        (TriggerKind::Start, _) => "start",
        (TriggerKind::BatchComplete, _) => "batch_complete",
    }
}
