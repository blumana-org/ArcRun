use crate::{DbPool, db_operation, metrics, workers::WorkerNudges};
use actix_web::rt;
use tokio::sync::watch;

/// Background loop that detects timed-out Running tasks and failed stale claims.
///
/// For each timed-out task, the mark-failed + child propagation + outbox enqueue
/// runs inside a single transaction so a crash between them cannot leave children
/// stuck in Waiting or lose the on_failure notification. Webhooks themselves are
/// delivered async by the delivery loop (Lot 2).
/// Safety cap on the number of timeout-drain passes per loop iteration. Bounds the
/// worst-case time the timeout phase can run before the loop returns to its tick
/// (and re-runs the stale-Claimed requeue). With the default batch size of 100
/// this drains up to 5000 timed-out tasks per iteration; anything beyond that is
/// picked up on the next tick.
const MAX_TIMEOUT_DRAIN_PASSES: usize = 50;

pub async fn timeout_loop(
    pool: DbPool,
    interval: std::time::Duration,
    claim_timeout: std::time::Duration,
    dead_end_enabled: bool,
    timeout_batch_size: i64,
    mut shutdown: watch::Receiver<bool>,
    nudges: WorkerNudges,
) {
    loop {
        let loop_start = std::time::Instant::now();
        // note that obtaining a connection from the pool is also potentially blocking
        let conn = pool.get();

        if let Ok(mut conn) = conn.await {
            // --- Requeue stale Claimed tasks ---
            let requeued =
                db_operation::requeue_stale_claimed_tasks(&mut conn, claim_timeout).await;
            match requeued {
                Ok(ids) => {
                    if !ids.is_empty() {
                        for _ in &ids {
                            metrics::record_status_transition("Claimed", "Pending");
                        }
                        log::warn!(
                            "Timeout worker: requeued {} stale claimed tasks, {:?}",
                            ids.len(),
                            ids
                        );
                    } else {
                        log::debug!("Timeout worker: no stale claimed tasks");
                    }
                }
                Err(e) => {
                    log::error!("Timeout worker: error requeuing claimed tasks: {:?}", e);
                }
            }

            // --- Timeout Running tasks (bounded drain, Audit 2, B7) ---
            // Each pass fetches at most `timeout_batch_size` timed-out ids and marks
            // them failed one-tx-per-task. If a pass returns a FULL batch there may be
            // more, so we re-fetch immediately (drain) instead of waiting a full tick —
            // but capped at MAX_TIMEOUT_DRAIN_PASSES so a mass-timeout can never pin the
            // loop and starve the stale-Claimed requeue (which already ran above, once,
            // this iteration). A shorter-than-full batch means the backlog is drained.
            let mut enqueued_outbox = false;
            for pass in 0..MAX_TIMEOUT_DRAIN_PASSES {
                // Step 1: find timed-out task IDs (read-only, no lock), bounded.
                let timed_out_ids =
                    match db_operation::find_timed_out_tasks(&mut conn, timeout_batch_size).await {
                        Ok(ids) => ids,
                        Err(e) => {
                            log::error!("Timeout worker: error finding timed-out tasks: {:?}", e);
                            break;
                        }
                    };

                if timed_out_ids.is_empty() {
                    if pass == 0 {
                        log::debug!("Timeout worker: no tasks timed out");
                    }
                    break;
                }

                let batch_len = timed_out_ids.len() as i64;
                log::warn!(
                    "Timeout worker: {} tasks timed out (pass {}), {:?}",
                    batch_len,
                    pass,
                    timed_out_ids
                );

                // Step 2: for each, atomically mark failed + propagate (in tx).
                for task_id in timed_out_ids {
                    let result = db_operation::timeout_task_and_propagate(
                        &mut conn,
                        task_id,
                        dead_end_enabled,
                    )
                    .await;

                    match result {
                        Ok(Some((_failed_task, _cascade_failed, _canceled_ancestors))) => {
                            metrics::record_task_timeout();
                            metrics::record_status_transition("Running", "Failure");
                            // on_failure notifications (task + cascade + dead-end
                            // ancestors) were enqueued into the outbox inside the
                            // transaction; the delivery loop sends them async.
                            enqueued_outbox = true;
                        }
                        Ok(None) => {
                            // Task already transitioned concurrently, nothing to do
                            log::debug!(
                                "Timeout worker: task {} already transitioned, skipping",
                                task_id
                            );
                        }
                        Err(e) => {
                            log::error!(
                                "Timeout worker: failed to timeout task {}: {:?}",
                                task_id,
                                e
                            );
                        }
                    }
                }

                // Fewer than a full batch => backlog drained, stop early.
                if batch_len < timeout_batch_size {
                    break;
                }
                if pass + 1 == MAX_TIMEOUT_DRAIN_PASSES {
                    log::warn!(
                        "Timeout worker: hit max drain passes ({}); remaining timed-out \
                         tasks will be handled next tick",
                        MAX_TIMEOUT_DRAIN_PASSES
                    );
                }
            }

            // B4: wake the delivery loop for the on_failure outbox rows just enqueued,
            // rather than letting them wait a full delivery tick. Best-effort — the
            // delivery poll is the fallback.
            if enqueued_outbox {
                nudges.nudge_delivery();
            }
        }
        metrics::record_worker_loop_iteration("timeout", loop_start.elapsed().as_secs_f64());
        tokio::select! {
            _ = shutdown.changed() => {
                log::info!("Timeout worker: shutdown signal received, exiting");
                return;
            }
            _ = rt::time::sleep(interval) => {}
        }
    }
}
