// Single integration test binary.
// All test modules share one PostgreSQL container via LazyLock,
// with per-test isolation via CREATE DATABASE ... TEMPLATE.

#[macro_use]
mod common;

mod test_actions;
mod test_batch_complete;
mod test_batch_remaining;
mod test_batch_rules;
mod test_batch_scope;
mod test_batch_stats;
mod test_batch_update;
mod test_bug_audit1;
mod test_bug_audit10;
mod test_bug_audit2;
mod test_bug_audit3;
mod test_bug_audit4;
mod test_bug_audit5;
mod test_bug_audit6;
mod test_bug_audit7;
mod test_bug_audit8;
mod test_bug_audit9;
mod test_cancel_webhook;
mod test_capacity_slot;
mod test_claim_loop;
mod test_concurrency;
mod test_crud;
mod test_dag;
mod test_dead_end_cancel;
mod test_dedupe;
mod test_delivery_lease;
mod test_edge_cases;
mod test_filtering;
mod test_health;
mod test_idempotency;
mod test_outbox;
mod test_parallel_webhooks;
mod test_priority;
mod test_propagation;
mod test_propagation_edge;
mod test_regressions;
mod test_requeue_stale;
mod test_retention_archive;
mod test_rule_slot;
mod test_status;
mod test_stop_batch;
mod test_validation_e2e;
mod test_webhook_flows;
