// @generated automatically by Diesel CLI.

pub mod sql_types {
    #[derive(diesel::query_builder::QueryId, Clone, diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "action_kind"))]
    pub struct ActionKind;

    #[derive(diesel::query_builder::QueryId, Clone, diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "status_kind"))]
    pub struct StatusKind;

    #[derive(diesel::query_builder::QueryId, Clone, diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "trigger_condition"))]
    pub struct TriggerCondition;

    #[derive(diesel::query_builder::QueryId, Clone, diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "trigger_kind"))]
    pub struct TriggerKind;

    #[derive(diesel::query_builder::QueryId, Clone, diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "webhook_execution_status"))]
    pub struct WebhookExecutionStatus;
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::ActionKind;
    use super::sql_types::TriggerKind;
    use super::sql_types::TriggerCondition;

    action (id) {
        id -> Uuid,
        task_id -> Uuid,
        kind -> ActionKind,
        params -> Jsonb,
        trigger -> TriggerKind,
        condition -> TriggerCondition,
        success -> Nullable<Bool>,
    }
}

diesel::table! {
    batch (id) {
        id -> Uuid,
        on_complete -> Jsonb,
        created_at -> Timestamptz,
        scope -> Nullable<Text>,
        metadata -> Jsonb,
        remaining -> Int4,
    }
}

diesel::table! {
    batch_catalog (batch_id) {
        batch_id -> Uuid,
        first_created_at -> Timestamptz,
        last_created_at -> Timestamptz,
    }
}

diesel::table! {
    link (parent_id, child_id) {
        parent_id -> Uuid,
        child_id -> Uuid,
        requires_success -> Bool,
    }
}

diesel::table! {
    rule_slot (lock_key) {
        lock_key -> Text,
        used -> Int4,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::StatusKind;

    task (id) {
        id -> Uuid,
        name -> Text,
        kind -> Text,
        status -> StatusKind,
        timeout -> Int4,
        created_at -> Timestamptz,
        started_at -> Nullable<Timestamptz>,
        last_updated -> Timestamptz,
        metadata -> Jsonb,
        ended_at -> Nullable<Timestamptz>,
        start_condition -> Jsonb,
        wait_success -> Int4,
        wait_finished -> Int4,
        success -> Int4,
        failures -> Int4,
        failure_reason -> Nullable<Text>,
        batch_id -> Nullable<Uuid>,
        expected_count -> Nullable<Int4>,
        dead_end_barrier -> Bool,
        priority -> Int4,
        claimed_slot_keys -> Nullable<Array<Text>>,
        capacity_charge -> Nullable<Int4>,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::StatusKind;

    task_archive (id) {
        id -> Uuid,
        name -> Text,
        kind -> Text,
        status -> StatusKind,
        timeout -> Int4,
        created_at -> Timestamptz,
        started_at -> Nullable<Timestamptz>,
        last_updated -> Timestamptz,
        metadata -> Jsonb,
        ended_at -> Nullable<Timestamptz>,
        start_condition -> Jsonb,
        wait_success -> Int4,
        wait_finished -> Int4,
        success -> Int4,
        failures -> Int4,
        failure_reason -> Nullable<Text>,
        batch_id -> Nullable<Uuid>,
        expected_count -> Nullable<Int4>,
        dead_end_barrier -> Bool,
        priority -> Int4,
        claimed_slot_keys -> Nullable<Array<Text>>,
        capacity_charge -> Nullable<Int4>,
        archived_at -> Timestamptz,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::TriggerKind;
    use super::sql_types::TriggerCondition;
    use super::sql_types::WebhookExecutionStatus;

    webhook_execution (id) {
        id -> Uuid,
        task_id -> Nullable<Uuid>,
        trigger -> TriggerKind,
        condition -> TriggerCondition,
        idempotency_key -> Text,
        status -> WebhookExecutionStatus,
        attempts -> Int4,
        created_at -> Timestamptz,
        updated_at -> Timestamptz,
        next_attempt_at -> Timestamptz,
        last_error -> Nullable<Text>,
        batch_id -> Nullable<Uuid>,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::TriggerKind;
    use super::sql_types::TriggerCondition;

    webhook_outbox (id) {
        id -> Uuid,
        task_id -> Nullable<Uuid>,
        batch_id -> Nullable<Uuid>,
        trigger -> TriggerKind,
        condition -> TriggerCondition,
        idempotency_key -> Text,
        attempts -> Int4,
        created_at -> Timestamptz,
        updated_at -> Timestamptz,
        next_attempt_at -> Timestamptz,
        last_error -> Nullable<Text>,
        lease_token -> Nullable<Uuid>,
    }
}

diesel::joinable!(action -> task (task_id));
diesel::joinable!(webhook_execution -> task (task_id));
diesel::joinable!(webhook_execution -> batch (batch_id));
diesel::joinable!(webhook_outbox -> task (task_id));
diesel::joinable!(webhook_outbox -> batch (batch_id));

diesel::allow_tables_to_appear_in_same_query!(
    action,
    batch,
    batch_catalog,
    link,
    rule_slot,
    task,
    task_archive,
    webhook_execution,
    webhook_outbox,
);
