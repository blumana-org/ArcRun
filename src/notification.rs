//! Valid task notifications at the boundary between lifecycle changes and the outbox.

use crate::{
    error::ArcRunError,
    models::{StatusKind, TriggerCondition, TriggerKind},
};

/// A task status proven to be terminal. Construct it through `TryFrom` or the constants.
///
/// ```compile_fail
/// use arcrun::{models::StatusKind, notification::TerminalStatus};
/// let invalid = TerminalStatus(StatusKind::Pending);
/// ```
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TerminalStatus(StatusKind);

impl TerminalStatus {
    pub const SUCCESS: Self = Self(StatusKind::Success);
    pub const FAILURE: Self = Self(StatusKind::Failure);
    pub const CANCELED: Self = Self(StatusKind::Canceled);

    pub fn end_condition(self) -> TriggerCondition {
        if self == Self::SUCCESS {
            TriggerCondition::Success
        } else {
            TriggerCondition::Failure
        }
    }
}

impl TryFrom<StatusKind> for TerminalStatus {
    type Error = ArcRunError;

    fn try_from(status: StatusKind) -> Result<Self, Self::Error> {
        match status {
            StatusKind::Success => Ok(Self::SUCCESS),
            StatusKind::Failure => Ok(Self::FAILURE),
            StatusKind::Canceled => Ok(Self::CANCELED),
            _ => Err(ArcRunError::InvalidState {
                message: format!(
                    "Cannot create an end notification for non-terminal status {status:?}"
                ),
            }),
        }
    }
}

/// Task events supported by the durable notification queue.
/// Start requests and batch completion use separate paths.
#[derive(Debug, Clone, Copy)]
pub enum TaskNotification {
    End(TerminalStatus),
    Cancel,
}

impl TaskNotification {
    pub fn trigger(self) -> TriggerKind {
        match self {
            Self::End(_) => TriggerKind::End,
            Self::Cancel => TriggerKind::Cancel,
        }
    }

    pub fn condition(self) -> TriggerCondition {
        match self {
            Self::End(status) => status.end_condition(),
            Self::Cancel => TriggerCondition::Success,
        }
    }

    pub fn idempotency_key(self, task_id: uuid::Uuid) -> String {
        crate::action::idempotency_key(task_id, &self.trigger(), &self.condition())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_every_non_terminal_status() {
        for status in [
            StatusKind::Pending,
            StatusKind::Waiting,
            StatusKind::Claimed,
            StatusKind::Running,
            StatusKind::Paused,
        ] {
            assert!(TerminalStatus::try_from(status).is_err(), "{status:?}");
        }
    }

    #[test]
    fn terminal_notifications_preserve_event_identity() {
        let id = uuid::Uuid::nil();
        for (status, condition, suffix) in [
            (StatusKind::Success, TriggerCondition::Success, "success"),
            (StatusKind::Failure, TriggerCondition::Failure, "failure"),
            (StatusKind::Canceled, TriggerCondition::Failure, "failure"),
        ] {
            let event = TaskNotification::End(status.try_into().unwrap());
            assert_eq!(event.trigger(), TriggerKind::End);
            assert_eq!(event.condition(), condition);
            assert_eq!(event.idempotency_key(id), format!("{id}:end:{suffix}"));
        }
        let cancel = TaskNotification::Cancel;
        assert_eq!(cancel.trigger(), TriggerKind::Cancel);
        assert_eq!(cancel.condition(), TriggerCondition::Success);
        assert_eq!(cancel.idempotency_key(id), format!("{id}:cancel"));
    }
}
