"""Data models for Aegis workflow engine."""

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any
from uuid import UUID, uuid4


def _utcnow() -> datetime:
    """Get current UTC datetime."""
    return datetime.now(timezone.utc)


class WorkflowStatus(str, Enum):
    """Workflow execution status."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    RECOVERING = "RECOVERING"


class TaskStatus(str, Enum):
    """Activity task status."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    RETRYING = "RETRYING"


class EventType(str, Enum):
    """History event types."""

    WORKFLOW_STARTED = "WorkflowStarted"
    WORKFLOW_COMPLETED = "WorkflowCompleted"
    WORKFLOW_FAILED = "WorkflowFailed"
    ACTIVITY_SCHEDULED = "ActivityScheduled"
    ACTIVITY_STARTED = "ActivityStarted"
    ACTIVITY_COMPLETED = "ActivityCompleted"
    ACTIVITY_FAILED = "ActivityFailed"
    WORKFLOW_DECISION = "WorkflowDecision"


@dataclass
class RetryPolicy:
    """Configuration for activity retry behavior."""

    max_attempts: int = 3
    initial_interval: timedelta = field(default_factory=lambda: timedelta(seconds=1))
    backoff_coefficient: float = 2.0
    max_interval: timedelta = field(default_factory=lambda: timedelta(minutes=5))

    def next_retry_delay(self, attempt: int) -> timedelta:
        """Calculate delay for next retry attempt."""
        delay_seconds = self.initial_interval.total_seconds() * (
            self.backoff_coefficient ** attempt
        )
        max_seconds = self.max_interval.total_seconds()
        return timedelta(seconds=min(delay_seconds, max_seconds))


@dataclass
class WorkflowExecution:
    """Represents a workflow execution instance."""

    execution_id: UUID
    workflow_name: str
    input_data: dict[str, Any]
    status: WorkflowStatus = WorkflowStatus.PENDING
    result: dict[str, Any] | None = None
    workflow_version: str = "1.0"
    current_step: str | None = None
    last_event_id: int | None = None
    parent_execution_id: UUID | None = None
    correlation_id: str | None = None
    created_at: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    attempt_count: int = 1
    max_attempts: int = 3
    last_failure_reason: str | None = None
    version: int = 1

    @classmethod
    def create(
        cls,
        workflow_name: str,
        input_data: dict[str, Any],
        correlation_id: str | None = None,
    ) -> "WorkflowExecution":
        """Create a new workflow execution."""
        return cls(
            execution_id=uuid4(),
            workflow_name=workflow_name,
            input_data=input_data,
            correlation_id=correlation_id,
            created_at=_utcnow(),
        )


@dataclass
class HistoryEvent:
    """Represents an event in workflow history."""

    event_id: int | None
    execution_id: UUID
    event_type: EventType
    event_data: dict[str, Any]
    sequence_number: int
    activity_name: str | None = None
    step_name: str | None = None
    event_timestamp: datetime | None = None
    created_by: str | None = None

    @classmethod
    def create(
        cls,
        execution_id: UUID,
        event_type: EventType,
        event_data: dict[str, Any],
        sequence_number: int,
        activity_name: str | None = None,
    ) -> "HistoryEvent":
        """Create a new history event."""
        return cls(
            event_id=None,
            execution_id=execution_id,
            event_type=event_type,
            event_data=event_data,
            sequence_number=sequence_number,
            activity_name=activity_name,
            event_timestamp=_utcnow(),
        )


@dataclass
class ActivityTask:
    """Represents an activity task in the queue."""

    task_id: UUID
    execution_id: UUID
    activity_name: str
    activity_input: dict[str, Any]
    status: TaskStatus = TaskStatus.PENDING
    result: dict[str, Any] | None = None
    error_message: str | None = None
    scheduled_at: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    attempt_count: int = 0
    max_attempts: int = 3
    next_retry_at: datetime | None = None
    backoff_coefficient: float = 2.0
    worker_id: str | None = None
    heartbeat_timeout: datetime | None = None
    priority: int = 0
    idempotency_key: str | None = None

    @classmethod
    def create(
        cls,
        execution_id: UUID,
        activity_name: str,
        activity_input: dict[str, Any],
        retry_policy: RetryPolicy | None = None,
        idempotency_key: str | None = None,
    ) -> "ActivityTask":
        """Create a new activity task."""
        policy = retry_policy or RetryPolicy()
        return cls(
            task_id=uuid4(),
            execution_id=execution_id,
            activity_name=activity_name,
            activity_input=activity_input,
            max_attempts=policy.max_attempts,
            backoff_coefficient=policy.backoff_coefficient,
            scheduled_at=_utcnow(),
            idempotency_key=idempotency_key,
        )


@dataclass
class ReplayContext:
    """Context for replaying workflow execution."""

    activity_results: dict[str, Any] = field(default_factory=dict)
    decisions: dict[str, str] = field(default_factory=dict)
    next_sequence: int = 1
    resume_from: int | None = None

    def get_activity_result(self, activity_name: str) -> Any | None:
        """Get recorded result for an activity."""
        return self.activity_results.get(activity_name)

    def has_activity_result(self, activity_name: str) -> bool:
        """Check if activity result exists."""
        return activity_name in self.activity_results
