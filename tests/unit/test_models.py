"""Tests for Aegis workflow engine."""

import pytest
from datetime import timedelta
from uuid import uuid4

from aegis import (
    workflow,
    activity,
    RetryPolicy,
    WorkflowStatus,
    TaskStatus,
    EventType,
)
from aegis.models import (
    WorkflowExecution,
    HistoryEvent,
    ActivityTask,
    ReplayContext,
)
from aegis.definition import WorkflowDefinition, ActivityDefinition


class TestRetryPolicy:
    """Tests for RetryPolicy."""

    def test_default_values(self) -> None:
        """Test default retry policy values."""
        policy = RetryPolicy()
        assert policy.max_attempts == 3
        assert policy.initial_interval == timedelta(seconds=1)
        assert policy.backoff_coefficient == 2.0
        assert policy.max_interval == timedelta(minutes=5)

    def test_next_retry_delay(self) -> None:
        """Test exponential backoff calculation."""
        policy = RetryPolicy(
            initial_interval=timedelta(seconds=1),
            backoff_coefficient=2.0,
            max_interval=timedelta(seconds=10),
        )

        # First retry: 1 * 2^0 = 1 second
        assert policy.next_retry_delay(0) == timedelta(seconds=1)

        # Second retry: 1 * 2^1 = 2 seconds
        assert policy.next_retry_delay(1) == timedelta(seconds=2)

        # Third retry: 1 * 2^2 = 4 seconds
        assert policy.next_retry_delay(2) == timedelta(seconds=4)

        # Fourth retry: 1 * 2^3 = 8 seconds
        assert policy.next_retry_delay(3) == timedelta(seconds=8)

        # Fifth retry: 1 * 2^4 = 16 seconds, capped at 10
        assert policy.next_retry_delay(4) == timedelta(seconds=10)


class TestWorkflowExecution:
    """Tests for WorkflowExecution model."""

    def test_create(self) -> None:
        """Test creating a workflow execution."""
        execution = WorkflowExecution.create(
            workflow_name="test_workflow",
            input_data={"key": "value"},
            correlation_id="test-123",
        )

        assert execution.workflow_name == "test_workflow"
        assert execution.input_data == {"key": "value"}
        assert execution.correlation_id == "test-123"
        assert execution.status == WorkflowStatus.PENDING
        assert execution.execution_id is not None
        assert execution.created_at is not None


class TestHistoryEvent:
    """Tests for HistoryEvent model."""

    def test_create(self) -> None:
        """Test creating a history event."""
        execution_id = uuid4()
        event = HistoryEvent.create(
            execution_id=execution_id,
            event_type=EventType.WORKFLOW_STARTED,
            event_data={"test": "data"},
            sequence_number=1,
        )

        assert event.execution_id == execution_id
        assert event.event_type == EventType.WORKFLOW_STARTED
        assert event.event_data == {"test": "data"}
        assert event.sequence_number == 1
        assert event.event_timestamp is not None


class TestActivityTask:
    """Tests for ActivityTask model."""

    def test_create(self) -> None:
        """Test creating an activity task."""
        execution_id = uuid4()
        task = ActivityTask.create(
            execution_id=execution_id,
            activity_name="test_activity",
            activity_input={"arg1": "value1"},
        )

        assert task.execution_id == execution_id
        assert task.activity_name == "test_activity"
        assert task.activity_input == {"arg1": "value1"}
        assert task.status == TaskStatus.PENDING
        assert task.max_attempts == 3  # Default from RetryPolicy


class TestReplayContext:
    """Tests for ReplayContext."""

    def test_activity_result_tracking(self) -> None:
        """Test tracking activity results."""
        ctx = ReplayContext()

        # Initially no results
        assert not ctx.has_activity_result("test_activity")
        assert ctx.get_activity_result("test_activity") is None

        # Add result
        ctx.activity_results["test_activity"] = {"value": 42}

        # Now has result
        assert ctx.has_activity_result("test_activity")
        assert ctx.get_activity_result("test_activity") == {"value": 42}


class TestDecorators:
    """Tests for workflow and activity decorators."""

    def test_workflow_decorator(self) -> None:
        """Test workflow decorator creates WorkflowDefinition."""

        @workflow
        async def my_workflow(x: int) -> int:
            return x * 2

        assert isinstance(my_workflow, WorkflowDefinition)
        assert my_workflow.name == "my_workflow"

    def test_workflow_decorator_with_name(self) -> None:
        """Test workflow decorator with custom name."""

        @workflow(name="custom_name")
        async def my_workflow(x: int) -> int:
            return x * 2

        assert isinstance(my_workflow, WorkflowDefinition)
        assert my_workflow.name == "custom_name"

    def test_activity_decorator(self) -> None:
        """Test activity decorator creates ActivityDefinition."""

        @activity
        async def my_activity(x: int) -> int:
            return x * 2

        assert isinstance(my_activity, ActivityDefinition)
        assert my_activity.name == "my_activity"

    def test_activity_decorator_with_retry_policy(self) -> None:
        """Test activity decorator with custom retry policy."""
        policy = RetryPolicy(max_attempts=5)

        @activity(retry_policy=policy)
        async def my_activity(x: int) -> int:
            return x * 2

        assert isinstance(my_activity, ActivityDefinition)
        assert my_activity.retry_policy.max_attempts == 5


class TestEnums:
    """Tests for enum values."""

    def test_workflow_status_values(self) -> None:
        """Test WorkflowStatus enum values."""
        assert WorkflowStatus.PENDING.value == "PENDING"
        assert WorkflowStatus.RUNNING.value == "RUNNING"
        assert WorkflowStatus.COMPLETED.value == "COMPLETED"
        assert WorkflowStatus.FAILED.value == "FAILED"
        assert WorkflowStatus.CANCELLED.value == "CANCELLED"
        assert WorkflowStatus.RECOVERING.value == "RECOVERING"

    def test_task_status_values(self) -> None:
        """Test TaskStatus enum values."""
        assert TaskStatus.PENDING.value == "PENDING"
        assert TaskStatus.RUNNING.value == "RUNNING"
        assert TaskStatus.COMPLETED.value == "COMPLETED"
        assert TaskStatus.FAILED.value == "FAILED"
        assert TaskStatus.RETRYING.value == "RETRYING"

    def test_event_type_values(self) -> None:
        """Test EventType enum values."""
        assert EventType.WORKFLOW_STARTED.value == "WorkflowStarted"
        assert EventType.WORKFLOW_COMPLETED.value == "WorkflowCompleted"
        assert EventType.ACTIVITY_SCHEDULED.value == "ActivityScheduled"
        assert EventType.ACTIVITY_COMPLETED.value == "ActivityCompleted"
