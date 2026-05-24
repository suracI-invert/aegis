"""Integration tests for Database operations."""

import pytest
from datetime import timedelta
from uuid import uuid4

from aegis.database import Database
from aegis.models import (
    ActivityTask,
    EventType,
    HistoryEvent,
    RetryPolicy,
    TaskStatus,
    WorkflowExecution,
    WorkflowStatus,
)

pytestmark = pytest.mark.integration


class TestDatabaseConnection:
    """Tests for database connection lifecycle."""

    @pytest.mark.asyncio
    async def test_connect_and_close(self, database: Database) -> None:
        """Test database connects and closes properly."""
        assert database is not None
        assert database._engine is not None

    @pytest.mark.asyncio
    async def test_init_schema(self, database: Database) -> None:
        """Test schema initialization."""
        # Schema should already be initialized by fixture
        # Just verify we can query
        execution_id = uuid4()
        result = await database.get_execution(execution_id)
        assert result is None  # Not found, but no error


class TestWorkflowExecutionOperations:
    """Tests for workflow execution CRUD operations."""

    @pytest.mark.asyncio
    async def test_create_and_get_execution(self, database: Database) -> None:
        """Test creating and retrieving workflow execution."""
        execution = WorkflowExecution.create(
            workflow_name="test_workflow",
            input_data={"key": "value"},
            correlation_id="test-123",
        )

        await database.create_execution(execution)

        # Retrieve
        retrieved = await database.get_execution(execution.execution_id)

        assert retrieved is not None
        assert retrieved.execution_id == execution.execution_id
        assert retrieved.workflow_name == "test_workflow"
        assert retrieved.input_data == {"key": "value"}
        assert retrieved.correlation_id == "test-123"
        assert retrieved.status == WorkflowStatus.PENDING

    @pytest.mark.asyncio
    async def test_update_execution_status(self, database: Database) -> None:
        """Test updating workflow execution status."""
        execution = WorkflowExecution.create(
            workflow_name="test_workflow",
            input_data={},
        )
        await database.create_execution(execution)

        # Update to RUNNING
        success = await database.update_execution_status(
            execution.execution_id,
            WorkflowStatus.RUNNING,
        )
        assert success is True

        retrieved = await database.get_execution(execution.execution_id)
        assert retrieved is not None
        assert retrieved.status == WorkflowStatus.RUNNING

    @pytest.mark.asyncio
    async def test_update_execution_status_with_result(self, database: Database) -> None:
        """Test updating workflow execution status with result."""
        execution = WorkflowExecution.create(
            workflow_name="test_workflow",
            input_data={},
        )
        await database.create_execution(execution)

        # Complete with result
        success = await database.update_execution_status(
            execution.execution_id,
            WorkflowStatus.COMPLETED,
            result={"value": 42},
        )
        assert success is True

        retrieved = await database.get_execution(execution.execution_id)
        assert retrieved is not None
        assert retrieved.status == WorkflowStatus.COMPLETED
        assert retrieved.result == {"value": 42}
        assert retrieved.completed_at is not None

    @pytest.mark.asyncio
    async def test_update_execution_status_with_error(self, database: Database) -> None:
        """Test updating workflow execution status with error."""
        execution = WorkflowExecution.create(
            workflow_name="test_workflow",
            input_data={},
        )
        await database.create_execution(execution)

        # Fail with error
        success = await database.update_execution_status(
            execution.execution_id,
            WorkflowStatus.FAILED,
            error="Something went wrong",
        )
        assert success is True

        retrieved = await database.get_execution(execution.execution_id)
        assert retrieved is not None
        assert retrieved.status == WorkflowStatus.FAILED
        assert retrieved.last_failure_reason == "Something went wrong"

    @pytest.mark.asyncio
    async def test_optimistic_locking(self, database: Database) -> None:
        """Test optimistic locking with expected_version."""
        execution = WorkflowExecution.create(
            workflow_name="test_workflow",
            input_data={},
        )
        await database.create_execution(execution)

        # First update with correct version
        success = await database.update_execution_status(
            execution.execution_id,
            WorkflowStatus.RUNNING,
            expected_version=1,
        )
        assert success is True

        # Second update with wrong version should fail
        success = await database.update_execution_status(
            execution.execution_id,
            WorkflowStatus.COMPLETED,
            expected_version=1,  # Should be 2 now
        )
        assert success is False

    @pytest.mark.asyncio
    async def test_get_incomplete_executions(self, database: Database) -> None:
        """Test retrieving incomplete workflow executions."""
        # Create multiple executions
        pending = WorkflowExecution.create(
            workflow_name="test_workflow",
            input_data={},
        )
        await database.create_execution(pending)

        running = WorkflowExecution.create(
            workflow_name="test_workflow",
            input_data={},
        )
        await database.create_execution(running)
        await database.update_execution_status(running.execution_id, WorkflowStatus.RUNNING)

        completed = WorkflowExecution.create(
            workflow_name="test_workflow",
            input_data={},
        )
        await database.create_execution(completed)
        await database.update_execution_status(completed.execution_id, WorkflowStatus.COMPLETED)

        # Get incomplete
        incomplete = await database.get_incomplete_executions(("test_workflow",))

        incomplete_ids = {e.execution_id for e in incomplete}
        assert pending.execution_id in incomplete_ids
        assert running.execution_id in incomplete_ids
        assert completed.execution_id not in incomplete_ids


class TestWorkflowLocking:
    """Tests for distributed workflow locking."""

    @pytest.mark.asyncio
    async def test_try_lock_workflow(self, database: Database) -> None:
        """Test acquiring workflow lock."""
        execution = WorkflowExecution.create(
            workflow_name="test_workflow",
            input_data={},
        )
        await database.create_execution(execution)

        # Acquire lock
        locked = await database.try_lock_workflow(
            execution.execution_id,
            "worker-1",
            lock_duration=timedelta(minutes=5),
        )
        assert locked is True

    @pytest.mark.asyncio
    async def test_lock_prevents_double_lock(self, database: Database) -> None:
        """Test that locked workflow cannot be locked by another worker."""
        execution = WorkflowExecution.create(
            workflow_name="test_workflow",
            input_data={},
        )
        await database.create_execution(execution)

        # First worker acquires lock
        locked1 = await database.try_lock_workflow(
            execution.execution_id,
            "worker-1",
            lock_duration=timedelta(minutes=5),
        )
        assert locked1 is True

        # Second worker cannot acquire
        locked2 = await database.try_lock_workflow(
            execution.execution_id,
            "worker-2",
            lock_duration=timedelta(minutes=5),
        )
        assert locked2 is False

    @pytest.mark.asyncio
    async def test_release_workflow_lock(self, database: Database) -> None:
        """Test releasing workflow lock."""
        execution = WorkflowExecution.create(
            workflow_name="test_workflow",
            input_data={},
        )
        await database.create_execution(execution)

        # Acquire lock
        await database.try_lock_workflow(execution.execution_id, "worker-1")

        # Release lock
        released = await database.release_workflow_lock(
            execution.execution_id,
            "worker-1",
        )
        assert released is True

        # Now another worker can acquire
        locked = await database.try_lock_workflow(execution.execution_id, "worker-2")
        assert locked is True

    @pytest.mark.asyncio
    async def test_release_lock_wrong_worker(self, database: Database) -> None:
        """Test that wrong worker cannot release lock."""
        execution = WorkflowExecution.create(
            workflow_name="test_workflow",
            input_data={},
        )
        await database.create_execution(execution)

        await database.try_lock_workflow(execution.execution_id, "worker-1")

        # Wrong worker cannot release
        released = await database.release_workflow_lock(
            execution.execution_id,
            "worker-2",
        )
        assert released is False

    @pytest.mark.asyncio
    async def test_extend_workflow_lock(self, database: Database) -> None:
        """Test extending workflow lock (heartbeat)."""
        execution = WorkflowExecution.create(
            workflow_name="test_workflow",
            input_data={},
        )
        await database.create_execution(execution)

        await database.try_lock_workflow(execution.execution_id, "worker-1")

        # Extend lock
        extended = await database.extend_workflow_lock(
            execution.execution_id,
            "worker-1",
            lock_duration=timedelta(minutes=10),
        )
        assert extended is True


class TestHistoryEventOperations:
    """Tests for history event operations."""

    @pytest.mark.asyncio
    async def test_append_and_get_events(self, database: Database) -> None:
        """Test appending and retrieving events."""
        execution = WorkflowExecution.create(
            workflow_name="test_workflow",
            input_data={},
        )
        await database.create_execution(execution)

        # Append events
        event1 = HistoryEvent.create(
            execution_id=execution.execution_id,
            event_type=EventType.WORKFLOW_STARTED,
            event_data={"input": {}},
            sequence_number=1,
        )
        await database.append_event(event1)

        event2 = HistoryEvent.create(
            execution_id=execution.execution_id,
            event_type=EventType.ACTIVITY_SCHEDULED,
            event_data={"args": [1]},
            sequence_number=2,
            activity_name="test_activity",
        )
        await database.append_event(event2)

        # Get events
        events = await database.get_events(execution.execution_id)

        assert len(events) == 2
        assert events[0].event_type == EventType.WORKFLOW_STARTED
        assert events[1].event_type == EventType.ACTIVITY_SCHEDULED
        assert events[1].activity_name == "test_activity"

    @pytest.mark.asyncio
    async def test_get_next_sequence_number(self, database: Database) -> None:
        """Test getting next sequence number."""
        execution = WorkflowExecution.create(
            workflow_name="test_workflow",
            input_data={},
        )
        await database.create_execution(execution)

        # Initially 1
        seq1 = await database.get_next_sequence_number(execution.execution_id)
        assert seq1 == 1

        # Append event
        event = HistoryEvent.create(
            execution_id=execution.execution_id,
            event_type=EventType.WORKFLOW_STARTED,
            event_data={},
            sequence_number=1,
        )
        await database.append_event(event)

        # Now 2
        seq2 = await database.get_next_sequence_number(execution.execution_id)
        assert seq2 == 2


class TestActivityTaskOperations:
    """Tests for activity task queue operations."""

    @pytest.mark.asyncio
    async def test_enqueue_and_dequeue_task(self, database: Database) -> None:
        """Test enqueueing and dequeueing activity task."""
        execution = WorkflowExecution.create(
            workflow_name="test_workflow",
            input_data={},
        )
        await database.create_execution(execution)

        # Enqueue task
        task = ActivityTask.create(
            execution_id=execution.execution_id,
            activity_name="test_activity",
            activity_input={"x": 1},
            idempotency_key=f"{execution.execution_id}:test_activity:1",
        )
        await database.enqueue_task(task)

        # Dequeue task
        tasks = await database.dequeue_task(
            "worker-1",
            ("test_activity",),
            prefetch=1,
        )

        assert tasks is not None
        assert len(tasks) == 1
        assert tasks[0].task_id == task.task_id
        assert tasks[0].status == TaskStatus.RUNNING
        assert tasks[0].worker_id == "worker-1"
        assert tasks[0].attempt_count == 1

    @pytest.mark.asyncio
    async def test_complete_task(self, database: Database) -> None:
        """Test completing activity task."""
        execution = WorkflowExecution.create(
            workflow_name="test_workflow",
            input_data={},
        )
        await database.create_execution(execution)

        task = ActivityTask.create(
            execution_id=execution.execution_id,
            activity_name="test_activity",
            activity_input={},
            idempotency_key=f"{execution.execution_id}:test_activity:1",
        )
        await database.enqueue_task(task)

        # Complete
        await database.complete_task(task.task_id, {"value": 42})

        # Verify
        retrieved = await database.get_task(task.task_id)
        assert retrieved is not None
        assert retrieved.status == TaskStatus.COMPLETED
        assert retrieved.result == {"value": 42}
        assert retrieved.completed_at is not None

    @pytest.mark.asyncio
    async def test_fail_task_with_retry(self, database: Database) -> None:
        """Test failing task with retry scheduled."""
        execution = WorkflowExecution.create(
            workflow_name="test_workflow",
            input_data={},
        )
        await database.create_execution(execution)

        task = ActivityTask.create(
            execution_id=execution.execution_id,
            activity_name="test_activity",
            activity_input={},
            idempotency_key=f"{execution.execution_id}:test_activity:1",
        )
        await database.enqueue_task(task)

        # Fail with retry
        from datetime import datetime, timezone
        retry_at = datetime.now(timezone.utc) + timedelta(seconds=5)
        await database.fail_task(
            task.task_id,
            "Temporary error",
            retry=True,
            next_retry_at=retry_at,
        )

        # Verify
        retrieved = await database.get_task(task.task_id)
        assert retrieved is not None
        assert retrieved.status == TaskStatus.RETRYING
        assert retrieved.error_message == "Temporary error"
        assert retrieved.next_retry_at is not None

    @pytest.mark.asyncio
    async def test_fail_task_no_retry(self, database: Database) -> None:
        """Test failing task without retry."""
        execution = WorkflowExecution.create(
            workflow_name="test_workflow",
            input_data={},
        )
        await database.create_execution(execution)

        task = ActivityTask.create(
            execution_id=execution.execution_id,
            activity_name="test_activity",
            activity_input={},
            idempotency_key=f"{execution.execution_id}:test_activity:1",
        )
        await database.enqueue_task(task)

        await database.fail_task(task.task_id, "Fatal error", retry=False)

        retrieved = await database.get_task(task.task_id)
        assert retrieved is not None
        assert retrieved.status == TaskStatus.FAILED

    @pytest.mark.asyncio
    async def test_idempotency_key_prevents_duplicate(self, database: Database) -> None:
        """Test idempotency key prevents duplicate task creation."""
        execution = WorkflowExecution.create(
            workflow_name="test_workflow",
            input_data={},
        )
        await database.create_execution(execution)

        idempotency_key = f"{execution.execution_id}:test_activity:1"

        # First task
        task1 = ActivityTask.create(
            execution_id=execution.execution_id,
            activity_name="test_activity",
            activity_input={"x": 1},
            idempotency_key=idempotency_key,
        )
        await database.enqueue_task(task1)

        # Second task with same idempotency key
        task2 = ActivityTask.create(
            execution_id=execution.execution_id,
            activity_name="test_activity",
            activity_input={"x": 2},
            idempotency_key=idempotency_key,
        )
        await database.enqueue_task(task2)  # Should be ignored

        # Only first task exists
        retrieved = await database.get_task(task1.task_id)
        assert retrieved is not None
        assert retrieved.activity_input == {"x": 1}

    @pytest.mark.asyncio
    async def test_heartbeat_task(self, database: Database) -> None:
        """Test heartbeat extends task timeout."""
        execution = WorkflowExecution.create(
            workflow_name="test_workflow",
            input_data={},
        )
        await database.create_execution(execution)

        task = ActivityTask.create(
            execution_id=execution.execution_id,
            activity_name="test_activity",
            activity_input={},
            idempotency_key=f"{execution.execution_id}:test_activity:1",
        )
        await database.enqueue_task(task)

        # Dequeue to make it RUNNING
        await database.dequeue_task("worker-1", ("test_activity",))

        # Heartbeat
        success = await database.heartbeat_task(task.task_id, timedelta(minutes=5))
        assert success is True

    @pytest.mark.asyncio
    async def test_get_pending_task_for_activity(self, database: Database) -> None:
        """Test getting pending task for specific activity."""
        execution = WorkflowExecution.create(
            workflow_name="test_workflow",
            input_data={},
        )
        await database.create_execution(execution)

        task = ActivityTask.create(
            execution_id=execution.execution_id,
            activity_name="test_activity",
            activity_input={},
            idempotency_key=f"{execution.execution_id}:test_activity:1",
        )
        await database.enqueue_task(task)

        # Get pending
        pending = await database.get_pending_task_for_activity(
            execution.execution_id,
            "test_activity",
        )
        assert pending is not None
        assert pending.task_id == task.task_id

        # Complete it
        await database.complete_task(task.task_id, {})

        # No longer pending
        pending = await database.get_pending_task_for_activity(
            execution.execution_id,
            "test_activity",
        )
        assert pending is None
