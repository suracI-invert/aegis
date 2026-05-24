"""Unit tests for Worker class."""

import pytest
from unittest.mock import MagicMock, AsyncMock
from uuid import uuid4

from aegis import Worker, WorkerConfig, activity, workflow
from aegis.definition import ActivityDefinition, WorkflowDefinition
from aegis.models import (
    EventType,
    HistoryEvent,
    ReplayContext,
    WorkflowStatus,
)


class TestWorkerConfig:
    """Tests for WorkerConfig."""

    def test_default_values(self) -> None:
        """Test default WorkerConfig values."""
        config = WorkerConfig(database_url="postgresql://test")

        assert config.database_url == "postgresql://test"
        assert config.task_queue == "default"
        assert config.max_concurrent_activities == 10
        assert config.max_concurrent_workflows == 5
        assert config.poll_interval == 1.0
        assert config.worker_id.startswith("worker-")

    def test_custom_values(self) -> None:
        """Test custom WorkerConfig values."""
        config = WorkerConfig(
            database_url="postgresql://custom",
            task_queue="custom_queue",
            max_concurrent_activities=20,
            max_concurrent_workflows=10,
            poll_interval=0.5,
            worker_id="custom-worker",
        )

        assert config.database_url == "postgresql://custom"
        assert config.task_queue == "custom_queue"
        assert config.max_concurrent_activities == 20
        assert config.max_concurrent_workflows == 10
        assert config.poll_interval == 0.5
        assert config.worker_id == "custom-worker"


class TestWorkerRegistration:
    """Tests for workflow and activity registration."""

    def test_register_workflow(self) -> None:
        """Test registering a workflow."""
        config = WorkerConfig(database_url="postgresql://test")
        worker = Worker(config)

        @workflow
        async def my_workflow(x: int) -> int:
            return x

        worker.register_workflow(my_workflow)

        assert "my_workflow" in worker._workflows
        assert worker._workflows["my_workflow"] is my_workflow

    def test_register_activity(self) -> None:
        """Test registering an activity."""
        config = WorkerConfig(database_url="postgresql://test")
        worker = Worker(config)

        @activity
        async def my_activity(x: int) -> int:
            return x

        worker.register_activity(my_activity)

        assert "my_activity" in worker._activities
        assert worker._activities["my_activity"] is my_activity

    def test_register_multiple_workflows(self) -> None:
        """Test registering multiple workflows."""
        config = WorkerConfig(database_url="postgresql://test")
        worker = Worker(config)

        @workflow
        async def workflow1() -> int:
            return 1

        @workflow
        async def workflow2() -> int:
            return 2

        worker.register_workflow(workflow1)
        worker.register_workflow(workflow2)

        assert len(worker._workflows) == 2
        assert "workflow1" in worker._workflows
        assert "workflow2" in worker._workflows


class TestBuildReplayContext:
    """Tests for _build_replay_context method."""

    def test_build_replay_context_empty_events(self) -> None:
        """Test building replay context from empty events."""
        config = WorkerConfig(database_url="postgresql://test")
        worker = Worker(config)

        ctx = worker._build_replay_context([])

        assert ctx.activity_results == {}
        assert ctx.decisions == {}
        assert ctx.next_sequence == 1
        assert ctx.resume_from is None

    def test_build_replay_context_with_completed_activity(self) -> None:
        """Test building replay context with completed activity event."""
        config = WorkerConfig(database_url="postgresql://test")
        worker = Worker(config)

        execution_id = uuid4()
        events = [
            HistoryEvent(
                event_id=1,
                execution_id=execution_id,
                event_type=EventType.ACTIVITY_COMPLETED,
                event_data={"result": {"value": 42}},
                sequence_number=1,
                activity_name="test_activity",
            ),
        ]

        ctx = worker._build_replay_context(events)

        assert ctx.activity_results["test_activity"] == 42
        assert ctx.next_sequence == 2

    def test_build_replay_context_with_decision(self) -> None:
        """Test building replay context with workflow decision event."""
        config = WorkerConfig(database_url="postgresql://test")
        worker = Worker(config)

        execution_id = uuid4()
        events = [
            HistoryEvent(
                event_id=1,
                execution_id=execution_id,
                event_type=EventType.WORKFLOW_DECISION,
                event_data={"decision_point": "branch_1", "branch_taken": "left"},
                sequence_number=1,
            ),
        ]

        ctx = worker._build_replay_context(events)

        assert ctx.decisions["branch_1"] == "left"

    def test_build_replay_context_with_scheduled_not_completed(self) -> None:
        """Test replay context sets resume_from for scheduled but not completed."""
        config = WorkerConfig(database_url="postgresql://test")
        worker = Worker(config)

        execution_id = uuid4()
        events = [
            HistoryEvent(
                event_id=1,
                execution_id=execution_id,
                event_type=EventType.ACTIVITY_SCHEDULED,
                event_data={"args": [], "kwargs": {}},
                sequence_number=1,
                activity_name="pending_activity",
            ),
        ]

        ctx = worker._build_replay_context(events)

        assert ctx.resume_from == 1
        assert "pending_activity" not in ctx.activity_results

    def test_build_replay_context_multiple_activities(self) -> None:
        """Test replay context with multiple completed activities."""
        config = WorkerConfig(database_url="postgresql://test")
        worker = Worker(config)

        execution_id = uuid4()
        events = [
            HistoryEvent(
                event_id=1,
                execution_id=execution_id,
                event_type=EventType.WORKFLOW_STARTED,
                event_data={},
                sequence_number=1,
            ),
            HistoryEvent(
                event_id=2,
                execution_id=execution_id,
                event_type=EventType.ACTIVITY_COMPLETED,
                event_data={"result": {"value": 10}},
                sequence_number=2,
                activity_name="activity_1",
            ),
            HistoryEvent(
                event_id=3,
                execution_id=execution_id,
                event_type=EventType.ACTIVITY_COMPLETED,
                event_data={"result": {"value": 20}},
                sequence_number=3,
                activity_name="activity_2",
            ),
        ]

        ctx = worker._build_replay_context(events)

        assert ctx.activity_results["activity_1"] == 10
        assert ctx.activity_results["activity_2"] == 20
        assert ctx.next_sequence == 4
