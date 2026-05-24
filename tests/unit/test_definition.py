"""Unit tests for WorkflowDefinition and ActivityDefinition."""

import pytest

from aegis import RetryPolicy, activity, workflow
from aegis.definition import (
    ActivityDefinition,
    WorkflowContext,
    WorkflowDefinition,
    _current_workflow_context,
)
from aegis.models import ReplayContext


class TestWorkflowDefinition:
    """Tests for WorkflowDefinition class."""

    def test_workflow_decorator_creates_definition(self) -> None:
        """Test workflow decorator creates WorkflowDefinition."""

        @workflow
        async def my_workflow(x: int) -> int:
            return x * 2

        assert isinstance(my_workflow, WorkflowDefinition)
        assert my_workflow.name == "my_workflow"

    def test_workflow_decorator_with_custom_name(self) -> None:
        """Test workflow decorator with custom name."""

        @workflow(name="custom_workflow")
        async def my_workflow(x: int) -> int:
            return x * 2

        assert my_workflow.name == "custom_workflow"

    @pytest.mark.asyncio
    async def test_workflow_call_outside_context_raises(self) -> None:
        """Test calling workflow outside context raises error."""

        @workflow
        async def my_workflow(x: int) -> int:
            return x * 2

        with pytest.raises(RuntimeError, match="must be started via Client"):
            await my_workflow(5)


class TestActivityDefinition:
    """Tests for ActivityDefinition class."""

    def test_activity_decorator_creates_definition(self) -> None:
        """Test activity decorator creates ActivityDefinition."""

        @activity
        async def my_activity(x: int) -> int:
            return x * 2

        assert isinstance(my_activity, ActivityDefinition)
        assert my_activity.name == "my_activity"

    def test_activity_decorator_with_custom_name(self) -> None:
        """Test activity decorator with custom name."""

        @activity(name="custom_activity")
        async def my_activity(x: int) -> int:
            return x * 2

        assert my_activity.name == "custom_activity"

    def test_activity_decorator_with_retry_policy(self) -> None:
        """Test activity decorator with custom retry policy."""
        policy = RetryPolicy(max_attempts=5)

        @activity(retry_policy=policy)
        async def my_activity(x: int) -> int:
            return x * 2

        assert my_activity.retry_policy.max_attempts == 5

    def test_activity_default_retry_policy(self) -> None:
        """Test activity has default retry policy."""

        @activity
        async def my_activity(x: int) -> int:
            return x * 2

        assert my_activity.retry_policy.max_attempts == 3

    @pytest.mark.asyncio
    async def test_activity_direct_execution(self) -> None:
        """Test activity can be called directly outside workflow."""

        @activity
        async def my_activity(x: int) -> int:
            return x * 2

        result = await my_activity(5)
        assert result == 10

    @pytest.mark.asyncio
    async def test_sync_activity_direct_execution(self) -> None:
        """Test sync activity can be called directly."""

        @activity
        def my_sync_activity(x: int) -> int:
            return x + 10

        result = await my_sync_activity(5)
        assert result == 15


class TestWorkflowContext:
    """Tests for WorkflowContext class."""

    def test_current_raises_when_no_context(self) -> None:
        """Test current() raises when no active context."""
        with pytest.raises(RuntimeError, match="No active workflow context"):
            WorkflowContext.current()

    def test_get_current_returns_none_when_no_context(self) -> None:
        """Test get_current() returns None when no active context."""
        assert WorkflowContext.get_current() is None

    def test_next_sequence_increments(self) -> None:
        """Test next_sequence increments counter."""
        from uuid import uuid4

        from aegis.database import Database
        from unittest.mock import MagicMock

        mock_db = MagicMock(spec=Database)
        ctx = WorkflowContext(
            execution_id=uuid4(),
            workflow_name="test",
            db=mock_db,
        )

        assert ctx.next_sequence() == 1
        assert ctx.next_sequence() == 2
        assert ctx.next_sequence() == 3


class TestReplayContextIntegration:
    """Tests for ReplayContext with activity definitions."""

    def test_replay_context_activity_result_tracking(self) -> None:
        """Test tracking activity results in replay context."""
        ctx = ReplayContext()

        # Initially no results
        assert not ctx.has_activity_result("test_activity")
        assert ctx.get_activity_result("test_activity") is None

        # Add result
        ctx.activity_results["test_activity"] = 42

        # Now has result
        assert ctx.has_activity_result("test_activity")
        assert ctx.get_activity_result("test_activity") == 42

    def test_replay_context_decision_tracking(self) -> None:
        """Test tracking decisions in replay context."""
        ctx = ReplayContext()

        # Add decisions
        ctx.decisions["branch_1"] = "left"
        ctx.decisions["branch_2"] = "right"

        assert ctx.decisions["branch_1"] == "left"
        assert ctx.decisions["branch_2"] == "right"
