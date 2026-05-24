"""Integration tests for Client."""

import pytest
from uuid import uuid4

from aegis import Client, workflow, activity, WorkflowStatus
from aegis.models import EventType

pytestmark = pytest.mark.integration


class TestClientLifecycle:
    """Tests for Client connection lifecycle."""

    @pytest.mark.asyncio
    async def test_context_manager(self) -> None:
        """Test Client as async context manager."""
        from tests.conftest import DATABASE_URL

        async with Client(DATABASE_URL) as client:
            assert client._db is not None

    @pytest.mark.asyncio
    async def test_connect_and_close(self) -> None:
        """Test explicit connect and close."""
        from tests.conftest import DATABASE_URL

        client = Client(DATABASE_URL)
        await client.connect()
        assert client._db is not None

        await client.close()

    @pytest.mark.asyncio
    async def test_error_when_not_connected(self) -> None:
        """Test operations fail when not connected."""
        from tests.conftest import DATABASE_URL

        client = Client(DATABASE_URL)

        @workflow
        async def dummy() -> int:
            return 1

        with pytest.raises(RuntimeError, match="not connected"):
            await client.start_workflow(dummy)


class TestStartWorkflow:
    """Tests for starting workflows."""

    @pytest.mark.asyncio
    async def test_start_workflow(self, client: Client) -> None:
        """Test starting a workflow."""
        @workflow
        async def test_wf(x: int) -> int:
            return x

        execution_id = await client.start_workflow(
            test_wf,
            args=(42,),
        )

        assert execution_id is not None

        # Verify status
        status = await client.get_workflow_status(execution_id)
        assert status == WorkflowStatus.PENDING

    @pytest.mark.asyncio
    async def test_start_workflow_with_correlation_id(self, client: Client) -> None:
        """Test starting workflow with correlation ID."""
        @workflow
        async def test_wf() -> int:
            return 1

        execution_id = await client.start_workflow(
            test_wf,
            correlation_id="order-123",
        )

        # Verify via database
        execution = await client._db.get_execution(execution_id)
        assert execution is not None
        assert execution.correlation_id == "order-123"

    @pytest.mark.asyncio
    async def test_start_workflow_with_kwargs(self, client: Client) -> None:
        """Test starting workflow with keyword arguments."""
        @workflow
        async def test_wf(x: int, multiplier: int = 1) -> int:
            return x * multiplier

        execution_id = await client.start_workflow(
            test_wf,
            args=(5,),
            kwargs={"multiplier": 3},
        )

        execution = await client._db.get_execution(execution_id)
        assert execution is not None
        assert execution.input_data["args"] == [5]
        assert execution.input_data["kwargs"] == {"multiplier": 3}


class TestGetWorkflowStatus:
    """Tests for getting workflow status."""

    @pytest.mark.asyncio
    async def test_get_status_pending(self, client: Client) -> None:
        """Test getting status of pending workflow."""
        @workflow
        async def test_wf() -> int:
            return 1

        execution_id = await client.start_workflow(test_wf)
        status = await client.get_workflow_status(execution_id)

        assert status == WorkflowStatus.PENDING

    @pytest.mark.asyncio
    async def test_get_status_not_found(self, client: Client) -> None:
        """Test getting status of non-existent workflow."""
        status = await client.get_workflow_status(uuid4())
        assert status is None


class TestGetWorkflowResult:
    """Tests for getting workflow result."""

    @pytest.mark.asyncio
    async def test_get_result_not_completed(self, client: Client) -> None:
        """Test getting result of incomplete workflow."""
        @workflow
        async def test_wf() -> int:
            return 1

        execution_id = await client.start_workflow(test_wf)
        result = await client.get_workflow_result(execution_id)

        assert result is None

    @pytest.mark.asyncio
    async def test_get_result_completed(self, client: Client) -> None:
        """Test getting result of completed workflow."""
        @workflow
        async def test_wf() -> int:
            return 42

        execution_id = await client.start_workflow(test_wf)

        # Manually complete for test
        await client._db.update_execution_status(
            execution_id,
            WorkflowStatus.COMPLETED,
            result={"value": 42},
        )

        result = await client.get_workflow_result(execution_id)
        assert result == 42

    @pytest.mark.asyncio
    async def test_get_result_not_found(self, client: Client) -> None:
        """Test getting result of non-existent workflow."""
        result = await client.get_workflow_result(uuid4())
        assert result is None


class TestCancelWorkflow:
    """Tests for cancelling workflows (CK9)."""

    @pytest.mark.asyncio
    async def test_cancel_pending_workflow(self, client: Client) -> None:
        """Test cancelling a pending workflow."""
        @workflow
        async def test_wf() -> int:
            return 1

        execution_id = await client.start_workflow(test_wf)

        # Cancel
        cancelled = await client.cancel_workflow(execution_id)
        assert cancelled is True

        # Verify status
        status = await client.get_workflow_status(execution_id)
        assert status == WorkflowStatus.CANCELLED

    @pytest.mark.asyncio
    async def test_cancel_running_workflow(self, client: Client) -> None:
        """Test cancelling a running workflow."""
        @workflow
        async def test_wf() -> int:
            return 1

        execution_id = await client.start_workflow(test_wf)

        # Set to running
        await client._db.update_execution_status(
            execution_id,
            WorkflowStatus.RUNNING,
        )

        # Cancel
        cancelled = await client.cancel_workflow(execution_id)
        assert cancelled is True

        status = await client.get_workflow_status(execution_id)
        assert status == WorkflowStatus.CANCELLED

    @pytest.mark.asyncio
    async def test_cancel_completed_workflow_fails(self, client: Client) -> None:
        """Test cancelling already completed workflow fails."""
        @workflow
        async def test_wf() -> int:
            return 1

        execution_id = await client.start_workflow(test_wf)

        await client._db.update_execution_status(
            execution_id,
            WorkflowStatus.COMPLETED,
        )

        cancelled = await client.cancel_workflow(execution_id)
        assert cancelled is False

    @pytest.mark.asyncio
    async def test_cancel_failed_workflow_fails(self, client: Client) -> None:
        """Test cancelling already failed workflow fails."""
        @workflow
        async def test_wf() -> int:
            return 1

        execution_id = await client.start_workflow(test_wf)

        await client._db.update_execution_status(
            execution_id,
            WorkflowStatus.FAILED,
        )

        cancelled = await client.cancel_workflow(execution_id)
        assert cancelled is False

    @pytest.mark.asyncio
    async def test_cancel_not_found(self, client: Client) -> None:
        """Test cancelling non-existent workflow."""
        cancelled = await client.cancel_workflow(uuid4())
        assert cancelled is False


class TestGetWorkflowHistory:
    """Tests for workflow event history (CK8)."""

    @pytest.mark.asyncio
    async def test_get_history(self, client: Client) -> None:
        """Test getting workflow event history."""
        @workflow
        async def test_wf() -> int:
            return 1

        execution_id = await client.start_workflow(test_wf)

        # Get history - should have WorkflowStarted event
        history = await client.get_workflow_history(execution_id)

        assert len(history) >= 1
        assert history[0].event_type == EventType.WORKFLOW_STARTED

    @pytest.mark.asyncio
    async def test_history_empty_for_nonexistent(self, client: Client) -> None:
        """Test getting history for non-existent workflow."""
        history = await client.get_workflow_history(uuid4())
        assert history == []


class TestRecoverWorkflow:
    """Tests for workflow recovery."""

    @pytest.mark.asyncio
    async def test_recover_failed_workflow(self, client: Client) -> None:
        """Test recovering a failed workflow."""
        @workflow
        async def test_wf() -> int:
            return 1

        execution_id = await client.start_workflow(test_wf)

        await client._db.update_execution_status(
            execution_id,
            WorkflowStatus.FAILED,
        )

        recovered = await client.recover_workflow(execution_id)
        assert recovered is True

        status = await client.get_workflow_status(execution_id)
        assert status == WorkflowStatus.RUNNING

    @pytest.mark.asyncio
    async def test_recover_non_failed_workflow_fails(self, client: Client) -> None:
        """Test recovering non-failed workflow fails."""
        @workflow
        async def test_wf() -> int:
            return 1

        execution_id = await client.start_workflow(test_wf)

        # Still pending
        recovered = await client.recover_workflow(execution_id)
        assert recovered is False

    @pytest.mark.asyncio
    async def test_recover_not_found(self, client: Client) -> None:
        """Test recovering non-existent workflow."""
        recovered = await client.recover_workflow(uuid4())
        assert recovered is False
