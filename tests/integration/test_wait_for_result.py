"""Integration tests for wait_for_result edge cases."""

import asyncio
import pytest
from uuid import uuid4

from aegis import Client, workflow, WorkflowStatus

from tests.conftest import DATABASE_URL

pytestmark = pytest.mark.integration


class TestWaitForResult:
    """Tests for wait_for_result scenarios."""

    @pytest.mark.asyncio
    async def test_wait_for_result_timeout(self, client: Client) -> None:
        """Test wait_for_result times out correctly."""
        @workflow
        async def slow_wf() -> int:
            return 1

        execution_id = await client.start_workflow(slow_wf)

        # Should timeout since no worker processes it
        with pytest.raises(TimeoutError):
            await client.wait_for_result(execution_id, timeout=0.5, poll_interval=0.1)

    @pytest.mark.asyncio
    async def test_wait_for_result_failed(self, client: Client) -> None:
        """Test wait_for_result raises on failed workflow."""
        @workflow
        async def fail_wf() -> int:
            return 1

        execution_id = await client.start_workflow(fail_wf)

        # Manually fail the workflow
        await client._db.update_execution_status(
            execution_id,
            WorkflowStatus.FAILED,
            error="Test failure",
        )

        with pytest.raises(RuntimeError, match="Workflow failed"):
            await client.wait_for_result(execution_id, timeout=5.0)

    @pytest.mark.asyncio
    async def test_wait_for_result_cancelled(self, client: Client) -> None:
        """Test wait_for_result raises on cancelled workflow."""
        @workflow
        async def cancel_wf() -> int:
            return 1

        execution_id = await client.start_workflow(cancel_wf)

        # Cancel the workflow
        await client.cancel_workflow(execution_id)

        with pytest.raises(RuntimeError, match="cancelled"):
            await client.wait_for_result(execution_id, timeout=5.0)

    @pytest.mark.asyncio
    async def test_wait_for_result_not_found(self, client: Client) -> None:
        """Test wait_for_result raises for non-existent workflow."""
        with pytest.raises(RuntimeError, match="not found"):
            await client.wait_for_result(uuid4(), timeout=1.0)

    @pytest.mark.asyncio
    async def test_wait_for_result_completed_no_result(self, client: Client) -> None:
        """Test wait_for_result handles completed workflow with no result."""
        @workflow
        async def void_wf() -> None:
            pass

        execution_id = await client.start_workflow(void_wf)

        # Complete with no result
        await client._db.update_execution_status(
            execution_id,
            WorkflowStatus.COMPLETED,
        )

        result = await client.wait_for_result(execution_id, timeout=5.0)
        assert result is None
