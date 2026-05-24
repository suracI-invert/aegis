"""Integration tests for end-to-end workflow execution (CK1, CK5)."""

import asyncio
import pytest
from uuid import UUID

from aegis import Client, Worker, WorkerConfig, workflow, activity, WorkflowStatus
from aegis.models import EventType

from tests.conftest import DATABASE_URL

pytestmark = pytest.mark.integration


@activity
async def add_one(x: int) -> int:
    """Add one to input."""
    return x + 1


@activity
async def multiply_two(x: int) -> int:
    """Multiply input by two."""
    return x * 2


@activity
async def subtract_three(x: int) -> int:
    """Subtract three from input."""
    return x - 3


@workflow
async def three_activity_workflow(x: int) -> int:
    """Workflow with three sequential activities (CK1)."""
    result1 = await add_one(x)
    result2 = await multiply_two(result1)
    result3 = await subtract_three(result2)
    return result3


@workflow
async def single_activity_workflow(x: int) -> int:
    """Simple workflow with single activity."""
    return await add_one(x)


class TestSimpleWorkflowExecution:
    """Tests for simple workflow execution (CK1)."""

    @pytest.mark.asyncio
    async def test_single_activity_workflow(self) -> None:
        """Test workflow with single activity completes."""
        config = WorkerConfig(
            database_url=DATABASE_URL,
            max_concurrent_activities=5,
            poll_interval=0.1,
        )
        worker = Worker(config)
        worker.register_workflow(single_activity_workflow)
        worker.register_activity(add_one)

        # Start worker
        await worker.start()
        worker_task = asyncio.create_task(worker._activity_loop())

        try:
            async with Client(DATABASE_URL) as client:
                execution_id = await client.start_workflow(
                    single_activity_workflow,
                    args=(5,),
                )

                # Run recovery to process workflow
                await worker._recover_workflows()

                result = await client.wait_for_result(execution_id, timeout=10.0)
                assert result == 6  # 5 + 1

                status = await client.get_workflow_status(execution_id)
                assert status == WorkflowStatus.COMPLETED
        finally:
            worker._running = False
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass
            await worker.stop()

    @pytest.mark.asyncio
    async def test_three_activity_workflow(self) -> None:
        """Test workflow with three activities completes (CK1)."""
        config = WorkerConfig(
            database_url=DATABASE_URL,
            max_concurrent_activities=5,
            poll_interval=0.1,
        )
        worker = Worker(config)
        worker.register_workflow(three_activity_workflow)
        worker.register_activity(add_one)
        worker.register_activity(multiply_two)
        worker.register_activity(subtract_three)

        await worker.start()
        worker_task = asyncio.create_task(worker._activity_loop())

        try:
            async with Client(DATABASE_URL) as client:
                execution_id = await client.start_workflow(
                    three_activity_workflow,
                    args=(5,),
                )

                # Process workflow multiple times for all activities
                for _ in range(5):
                    await worker._recover_workflows()
                    await asyncio.sleep(0.2)

                result = await client.wait_for_result(execution_id, timeout=15.0)
                # (5 + 1) * 2 - 3 = 12 - 3 = 9
                assert result == 9

                # Verify event history
                history = await client.get_workflow_history(execution_id)
                event_types = [e.event_type for e in history]

                assert EventType.WORKFLOW_STARTED in event_types
                assert EventType.WORKFLOW_COMPLETED in event_types
        finally:
            worker._running = False
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass
            await worker.stop()


class TestConcurrentWorkflows:
    """Tests for concurrent workflow execution (CK5)."""

    @pytest.mark.asyncio
    async def test_multiple_workflows_concurrent(self) -> None:
        """Test multiple workflows run concurrently (CK5)."""
        config = WorkerConfig(
            database_url=DATABASE_URL,
            max_concurrent_activities=10,
            max_concurrent_workflows=5,
            poll_interval=0.1,
        )
        worker = Worker(config)
        worker.register_workflow(single_activity_workflow)
        worker.register_activity(add_one)

        await worker.start()
        worker_task = asyncio.create_task(worker._activity_loop())

        try:
            async with Client(DATABASE_URL) as client:
                # Start 5 workflows concurrently
                execution_ids: list[UUID] = []
                for i in range(5):
                    eid = await client.start_workflow(
                        single_activity_workflow,
                        args=(i,),
                    )
                    execution_ids.append(eid)

                # Process all
                for _ in range(10):
                    await worker._recover_workflows()
                    await asyncio.sleep(0.2)

                # Verify all completed
                results = []
                for eid in execution_ids:
                    result = await client.wait_for_result(eid, timeout=10.0)
                    results.append(result)

                # Each should be input + 1
                assert results == [1, 2, 3, 4, 5]

                # All should be completed
                for eid in execution_ids:
                    status = await client.get_workflow_status(eid)
                    assert status == WorkflowStatus.COMPLETED
        finally:
            worker._running = False
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass
            await worker.stop()


class TestEventHistory:
    """Tests for complete event logging (CK8)."""

    @pytest.mark.asyncio
    async def test_event_history_logged(self) -> None:
        """Test that all events are properly logged (CK8)."""
        config = WorkerConfig(
            database_url=DATABASE_URL,
            max_concurrent_activities=5,
            poll_interval=0.1,
        )
        worker = Worker(config)
        worker.register_workflow(single_activity_workflow)
        worker.register_activity(add_one)

        await worker.start()
        worker_task = asyncio.create_task(worker._activity_loop())

        try:
            async with Client(DATABASE_URL) as client:
                execution_id = await client.start_workflow(
                    single_activity_workflow,
                    args=(10,),
                )

                for _ in range(5):
                    await worker._recover_workflows()
                    await asyncio.sleep(0.2)

                await client.wait_for_result(execution_id, timeout=10.0)

                # Get history
                history = await client.get_workflow_history(execution_id)

                # Should have all event types
                event_types = [e.event_type for e in history]

                assert EventType.WORKFLOW_STARTED in event_types
                assert EventType.ACTIVITY_SCHEDULED in event_types
                assert EventType.WORKFLOW_COMPLETED in event_types

                # Events should be in sequence order
                sequences = [e.sequence_number for e in history]
                assert sequences == sorted(sequences)
        finally:
            worker._running = False
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass
            await worker.stop()
