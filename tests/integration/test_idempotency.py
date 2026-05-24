"""Integration tests for idempotency (CK10)."""

import asyncio
import pytest
from uuid import uuid4

from aegis import Client, Worker, WorkerConfig, workflow, activity
from aegis.database import Database
from aegis.models import ActivityTask, TaskStatus, WorkflowExecution

from tests.conftest import DATABASE_URL

pytestmark = pytest.mark.integration


@activity
async def idempotent_activity(x: int) -> int:
    """Activity for idempotency testing."""
    return x * 2


@workflow
async def idempotent_workflow(x: int) -> int:
    """Workflow for idempotency testing."""
    return await idempotent_activity(x)


class TestIdempotency:
    """Tests for idempotent task scheduling (CK10)."""

    @pytest.mark.asyncio
    async def test_duplicate_task_not_created(self, database: Database) -> None:
        """Test duplicate task with same idempotency key not created (CK10)."""
        execution = WorkflowExecution.create(
            workflow_name="test_workflow",
            input_data={},
        )
        await database.create_execution(execution)

        idempotency_key = f"{execution.execution_id}:test_activity:1"

        # Create first task
        task1 = ActivityTask.create(
            execution_id=execution.execution_id,
            activity_name="test_activity",
            activity_input={"x": 1},
            idempotency_key=idempotency_key,
        )
        await database.enqueue_task(task1)

        # Try to create second task with same key
        task2 = ActivityTask.create(
            execution_id=execution.execution_id,
            activity_name="test_activity",
            activity_input={"x": 2},  # Different input
            idempotency_key=idempotency_key,  # Same key
        )
        await database.enqueue_task(task2)  # Should be silently ignored

        # Only first task should exist
        retrieved = await database.get_task(task1.task_id)
        assert retrieved is not None
        assert retrieved.activity_input == {"x": 1}

        # Second task should not exist
        retrieved2 = await database.get_task(task2.task_id)
        assert retrieved2 is None

    @pytest.mark.asyncio
    async def test_different_idempotency_keys_create_tasks(
        self,
        database: Database,
    ) -> None:
        """Test different idempotency keys create separate tasks."""
        execution = WorkflowExecution.create(
            workflow_name="test_workflow",
            input_data={},
        )
        await database.create_execution(execution)

        # Create tasks with different idempotency keys
        task1 = ActivityTask.create(
            execution_id=execution.execution_id,
            activity_name="test_activity",
            activity_input={"x": 1},
            idempotency_key=f"{execution.execution_id}:test_activity:1",
        )
        await database.enqueue_task(task1)

        task2 = ActivityTask.create(
            execution_id=execution.execution_id,
            activity_name="test_activity",
            activity_input={"x": 2},
            idempotency_key=f"{execution.execution_id}:test_activity:2",
        )
        await database.enqueue_task(task2)

        # Both tasks should exist
        retrieved1 = await database.get_task(task1.task_id)
        retrieved2 = await database.get_task(task2.task_id)

        assert retrieved1 is not None
        assert retrieved2 is not None
        assert retrieved1.activity_input == {"x": 1}
        assert retrieved2.activity_input == {"x": 2}

    @pytest.mark.asyncio
    async def test_idempotency_key_generation(self) -> None:
        """Test idempotency key is generated correctly."""
        execution_id = uuid4()

        task = ActivityTask.create(
            execution_id=execution_id,
            activity_name="my_activity",
            activity_input={},
            idempotency_key=f"{execution_id}:my_activity:5",
        )

        assert task.idempotency_key == f"{execution_id}:my_activity:5"

    @pytest.mark.asyncio
    async def test_workflow_activity_idempotency(self) -> None:
        """Test that workflow activity scheduling is idempotent."""
        config = WorkerConfig(
            database_url=DATABASE_URL,
            max_concurrent_activities=5,
            poll_interval=0.1,
        )
        worker = Worker(config)
        worker.register_workflow(idempotent_workflow)
        worker.register_activity(idempotent_activity)

        await worker.start()

        try:
            async with Client(DATABASE_URL) as client:
                execution_id = await client.start_workflow(
                    idempotent_workflow,
                    args=(5,),
                )

                # Simulate calling recovery multiple times (could happen in distributed system)
                for _ in range(3):
                    await worker._recover_workflows()

                # Even with multiple recovery attempts, workflow should complete correctly
                worker_task = asyncio.create_task(worker._activity_loop())
                try:
                    for _ in range(5):
                        await worker._recover_workflows()
                        await asyncio.sleep(0.2)

                    result = await client.wait_for_result(execution_id, timeout=10.0)
                    assert result == 10  # 5 * 2

                finally:
                    worker._running = False
                    worker_task.cancel()
                    try:
                        await worker_task
                    except asyncio.CancelledError:
                        pass
        finally:
            await worker.stop()


class TestActivityTaskQueue:
    """Tests for activity task queue behavior."""

    @pytest.mark.asyncio
    async def test_task_claimed_only_once(self, database: Database) -> None:
        """Test task can only be claimed by one worker."""
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

        # Worker 1 claims task
        tasks1 = await database.dequeue_task("worker-1", ("test_activity",))
        assert tasks1 is not None
        assert len(tasks1) == 1
        assert tasks1[0].task_id == task.task_id

        # Worker 2 cannot claim same task
        tasks2 = await database.dequeue_task("worker-2", ("test_activity",))
        assert tasks2 is not None
        assert len(tasks2) == 0

    @pytest.mark.asyncio
    async def test_task_priority_ordering(self, database: Database) -> None:
        """Test tasks are dequeued by priority."""
        execution = WorkflowExecution.create(
            workflow_name="test_workflow",
            input_data={},
        )
        await database.create_execution(execution)

        # Create tasks with different priorities
        low_priority = ActivityTask.create(
            execution_id=execution.execution_id,
            activity_name="test_activity",
            activity_input={"priority": "low"},
            idempotency_key=f"{execution.execution_id}:test_activity:low",
        )
        low_priority.priority = 1
        await database.enqueue_task(low_priority)

        high_priority = ActivityTask.create(
            execution_id=execution.execution_id,
            activity_name="test_activity",
            activity_input={"priority": "high"},
            idempotency_key=f"{execution.execution_id}:test_activity:high",
        )
        high_priority.priority = 10
        await database.enqueue_task(high_priority)

        # High priority should be dequeued first
        tasks = await database.dequeue_task("worker-1", ("test_activity",), prefetch=1)
        assert tasks is not None
        assert len(tasks) == 1
        assert tasks[0].activity_input["priority"] == "high"

    @pytest.mark.asyncio
    async def test_prefetch_multiple_tasks(self, database: Database) -> None:
        """Test prefetching multiple tasks."""
        execution = WorkflowExecution.create(
            workflow_name="test_workflow",
            input_data={},
        )
        await database.create_execution(execution)

        # Create multiple tasks with unique activity name
        for i in range(5):
            task = ActivityTask.create(
                execution_id=execution.execution_id,
                activity_name="prefetch_activity",
                activity_input={"index": i},
                idempotency_key=f"{execution.execution_id}:prefetch_activity:{i}",
            )
            await database.enqueue_task(task)

        # Prefetch 3 tasks
        tasks = await database.dequeue_task(
            "prefetch-worker-1",
            ("prefetch_activity",),
            prefetch=3,
        )
        assert tasks is not None
        assert len(tasks) == 3

        # Remaining 2 tasks still available
        remaining = await database.dequeue_task(
            "prefetch-worker-2",
            ("prefetch_activity",),
            prefetch=3,
        )
        assert remaining is not None
        assert len(remaining) == 2
