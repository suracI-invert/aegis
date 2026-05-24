"""Integration tests for activity retry behavior (CK2, CK7)."""

import asyncio
import pytest
from datetime import timedelta

from aegis import Client, Worker, WorkerConfig, workflow, activity, WorkflowStatus
from aegis.models import RetryPolicy, TaskStatus, EventType

from tests.conftest import DATABASE_URL

pytestmark = pytest.mark.integration


# Track call counts for retry testing
_call_counts: dict[str, int] = {}


def reset_call_counts() -> None:
    """Reset call counts between tests."""
    global _call_counts
    _call_counts = {}


@activity(retry_policy=RetryPolicy(max_attempts=3, initial_interval=timedelta(seconds=0.1)))
async def fail_twice_then_succeed(key: str) -> str:
    """Activity that fails twice then succeeds."""
    global _call_counts
    _call_counts[key] = _call_counts.get(key, 0) + 1

    if _call_counts[key] <= 2:
        raise RuntimeError(f"Simulated failure {_call_counts[key]}")

    return f"success-{key}"


@activity(retry_policy=RetryPolicy(max_attempts=2, initial_interval=timedelta(seconds=0.1)))
async def always_fail(key: str) -> str:
    """Activity that always fails."""
    global _call_counts
    _call_counts[key] = _call_counts.get(key, 0) + 1
    raise RuntimeError(f"Always fails - attempt {_call_counts[key]}")


@activity
async def simple_success(x: int) -> int:
    """Activity that always succeeds."""
    return x * 2


@workflow
async def retry_workflow(key: str) -> str:
    """Workflow that uses retrying activity."""
    return await fail_twice_then_succeed(key)


@workflow
async def fail_workflow(key: str) -> str:
    """Workflow with activity that always fails."""
    return await always_fail(key)


@workflow
async def success_workflow(x: int) -> int:
    """Workflow that always succeeds."""
    return await simple_success(x)


class TestActivityRetry:
    """Tests for activity automatic retry (CK2)."""

    @pytest.mark.asyncio
    async def test_activity_retries_on_failure(self) -> None:
        """Test activity automatically retries after failure (CK2)."""
        reset_call_counts()

        config = WorkerConfig(
            database_url=DATABASE_URL,
            max_concurrent_activities=5,
            poll_interval=0.1,
        )
        worker = Worker(config)
        worker.register_workflow(retry_workflow)
        worker.register_activity(fail_twice_then_succeed)

        await worker.start()
        worker_task = asyncio.create_task(worker._activity_loop())

        try:
            async with Client(DATABASE_URL) as client:
                key = "test-retry-1"
                execution_id = await client.start_workflow(
                    retry_workflow,
                    args=(key,),
                )

                # Process multiple times for retries
                for _ in range(15):
                    await worker._recover_workflows()
                    await asyncio.sleep(0.2)

                result = await client.wait_for_result(execution_id, timeout=20.0)
                assert result == f"success-{key}"

                # Should have been called 3 times (2 failures + 1 success)
                assert _call_counts[key] == 3

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


class TestMaxRetryExceeded:
    """Tests for max retry exceeded behavior (CK7)."""

    @pytest.mark.asyncio
    async def test_workflow_fails_after_max_retries(self) -> None:
        """Test workflow fails when activity exceeds max retries (CK7)."""
        reset_call_counts()

        config = WorkerConfig(
            database_url=DATABASE_URL,
            max_concurrent_activities=5,
            poll_interval=0.1,
        )
        worker = Worker(config)
        worker.register_workflow(fail_workflow)
        worker.register_activity(always_fail)

        await worker.start()
        worker_task = asyncio.create_task(worker._activity_loop())

        try:
            async with Client(DATABASE_URL) as client:
                key = "test-max-retry"
                execution_id = await client.start_workflow(
                    fail_workflow,
                    args=(key,),
                )

                # Process multiple times
                for _ in range(15):
                    await worker._recover_workflows()
                    await asyncio.sleep(0.2)

                # Workflow should fail
                with pytest.raises(RuntimeError, match="failed"):
                    await client.wait_for_result(execution_id, timeout=20.0)

                status = await client.get_workflow_status(execution_id)
                assert status == WorkflowStatus.FAILED

                # Activity should have been called max_attempts times (2)
                assert _call_counts[key] == 2

                # Check event history has ACTIVITY_FAILED
                history = await client.get_workflow_history(execution_id)
                event_types = [e.event_type for e in history]
                assert EventType.ACTIVITY_FAILED in event_types
        finally:
            worker._running = False
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass
            await worker.stop()


class TestRetryPolicy:
    """Tests for retry policy configuration."""

    def test_retry_policy_defaults(self) -> None:
        """Test RetryPolicy default values."""
        policy = RetryPolicy()

        assert policy.max_attempts == 3
        assert policy.initial_interval == timedelta(seconds=1)
        assert policy.backoff_coefficient == 2.0
        assert policy.max_interval == timedelta(minutes=5)

    def test_retry_policy_custom(self) -> None:
        """Test RetryPolicy with custom values."""
        policy = RetryPolicy(
            max_attempts=5,
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=3.0,
            max_interval=timedelta(minutes=10),
        )

        assert policy.max_attempts == 5
        assert policy.initial_interval == timedelta(seconds=2)
        assert policy.backoff_coefficient == 3.0
        assert policy.max_interval == timedelta(minutes=10)

    def test_exponential_backoff(self) -> None:
        """Test exponential backoff calculation."""
        policy = RetryPolicy(
            initial_interval=timedelta(seconds=1),
            backoff_coefficient=2.0,
            max_interval=timedelta(seconds=30),
        )

        # attempt 0: 1 * 2^0 = 1s
        assert policy.next_retry_delay(0) == timedelta(seconds=1)

        # attempt 1: 1 * 2^1 = 2s
        assert policy.next_retry_delay(1) == timedelta(seconds=2)

        # attempt 2: 1 * 2^2 = 4s
        assert policy.next_retry_delay(2) == timedelta(seconds=4)

        # attempt 3: 1 * 2^3 = 8s
        assert policy.next_retry_delay(3) == timedelta(seconds=8)

        # attempt 4: 1 * 2^4 = 16s
        assert policy.next_retry_delay(4) == timedelta(seconds=16)

        # attempt 5: 1 * 2^5 = 32s, capped at 30s
        assert policy.next_retry_delay(5) == timedelta(seconds=30)
