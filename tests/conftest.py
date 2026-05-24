"""Shared pytest fixtures for Aegis tests."""

import asyncio
import os
from collections.abc import AsyncGenerator
from typing import Any

import pytest
from sqlalchemy import text

from aegis import Client, Worker, WorkerConfig, activity, workflow
from aegis.database import Database
from aegis.models import RetryPolicy

# Database URL for testing
DATABASE_URL = os.getenv(
    "TEST_DATABASE_URL", "postgresql://aegis:aegis@localhost:5432/aegis"
)


@pytest.fixture(scope="session")
def event_loop_policy():
    """Use default event loop policy."""
    return asyncio.DefaultEventLoopPolicy()


async def _cleanup_database(db: Database) -> None:
    """Clean up all test data from database."""
    async with db._session() as session:
        await session.execute(text('DELETE FROM aegis.history_events'))
        await session.execute(text('DELETE FROM aegis.activity_tasks'))
        await session.execute(text('DELETE FROM aegis.workflow_executions'))
        await session.commit()


@pytest.fixture
async def database() -> AsyncGenerator[Database, None]:
    """Create database connection for testing."""
    db = await Database.connect(DATABASE_URL)
    await db.init_schema()
    await _cleanup_database(db)
    yield db
    await _cleanup_database(db)
    await db.close()


@pytest.fixture
async def client() -> AsyncGenerator[Client, None]:
    """Create client for testing."""
    db = await Database.connect(DATABASE_URL)
    await db.init_schema()
    await _cleanup_database(db)
    await db.close()
    
    async with Client(DATABASE_URL) as c:
        yield c


@pytest.fixture
def worker_config() -> WorkerConfig:
    """Create worker config for testing."""
    return WorkerConfig(
        database_url=DATABASE_URL,
        max_concurrent_activities=5,
        max_concurrent_workflows=3,
        poll_interval=0.1,
    )


@pytest.fixture
async def worker(worker_config: WorkerConfig) -> AsyncGenerator[Worker, None]:
    """Create worker for testing."""
    w = Worker(worker_config)
    await w.start()
    yield w
    await w.stop()


# Sample workflow and activities for testing


@activity
async def simple_activity(x: int) -> int:
    """Simple activity that doubles input."""
    return x * 2


@activity
async def sync_activity(x: int) -> int:
    """Sync activity for testing."""
    return x + 1


@activity(retry_policy=RetryPolicy(max_attempts=3))
async def failing_activity(fail_count: int) -> str:
    """Activity that fails a specified number of times before succeeding."""
    # Use a class attribute to track attempts
    if not hasattr(failing_activity, "_attempts"):
        failing_activity._attempts = {}  # type: ignore[attr-defined]
    
    key = f"fail_{fail_count}"
    failing_activity._attempts[key] = failing_activity._attempts.get(key, 0) + 1  # type: ignore[attr-defined]
    
    if failing_activity._attempts[key] <= fail_count:  # type: ignore[attr-defined]
        raise RuntimeError(f"Simulated failure {failing_activity._attempts[key]}")  # type: ignore[attr-defined]
    
    return "success"


@activity(retry_policy=RetryPolicy(max_attempts=2))
async def always_failing_activity() -> str:
    """Activity that always fails."""
    raise RuntimeError("Always fails")


@activity
async def slow_activity(delay: float) -> str:
    """Activity that takes time to complete."""
    await asyncio.sleep(delay)
    return "done"


@workflow
async def simple_workflow(x: int) -> int:
    """Simple workflow with one activity."""
    result = await simple_activity(x)
    return result


@workflow
async def multi_activity_workflow(x: int) -> int:
    """Workflow with multiple sequential activities."""
    result1 = await simple_activity(x)
    result2 = await simple_activity(result1)
    result3 = await simple_activity(result2)
    return result3


@workflow
async def retry_workflow(fail_count: int) -> str:
    """Workflow that tests retry behavior."""
    return await failing_activity(fail_count)


@workflow
async def failing_workflow() -> str:
    """Workflow that will fail after max retries."""
    return await always_failing_activity()


@pytest.fixture
def sample_workflow() -> Any:
    """Return the simple workflow definition."""
    return simple_workflow


@pytest.fixture
def sample_activity() -> Any:
    """Return the simple activity definition."""
    return simple_activity
