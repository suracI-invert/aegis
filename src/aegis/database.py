"""Database operations for Aegis workflow engine using SQLAlchemy."""

import logging
from collections.abc import AsyncGenerator, Sequence
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from sqlalchemy import (
    TIMESTAMP,
    BigInteger,
    ForeignKey,
    Integer,
    MetaData,
    Numeric,
    String,
    Text,
    and_,
    func,
    or_,
    select,
    update,
)
from sqlalchemy.dialects.postgresql import JSONB, insert
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.sql import text

from aegis.models import (
    ActivityTask,
    EventType,
    HistoryEvent,
    TaskStatus,
    WorkflowExecution,
    WorkflowStatus,
)


def _utcnow() -> datetime:
    """Get current UTC datetime."""
    return datetime.now(timezone.utc)


# Workflow Executions Table


class Base(DeclarativeBase):
    metadata = MetaData(schema="aegis")


class HistoryEventModel(Base):
    __tablename__ = "history_events"

    event_id: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=True
    )
    task_id: Mapped[UUID | None] = mapped_column(
        PG_UUID(as_uuid=True),
        nullable=True,
    )

    execution_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("workflow_executions.execution_id", ondelete="CASCADE"),
        nullable=False,
    )
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    event_timestamp: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), default=func.now()
    )
    event_data: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default={}
    )
    activity_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    step_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_by: Mapped[str | None] = mapped_column(String(255), nullable=True)
    sequence_number: Mapped[int] = mapped_column(Integer, nullable=False)


class WorkflowExecutionModel(Base):
    __tablename__ = "workflow_executions"

    execution_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True)
    workflow_name: Mapped[str] = mapped_column(String(255), nullable=False)
    workflow_version: Mapped[str] = mapped_column(String(50), default="1.0")
    input: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    result: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="PENDING")
    current_step: Mapped[str] = mapped_column(String(255), nullable=True)
    last_event_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    parent_execution_id: Mapped[UUID | None] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("workflow_executions.execution_id"),
        nullable=True,
    )
    correlation_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    tags: Mapped[dict[str, Any]] = mapped_column(JSONB, default={})
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), default=func.now()
    )
    started_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    attempt_count: Mapped[int] = mapped_column(Integer, default=1)
    max_attempts: Mapped[int] = mapped_column(Integer, default=3)
    last_failure_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    version: Mapped[int] = mapped_column(Integer, default=1)
    locked_until: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    locked_by: Mapped[str | None] = mapped_column(String(255), nullable=True)


class ActivityTaskModel(Base):
    __tablename__ = "activity_tasks"
    task_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True)
    execution_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("workflow_executions.execution_id", ondelete="CASCADE"),
        nullable=False,
    )
    activity_name: Mapped[str] = mapped_column(String(255), nullable=False)
    activity_input: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="PENDING")
    result: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    scheduled_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), default=func.now()
    )
    started_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    attempt_count: Mapped[int] = mapped_column(Integer, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, default=3)
    next_retry_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    backoff_coefficient: Mapped[float] = mapped_column(Numeric(5, 2), default=2.0)
    worker_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    heartbeat_timeout: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    priority: Mapped[int] = mapped_column(Integer, default=0)
    idempotency_key: Mapped[str] = mapped_column(String(255), unique=True)


class Database:
    """Database connection and operations manager using SQLAlchemy."""

    def __init__(
        self, engine: AsyncEngine, logger: logging.Logger | None = None
    ) -> None:
        """Initialize database with async engine."""
        self._engine = engine
        self._session_factory = async_sessionmaker(
            engine, class_=AsyncSession, expire_on_commit=False
        )
        self._logger = logger or logging.getLogger(__name__)

    @classmethod
    async def connect(cls, database_url: str) -> "Database":
        """Create database connection.

        Args:
            database_url: PostgreSQL connection URL (postgresql:// or postgresql+psycopg://)
        """
        # Ensure async driver
        if database_url.startswith("postgresql://"):
            database_url = database_url.replace(
                "postgresql://", "postgresql+psycopg://", 1
            )

        engine = create_async_engine(database_url, echo=False)
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))  # Test connection
        return cls(engine)

    async def close(self) -> None:
        """Close database connection."""
        await self._engine.dispose()

    async def init_schema(self) -> None:
        """Initialize database schema."""
        async with self._engine.begin() as conn:
            schema = Base.metadata.schema or "aegis"
            await conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
            await conn.run_sync(Base.metadata.create_all)
            await conn.execute(
                text(f"""
-- Create activity_tasks notify function
CREATE OR REPLACE FUNCTION \"{schema}\".activity_tasks_notify() RETURNS TRIGGER AS $$
DECLARE
    payload text;
BEGIN
    payload := json_build_object(
        'task_id', NEW.task_id,
        'execution_id', NEW.execution_id,
        'activity_name', NEW.activity_name,
        'status', NEW.status,
        'op', TG_OP
    )::text;
    PERFORM pg_notify('aegis_activity_tasks', payload);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create activity_tasks trigger for INSERT
CREATE OR REPLACE TRIGGER aegis_activity_tasks_trigger
AFTER INSERT ON \"{schema}\".activity_tasks
FOR EACH ROW EXECUTE FUNCTION \"{schema}\".activity_tasks_notify();""")
            )

    def _session(self) -> AsyncSession:
        """Create a new session."""
        return self._session_factory()

    @asynccontextmanager
    async def _session_ctx(
        self, session: AsyncSession | None
    ) -> AsyncGenerator[AsyncSession, None]:
        """Use provided session or create a new isolated one with auto-commit.

        When a session is provided the caller owns the transaction — no commit
        is issued. When no session is provided a new session is created,
        committed, and closed automatically.
        """
        if session is not None:
            yield session
        else:
            async with self._session_factory() as s:
                yield s
                await s.commit()

    # Workflow Execution Operations

    async def create_execution(
        self, execution: WorkflowExecution, session: AsyncSession | None = None
    ) -> None:
        """Create a new workflow execution record."""
        async with self._session_ctx(session) as s:
            stmt = insert(WorkflowExecutionModel).values(
                execution_id=execution.execution_id,
                workflow_name=execution.workflow_name,
                workflow_version=execution.workflow_version,
                input=execution.input_data,
                status=execution.status.value,
                correlation_id=execution.correlation_id,
                created_at=execution.created_at,
                attempt_count=execution.attempt_count,
                max_attempts=execution.max_attempts,
            )
            await s.execute(stmt)

    async def get_execution(
        self, execution_id: UUID, session: AsyncSession | None = None
    ) -> WorkflowExecution | None:
        """Get workflow execution by ID."""
        async with self._session_ctx(session) as s:
            stmt = select(WorkflowExecutionModel).where(
                WorkflowExecutionModel.execution_id == execution_id
            )
            result = await s.execute(stmt)
            row = result.scalars().one_or_none()
            if row is None:
                return None
            return _convert_to_execution(row)

    async def update_execution_status(
        self,
        execution_id: UUID,
        status: WorkflowStatus,
        result: dict[str, Any] | None = None,
        error: str | None = None,
        expected_version: int | None = None,
        session: AsyncSession | None = None,
    ) -> bool:
        """Update workflow execution status with optimistic locking."""
        async with self._session_ctx(session) as s:
            stmt = update(WorkflowExecutionModel)
            now = _utcnow()

            values: dict[str, Any] = {
                "status": status.value,
                "version": WorkflowExecutionModel.version + 1,
            }

            if result is not None:
                values["result"] = result
            if error is not None:
                values["last_failure_reason"] = error

            if status in (WorkflowStatus.COMPLETED, WorkflowStatus.FAILED):
                values["completed_at"] = now

            # Build WHERE clause
            where_clause = WorkflowExecutionModel.execution_id == execution_id
            if expected_version is not None:
                where_clause = and_(
                    where_clause,
                    WorkflowExecutionModel.version == expected_version,
                )
            stmt = (
                stmt.where(where_clause)
                .values(**values)
                .returning(WorkflowExecutionModel.execution_id)
            )

            result_proxy = await s.execute(stmt)
            return result_proxy.scalars().one_or_none() is not None

    async def get_incomplete_executions(
        self,
        registered_workflows: Sequence[str],
        limit: int = 10,
        session: AsyncSession | None = None,
    ) -> list[WorkflowExecution]:
        """Get incomplete workflow executions that are not locked by other workers."""
        async with self._session_ctx(session) as s:
            now = _utcnow()
            stmt = (
                select(WorkflowExecutionModel)
                .where(
                    and_(
                        WorkflowExecutionModel.status.in_(
                            ["RUNNING", "RECOVERING", "PENDING"]
                        ),
                        WorkflowExecutionModel.workflow_name.in_(registered_workflows),
                        # Only get workflows not locked or with expired locks
                        or_(
                            WorkflowExecutionModel.locked_until.is_(None),
                            WorkflowExecutionModel.locked_until < now,
                        ),
                    )
                )
                .order_by(WorkflowExecutionModel.created_at.asc())
                .limit(limit)
            )
            result = await s.execute(stmt)
            return [_convert_to_execution(row) for row in result.scalars().all()]

    async def try_lock_workflow(
        self,
        execution_id: UUID,
        worker_id: str,
        lock_duration: timedelta = timedelta(minutes=5),
        session: AsyncSession | None = None,
    ) -> bool:
        """Try to acquire a lock on a workflow for exclusive processing.

        Uses optimistic locking to prevent multiple workers from processing
        the same workflow simultaneously.

        Args:
            execution_id: The workflow execution ID to lock.
            worker_id: The worker ID acquiring the lock.
            lock_duration: How long the lock should be held.

        Returns:
            True if lock acquired, False if already locked by another worker.
        """
        async with self._session_ctx(session) as s:
            now = _utcnow()
            lock_until = now + lock_duration

            # Only acquire lock if not currently locked (or lock expired)
            stmt = (
                update(WorkflowExecutionModel)
                .where(
                    and_(
                        WorkflowExecutionModel.execution_id == execution_id,
                        or_(
                            WorkflowExecutionModel.locked_until.is_(None),
                            WorkflowExecutionModel.locked_until < now,
                        ),
                    )
                )
                .values(
                    locked_by=worker_id,
                    locked_until=lock_until,
                )
                .returning(WorkflowExecutionModel.execution_id)
            )
            result = await s.execute(stmt)
            return result.tuples().one_or_none() is not None

    async def release_workflow_lock(
        self, execution_id: UUID, worker_id: str, session: AsyncSession | None = None
    ) -> bool:
        """Release a workflow lock.

        Only releases if the worker_id matches the current lock holder.

        Args:
            execution_id: The workflow execution ID.
            worker_id: The worker ID that holds the lock.

        Returns:
            True if lock released, False otherwise.
        """
        async with self._session_ctx(session) as s:
            stmt = (
                update(WorkflowExecutionModel)
                .where(
                    and_(
                        WorkflowExecutionModel.execution_id == execution_id,
                        WorkflowExecutionModel.locked_by == worker_id,
                    )
                )
                .values(
                    locked_by=None,
                    locked_until=None,
                )
                .returning(WorkflowExecutionModel.execution_id)
            )
            result = await s.execute(stmt)
            return result.tuples().one_or_none() is not None

    async def extend_workflow_lock(
        self,
        execution_id: UUID,
        worker_id: str,
        lock_duration: timedelta = timedelta(minutes=5),
        session: AsyncSession | None = None,
    ) -> bool:
        """Extend a workflow lock (heartbeat).

        Args:
            execution_id: The workflow execution ID.
            worker_id: The worker ID that holds the lock.
            lock_duration: How long to extend the lock.

        Returns:
            True if lock extended, False if not the lock holder.
        """
        async with self._session_ctx(session) as s:
            now = _utcnow()
            lock_until = now + lock_duration

            stmt = (
                update(WorkflowExecutionModel)
                .where(
                    and_(
                        WorkflowExecutionModel.execution_id == execution_id,
                        WorkflowExecutionModel.locked_by == worker_id,
                    )
                )
                .values(locked_until=lock_until)
                .returning(WorkflowExecutionModel.execution_id)
            )
            result = await s.execute(stmt)
            return result.tuples().one_or_none() is not None

    # History Event Operations

    async def append_event(
        self, event: HistoryEvent, session: AsyncSession | None = None
    ) -> int:
        """Append event to history log."""
        async with self._session_ctx(session) as s:
            stmt = (
                insert(HistoryEventModel)
                .values(
                    execution_id=event.execution_id,
                    event_type=event.event_type.value,
                    event_data=event.event_data,
                    sequence_number=event.sequence_number,
                    activity_name=event.activity_name,
                    step_name=event.step_name,
                    event_timestamp=event.event_timestamp,
                    created_by=event.created_by,
                )
                .returning(HistoryEventModel.event_id)
            )
            result = await s.execute(stmt)
            row = result.scalar_one_or_none()
            self._logger.debug(f"Appended event {event.event_type} to history log")
            return row if row else 0

    async def get_events(
        self, execution_id: UUID, session: AsyncSession | None = None
    ) -> list[HistoryEvent]:
        """Get all events for a workflow execution."""
        async with self._session_ctx(session) as s:
            stmt = (
                select(HistoryEventModel)
                .where(HistoryEventModel.execution_id == execution_id)
                .order_by(HistoryEventModel.sequence_number.asc())
            )
            result = await s.execute(stmt)
            return [_convert_to_event(row) for row in result.scalars().all()]

    async def get_next_sequence_number(
        self, execution_id: UUID, session: AsyncSession | None = None
    ) -> int:
        """Get next sequence number for events."""
        async with self._session_ctx(session) as s:
            stmt = select(
                func.coalesce(func.max(HistoryEventModel.sequence_number), 0) + 1
            ).where(HistoryEventModel.execution_id == execution_id)
            result = await s.execute(stmt)
            row = result.scalar_one_or_none()
            return row if row else 1

    # Activity Task Operations

    async def enqueue_task(
        self, task: ActivityTask, session: AsyncSession | None = None
    ) -> None:
        """Add activity task to queue."""
        async with self._session_ctx(session) as s:
            stmt = (
                insert(ActivityTaskModel)
                .values(
                    task_id=task.task_id,
                    execution_id=task.execution_id,
                    activity_name=task.activity_name,
                    activity_input=task.activity_input,
                    status=task.status.value,
                    scheduled_at=task.scheduled_at,
                    max_attempts=task.max_attempts,
                    backoff_coefficient=task.backoff_coefficient,
                    priority=task.priority,
                    idempotency_key=task.idempotency_key,
                )
                .on_conflict_do_nothing(index_elements=["idempotency_key"])
            )
            await s.execute(stmt)

    async def dequeue_task(
        self,
        worker_id: str,
        registered_activities: Sequence[str],
        prefetch: int = 1,
        session: AsyncSession | None = None,
    ) -> Sequence[ActivityTask] | None:
        """Claim next available task from queue using FOR UPDATE SKIP LOCKED."""
        async with self._session_ctx(session) as s:
            now = _utcnow()
            heartbeat = now + timedelta(seconds=30)

            # First, select the task to claim with FOR UPDATE SKIP LOCKED
            subquery = (
                select(ActivityTaskModel.task_id)
                .where(
                    and_(
                        or_(
                            # Normal pending/retrying tasks
                            and_(
                                ActivityTaskModel.status.in_(["PENDING", "RETRYING"]),
                                or_(
                                    ActivityTaskModel.next_retry_at.is_(None),
                                    ActivityTaskModel.next_retry_at <= now,
                                ),
                            ),
                            # Timed-out running tasks (heartbeat expired)
                            and_(
                                ActivityTaskModel.status == "RUNNING",
                                ActivityTaskModel.heartbeat_timeout < now,
                            ),
                        ),
                        ActivityTaskModel.activity_name.in_(registered_activities),
                    )
                )
                .order_by(
                    ActivityTaskModel.priority.desc(),
                    ActivityTaskModel.scheduled_at.asc(),
                )
                .limit(prefetch)
                .with_for_update(skip_locked=True)
            )

            # Update the selected task
            stmt = (
                update(ActivityTaskModel)
                .where(ActivityTaskModel.task_id.in_(subquery))
                .values(
                    status="RUNNING",
                    started_at=now,
                    worker_id=worker_id,
                    attempt_count=ActivityTaskModel.attempt_count + 1,
                    heartbeat_timeout=heartbeat,
                )
                .returning(ActivityTaskModel)
            )

            result = await s.execute(stmt)
            row = result.scalars().all()

            return [_convert_to_task(r) for r in row]

    async def complete_task(
        self, task_id: UUID, result: dict[str, Any], session: AsyncSession | None = None
    ) -> None:
        """Mark task as completed with result."""
        async with self._session_ctx(session) as s:
            stmt = (
                update(ActivityTaskModel)
                .where(ActivityTaskModel.task_id == task_id)
                .values(
                    status="COMPLETED",
                    result=result,
                    completed_at=_utcnow(),
                    heartbeat_timeout=None,
                )
            )
            await s.execute(stmt)

    async def heartbeat_task(
        self, task_id: UUID, timeout: timedelta, session: AsyncSession | None = None
    ) -> bool:
        """Extend task heartbeat timeout to prevent it from being claimed by other workers."""
        async with self._session_ctx(session) as s:
            now = _utcnow()
            new_timeout = now + timeout
            stmt = (
                update(ActivityTaskModel)
                .where(
                    and_(
                        ActivityTaskModel.task_id == task_id,
                        ActivityTaskModel.status == "RUNNING",
                    )
                )
                .values(heartbeat_timeout=new_timeout)
                .returning(ActivityTaskModel.task_id)
            )
            result = await s.execute(stmt)
            return result.scalars().one_or_none() is not None

    async def fail_task(
        self,
        task_id: UUID,
        error: str,
        retry: bool,
        next_retry_at: datetime | None = None,
        session: AsyncSession | None = None,
    ) -> None:
        """Mark task as failed, optionally scheduling retry."""
        async with self._session_ctx(session) as s:
            status = TaskStatus.RETRYING.value if retry else TaskStatus.FAILED.value
            stmt = (
                update(ActivityTaskModel)
                .where(ActivityTaskModel.task_id == task_id)
                .values(
                    status=status,
                    error_message=error,
                    next_retry_at=next_retry_at,
                )
            )
            await s.execute(stmt)

    async def get_task(
        self, task_id: UUID, session: AsyncSession | None = None
    ) -> ActivityTask | None:
        """Get activity task by ID."""
        async with self._session_ctx(session) as s:
            stmt = select(ActivityTaskModel).where(ActivityTaskModel.task_id == task_id)
            result = await s.execute(stmt)
            row = result.scalar_one_or_none()
            if row is None:
                return None
            return _convert_to_task(row)

    async def get_pending_task_for_activity(
        self,
        execution_id: UUID,
        activity_name: str,
        session: AsyncSession | None = None,
    ) -> ActivityTask | None:
        """Get pending task for specific activity."""
        async with self._session_ctx(session) as s:
            stmt = (
                select(ActivityTaskModel)
                .where(
                    and_(
                        ActivityTaskModel.execution_id == execution_id,
                        ActivityTaskModel.activity_name == activity_name,
                        ~ActivityTaskModel.status.in_(["COMPLETED", "FAILED"]),
                    )
                )
                .limit(1)
            )
            result = await s.execute(stmt)
            row = result.scalar_one_or_none()
            if row is None:
                return None
            return _convert_to_task(row)


def _convert_to_execution(row: WorkflowExecutionModel) -> WorkflowExecution:
    """Convert WorkflowExecutionModel to WorkflowExecution."""
    return WorkflowExecution(
        execution_id=row.execution_id,
        workflow_name=row.workflow_name,
        workflow_version=row.workflow_version,
        input_data=row.input,
        status=WorkflowStatus(row.status),
        result=row.result,
        current_step=row.current_step,
        last_event_id=row.last_event_id,
        parent_execution_id=row.parent_execution_id,
        correlation_id=row.correlation_id,
        created_at=row.created_at,
        started_at=row.started_at,
        completed_at=row.completed_at,
        attempt_count=row.attempt_count,
        max_attempts=row.max_attempts,
        last_failure_reason=row.last_failure_reason,
        version=row.version,
    )


def _convert_to_event(row: HistoryEventModel) -> HistoryEvent:
    """Convert HistoryEventModel to HistoryEvent."""
    return HistoryEvent(
        event_id=row.event_id,
        execution_id=row.execution_id,
        event_type=EventType(row.event_type),
        event_data=row.event_data,
        sequence_number=row.sequence_number,
        activity_name=row.activity_name,
        step_name=row.step_name,
        event_timestamp=row.event_timestamp,
        created_by=row.created_by,
    )


def _convert_to_task(row: ActivityTaskModel) -> ActivityTask:
    """Convert ActivityTaskModel to ActivityTask."""
    return ActivityTask(
        task_id=row.task_id,
        execution_id=row.execution_id,
        activity_name=row.activity_name,
        activity_input=row.activity_input,
        status=TaskStatus(row.status),
        result=row.result,
        error_message=row.error_message,
        scheduled_at=row.scheduled_at,
        started_at=row.started_at,
        completed_at=row.completed_at,
        attempt_count=row.attempt_count,
        max_attempts=row.max_attempts,
        next_retry_at=row.next_retry_at,
        backoff_coefficient=float(row.backoff_coefficient),
        worker_id=row.worker_id,
        heartbeat_timeout=row.heartbeat_timeout,
        priority=row.priority,
        idempotency_key=row.idempotency_key,
    )
