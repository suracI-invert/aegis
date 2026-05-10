"""Workflow and Activity decorators and execution context."""

from __future__ import annotations

import asyncio
import contextvars
from collections.abc import Callable, Coroutine
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import timedelta
from functools import wraps
from typing import Any
from uuid import UUID

from aegis.database import Database
from aegis.models import (
    ActivityTask,
    EventType,
    HistoryEvent,
    ReplayContext,
    RetryPolicy,
)
from aegis.utils import invoke_callable

# Context variable for current workflow context
_current_workflow_context: contextvars.ContextVar[WorkflowContext | None] = (
    contextvars.ContextVar("aegis_workflow_context", default=None)
)


@dataclass
class WorkflowContext:
    """Context for workflow execution."""

    execution_id: UUID
    workflow_name: str
    db: Database  # Database instance
    is_replaying: bool = False
    replay_context: ReplayContext = field(default_factory=ReplayContext)
    _sequence_counter: int = 0
    _pending_activities: dict[str, asyncio.Future[Any]] = field(default_factory=dict)

    @classmethod
    def current(cls) -> WorkflowContext:
        """Get current workflow context."""
        ctx = _current_workflow_context.get()
        if ctx is None:
            raise RuntimeError("No active workflow context. Are you inside a workflow?")
        return ctx

    @classmethod
    def get_current(cls) -> WorkflowContext | None:
        """Get current workflow context or None."""
        return _current_workflow_context.get()

    def next_sequence(self) -> int:
        """Get next sequence number."""
        self._sequence_counter += 1
        return self._sequence_counter

    async def log_event(
        self,
        event_type: EventType,
        event_data: dict[str, Any],
        activity_name: str | None = None,
    ) -> None:
        """Log event to history."""
        event = HistoryEvent.create(
            execution_id=self.execution_id,
            event_type=event_type,
            event_data=event_data,
            sequence_number=self.next_sequence(),
            activity_name=activity_name,
        )
        await self.db.append_event(event)


class WorkflowDefinition[**P, T]:
    """Wrapper for workflow function."""

    def __init__(
        self, func: Callable[P, Coroutine[Any, Any, T]], name: str | None = None
    ) -> None:
        """Initialize workflow definition."""
        self._func = func
        self._name = name or func.__name__
        wraps(func)(self)

    @property
    def name(self) -> str:
        """Get workflow name."""
        return self._name

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        """Execute workflow."""
        # If called within a context, just run the function
        ctx = WorkflowContext.get_current()
        if ctx is not None:
            return await self._func(*args, **kwargs)
        # Otherwise, this is an error - should use Client to start workflow
        raise RuntimeError("Workflow must be started via Client.start_workflow()")

    async def execute(
        self,
        db: Database,
        execution_id: UUID,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        replay_context: ReplayContext | None = None,
    ) -> T:
        """Execute workflow with context."""
        ctx = WorkflowContext(
            execution_id=execution_id,
            workflow_name=self._name,
            db=db,
            is_replaying=replay_context is not None,
            replay_context=replay_context or ReplayContext(),
        )

        # Set sequence counter from replay context
        if replay_context:
            ctx._sequence_counter = replay_context.next_sequence - 1

        token = _current_workflow_context.set(ctx)
        try:
            return await self._func(*args, **kwargs)
        finally:
            _current_workflow_context.reset(token)


class ActivityDefinition[**P, T]:
    """Wrapper for activity function."""

    def __init__(
        self,
        func: Callable[P, Coroutine[Any, Any, T]] | Callable[P, T],
        name: str | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        """Initialize activity definition."""
        self._func = func
        self._name = name or func.__name__
        self._retry_policy = retry_policy or RetryPolicy()
        wraps(func)(self)

    @property
    def name(self) -> str:
        """Get activity name."""
        return self._name

    @property
    def retry_policy(self) -> RetryPolicy:
        """Get retry policy."""
        return self._retry_policy

    async def __call__(
        self,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T | None:
        """Execute activity (schedules in workflow context or runs directly)."""
        ctx = WorkflowContext.get_current()
        call: Callable[P, Coroutine[Any, Any, T]] | Callable[P, T] = self._func

        if ctx is None:
            # Direct execution outside workflow
            return await invoke_callable(call, None, *args, **kwargs)

        # Inside workflow - check replay context first
        if ctx.replay_context.has_activity_result(self._name):
            # Return recorded result during replay
            return ctx.replay_context.get_activity_result(self._name)

        # Schedule activity and wait for result
        return await self._schedule_and_wait(ctx, args, kwargs)

    async def _schedule_and_wait(
        self,
        ctx: WorkflowContext,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Any:
        """Schedule activity task and wait for completion."""
        # Create idempotency key
        idempotency_key = f"{ctx.execution_id}:{self._name}:{ctx._sequence_counter}"

        # Log ActivityScheduled event

        # Create and enqueue task
        task = ActivityTask.create(
            execution_id=ctx.execution_id,
            activity_name=self._name,
            activity_input={"args": list(args), "kwargs": kwargs},
            retry_policy=self._retry_policy,
            idempotency_key=idempotency_key,
        )
        await ctx.log_event(
            EventType.ACTIVITY_SCHEDULED,
            {"args": list(args), "kwargs": kwargs},
            activity_name=self._name,
        )
        await ctx.db.enqueue_task(task)

        # Poll for result
        while True:
            await asyncio.sleep(0.5)  # Poll interval

            # Check if task completed
            updated_task = await ctx.db.get_task(task.task_id)
            if updated_task is None:
                raise RuntimeError(f"Task {task.task_id} not found")

            if updated_task.status.value == "COMPLETED":
                result = updated_task.result or {}
                # Log ActivityCompleted event
                await ctx.log_event(
                    EventType.ACTIVITY_COMPLETED,
                    {"result": result},
                    activity_name=self._name,
                )
                return result.get("value")

            if updated_task.status.value == "FAILED":
                error = updated_task.error_message or "Activity failed"
                await ctx.log_event(
                    EventType.ACTIVITY_FAILED,
                    {"error": error},
                    activity_name=self._name,
                )
                raise RuntimeError(f"Activity {self._name} failed: {error}")

    async def execute_direct(
        self, executor: ThreadPoolExecutor | None, *args: P.args, **kwargs: P.kwargs
    ) -> T:
        """Execute activity function directly (for workers)."""

        return await invoke_callable(self._func, executor, *args, **kwargs)


def workflow[**P, T](
    func: Callable[P, Coroutine[Any, Any, T]] | None = None,
    *,
    name: str | None = None,
) -> (
    WorkflowDefinition
    | Callable[[Callable[P, Coroutine[Any, Any, T]]], WorkflowDefinition]
):
    """Decorator to define a workflow.

    Args:
        func: Async function that defines workflow logic.
        name: Optional custom name for the workflow.

    Returns:
        WorkflowDefinition wrapping the function.
    """

    def decorator(f: Callable[P, Coroutine[Any, Any, T]]) -> WorkflowDefinition:
        return WorkflowDefinition(f, name=name)

    if func is not None:
        return decorator(func)
    return decorator


def activity[**P, T](
    func: Callable[P, Coroutine[Any, Any, T]] | Callable[P, T] | None = None,
    *,
    name: str | None = None,
    retry_policy: RetryPolicy | None = None,
) -> (
    ActivityDefinition
    | Callable[
        [Callable[P, Coroutine[Any, Any, T]] | Callable[P, T]], ActivityDefinition
    ]
):
    """Decorator to define an activity.

    Args:
        func: Async function that defines activity logic.
        name: Optional custom name for the activity.
        retry_policy: Retry configuration for the activity.

    Returns:
        ActivityDefinition wrapping the function.
    """

    def decorator(
        f: Callable[P, Coroutine[Any, Any, T]] | Callable[P, T],
    ) -> ActivityDefinition:
        return ActivityDefinition(f, name=name, retry_policy=retry_policy)

    if func is not None:
        return decorator(func)
    return decorator


def heartbeat(timeout: timedelta):
    """Decorator to add heartbeat to an activity.

    Args:
        timeout: Heartbeat timeout duration.

    Returns:
        Decorator that adds heartbeat functionality to an activity.
    """

    ctx = WorkflowContext.get_current()
    return ctx is not None
