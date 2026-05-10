"""Worker for processing activity tasks."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Coroutine
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from aegis.database import Database
from aegis.definition import ActivityDefinition, WorkflowDefinition
from aegis.models import (
    ActivityTask,
    EventType,
    HistoryEvent,
    ReplayContext,
    WorkflowExecution,
    WorkflowStatus,
)


def _utcnow() -> datetime:
    """Get current UTC datetime."""
    return datetime.now(timezone.utc)


logger = logging.getLogger(__name__)


@dataclass
class WorkerConfig:
    """Configuration for worker."""

    database_url: str
    task_queue: str = "default"
    max_concurrent_activities: int = 10
    max_concurrent_workflows: int = 5
    poll_interval: float = 1.0
    worker_id: str = field(default_factory=lambda: f"worker-{uuid4().hex[:8]}")


class Worker:
    """Worker that processes workflows and activities."""

    def __init__(self, config: WorkerConfig) -> None:
        """Initialize worker with configuration."""
        self.config = config
        self._db: Database | None = None
        self._workflows: dict[str, WorkflowDefinition] = {}
        self._activities: dict[str, ActivityDefinition] = {}
        self._running = False

        self._activity_executor = ThreadPoolExecutor(
            max_workers=self.config.max_concurrent_activities
        )
        self._workflow_executor = ThreadPoolExecutor(
            max_workers=self.config.max_concurrent_workflows
        )  # Single thread for workflow execution to maintain order

        self._activity_tasks: set[asyncio.Future[None]] = set()
        self._workflow_tasks: set[asyncio.Future[None]] = set()

    def register_workflow(self, workflow: WorkflowDefinition) -> None:
        """Register a workflow definition."""
        self._workflows[workflow.name] = workflow
        logger.info(f"Registered workflow: {workflow.name}")

    def register_activity(self, activity: ActivityDefinition) -> None:
        """Register an activity definition."""
        self._activities[activity.name] = activity
        logger.info(f"Registered activity: {activity.name}")

    async def start(self) -> None:
        """Start the worker."""
        self._db = await Database.connect(self.config.database_url)
        await self._db.init_schema()
        self._running = True
        logger.info(f"Worker {self.config.worker_id} started")

    async def stop(self) -> None:
        """Stop the worker."""
        self._running = False
        if self._activity_tasks:
            logger.info("Waiting for activity tasks to complete...")
            _, pending = await asyncio.wait(self._activity_tasks, timeout=5.0)
            if pending:
                logger.warning(f"Cancelling {len(pending)} pending activity tasks...")
                for t in pending:
                    t.cancel()
        if self._workflow_tasks:
            logger.info("Waiting for workflow tasks to complete...")
            _, pending = await asyncio.wait(self._workflow_tasks, timeout=5.0)
            if pending:
                logger.warning(f"Cancelling {len(pending)} pending workflow tasks...")
                for t in pending:
                    t.cancel()
        if self._activity_tasks or self._workflow_tasks:
            logger.warning(
                f"{len(self._activity_tasks)} activity tasks and {len(self._workflow_tasks)} workflow tasks did not complete in time"
            )
            all_to_finish: list[asyncio.Future[None]] = list(self._activity_tasks)
            all_to_finish.extend(list(self._workflow_tasks))
            await asyncio.gather(*all_to_finish, return_exceptions=True)

        if self._db:
            await self._db.close()
        self._activity_executor.shutdown(wait=True, cancel_futures=True)
        self._workflow_executor.shutdown(wait=True, cancel_futures=True)
        logger.info(f"Worker {self.config.worker_id} stopped")

    async def run(self) -> None:
        """Run the worker main loop."""
        await self.start()
        try:
            # Run recovery and activity processing concurrently
            await asyncio.gather(
                self._recovery_loop(),
                self._activity_loop(),
            )
        finally:
            await self.stop()

    async def run_once(self) -> None:
        """Run one iteration (useful for testing)."""
        if self._db is None:
            await self.start()
        await self._process_activities_once()

    async def _recovery_loop(self) -> None:
        """Loop to recover incomplete workflows."""
        while self._running:
            try:
                await self._recover_workflows()
            except Exception as e:
                logger.error(f"Error in recovery loop: {e}")
            await asyncio.sleep(5.0)  # Check every 5 seconds

    async def _activity_loop(self) -> None:
        """Loop to process activity tasks."""
        while self._running:
            try:
                await self._process_activities_once()
            except Exception as e:
                logger.error(f"Error in activity loop: {e}")
            await asyncio.sleep(self.config.poll_interval)

    async def _handle_activity_task(self, task: ActivityTask) -> None:
        if self._db is None:
            return
        logger.info(f"Processing task {task.task_id} for activity {task.activity_name}")

        activity = self._activities.get(task.activity_name)
        if activity is None:
            return

        try:
            # Execute activity
            args = task.activity_input.get("args", [])
            kwargs = task.activity_input.get("kwargs", {})

            event = HistoryEvent.create(
                execution_id=task.execution_id,
                event_type=EventType.ACTIVITY_STARTED,
                event_data={"args": list(args), "kwargs": kwargs},
                sequence_number=await self._db.get_next_sequence_number(
                    task.execution_id
                ),
                activity_name=task.activity_name,
            )
            await self._db.append_event(event)

            result = await activity.execute_direct(
                self._activity_executor, *args, **kwargs
            )
            # Complete task
            async with self._db._session() as session:
                await session.begin()
                # Log ActivityCompleted event
                event = HistoryEvent.create(
                    execution_id=task.execution_id,
                    event_type=EventType.ACTIVITY_COMPLETED,
                    event_data={"result": result},
                    sequence_number=await self._db.get_next_sequence_number(
                        task.execution_id
                    ),
                    activity_name=task.activity_name,
                )

                await self._db.complete_task(task.task_id, {"value": result})
                logger.info(f"Task {task.task_id} completed successfully")

        except Exception as e:
            try:
                async with self._db._session() as session:
                    await session.begin()
                    should_retry = task.attempt_count < task.max_attempts
                    next_retry: datetime | None = None
                    if should_retry:
                        delay = activity.retry_policy.next_retry_delay(
                            task.attempt_count
                        )
                        next_retry = _utcnow() + delay
                    else:
                        event = HistoryEvent.create(
                            execution_id=task.execution_id,
                            event_type=EventType.ACTIVITY_FAILED,
                            event_data={"error": str(e), "retry": should_retry},
                            sequence_number=await self._db.get_next_sequence_number(
                                task.execution_id
                            ),
                            activity_name=task.activity_name,
                        )
                        await self._db.append_event(event)

                    await self._db.fail_task(
                        task.task_id,
                        str(e),
                        retry=should_retry,
                        next_retry_at=next_retry,
                    )
                    logger.error(f"Task {task.task_id} failed: {e}")
            except Exception as db_e:
                logger.error(
                    f"Failed to update task {task.task_id} status after failure: {db_e}"
                )
            # Check if should retry

    async def _process_activities_once(self) -> None:
        """Process one batch of activity tasks."""
        if self._db is None:
            return

        if len(self._activity_tasks) >= self.config.max_concurrent_activities:
            logger.debug(
                f"Max concurrent activities reached ({len(self._activity_tasks)}/{self.config.max_concurrent_activities}), skipping polling"
            )
            return
        prefetch = self.config.max_concurrent_activities - len(self._activity_tasks)
        tasks = await self._db.dequeue_task(
            self.config.worker_id, tuple(self._activities.keys()), prefetch=prefetch
        )
        if not tasks:
            return
        for task in tasks:
            t = asyncio.get_running_loop().run_in_executor(
                self._activity_executor,
                self._wrap_execution,
                self._handle_activity_task(task),
            )
            self._activity_tasks.add(t)
            t.add_done_callback(self._activity_tasks.discard)

    async def _handle_workflow_resume(self, execution: WorkflowExecution) -> None:
        if self._db is None:
            return
        if execution.workflow_name not in self._workflows:
            return

        locked = await self._db.try_lock_workflow(
            execution.execution_id,
            self.config.worker_id,
        )
        if not locked:
            logger.debug(f"Workflow {execution.execution_id} locked by another worker")
            return

        logger.info(f"Resuming workflow {execution.execution_id}")
        try:
            await self._resume_workflow(execution)
        except Exception as e:
            logger.error(f"Failed to resume workflow {execution.execution_id}: {e}")
        finally:
            await self._db.release_workflow_lock(
                execution.execution_id,
                self.config.worker_id,
            )

    async def _recover_workflows(self) -> None:
        """Recover incomplete workflows with distributed locking."""
        if self._db is None:
            return
        if len(self._workflow_tasks) >= self.config.max_concurrent_workflows:
            logger.debug(
                f"Max concurrent workflows reached ({len(self._workflow_tasks)}/{self.config.max_concurrent_workflows}), skipping recovery"
            )
            return
        limit = self.config.max_concurrent_workflows - len(self._workflow_tasks)
        executions = await self._db.get_incomplete_executions(
            tuple(self._workflows.keys()), limit=limit
        )

        for execution in executions:
            t = asyncio.get_running_loop().run_in_executor(
                self._workflow_executor,
                self._wrap_execution,
                self._handle_workflow_resume(execution),
            )
            self._workflow_tasks.add(t)
            t.add_done_callback(self._workflow_tasks.discard)

    def _wrap_execution[T](
        self,
        coro: Coroutine[Any, Any, T],
    ) -> T:
        """Wrap a workflow execution coroutine to run in the workflow executor."""
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    async def _resume_workflow(self, execution: WorkflowExecution) -> None:
        """Resume a workflow execution."""
        if self._db is None:
            return

        workflow = self._workflows.get(execution.workflow_name)
        if workflow is None:
            return

        # Build replay context from events
        events = await self._db.get_events(execution.execution_id)
        replay_context = self._build_replay_context(events)

        # Update status to RUNNING
        await self._db.update_execution_status(
            execution.execution_id,
            WorkflowStatus.RUNNING,
        )

        # Execute workflow with replay context
        try:
            result = await workflow.execute(
                self._db,
                execution.execution_id,
                args=execution.input_data.get("args", ()),
                kwargs=execution.input_data.get("kwargs", {}),
                replay_context=replay_context,
            )

            # Complete workflow
            seq = await self._db.get_next_sequence_number(execution.execution_id)
            event = HistoryEvent.create(
                execution_id=execution.execution_id,
                event_type=EventType.WORKFLOW_COMPLETED,
                event_data={"result": result},
                sequence_number=seq,
            )
            await self._db.append_event(event)
            await self._db.update_execution_status(
                execution.execution_id,
                WorkflowStatus.COMPLETED,
                result={"value": result},
            )
            logger.info(f"Workflow {execution.execution_id} completed")

        except Exception as e:
            logger.exception(f"Workflow {execution.execution_id} failed: {e}")
            seq = await self._db.get_next_sequence_number(execution.execution_id)
            event = HistoryEvent.create(
                execution_id=execution.execution_id,
                event_type=EventType.WORKFLOW_FAILED,
                event_data={"error": str(e)},
                sequence_number=seq,
            )
            await self._db.append_event(event)
            await self._db.update_execution_status(
                execution.execution_id,
                WorkflowStatus.FAILED,
                error=str(e),
            )
        except asyncio.CancelledError:
            logger.warning(f"Workflow {execution.execution_id} cancelled")
            raise

    def _build_replay_context(self, events: list[HistoryEvent]) -> ReplayContext:
        """Build replay context from event history."""
        ctx = ReplayContext()
        max_seq = 0

        for event in events:
            max_seq = max(max_seq, event.sequence_number)

            if event.event_type == EventType.ACTIVITY_COMPLETED:
                activity_name = event.activity_name
                if activity_name:
                    result = event.event_data.get("result", {})
                    ctx.activity_results[activity_name] = result.get("value")

            elif event.event_type == EventType.WORKFLOW_DECISION:
                decision_point = event.event_data.get("decision_point")
                branch = event.event_data.get("branch_taken")
                if decision_point and branch:
                    ctx.decisions[decision_point] = branch

            elif event.event_type == EventType.ACTIVITY_SCHEDULED:
                activity_name = event.activity_name
                if activity_name and activity_name not in ctx.activity_results:
                    # Activity scheduled but not completed - resume from here
                    if ctx.resume_from is None:
                        ctx.resume_from = event.sequence_number

        ctx.next_sequence = max_seq + 1
        return ctx
