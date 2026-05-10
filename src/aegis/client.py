"""Client for starting and querying workflows."""

from __future__ import annotations

import asyncio
from typing import Any
from uuid import UUID

from aegis.database import Database
from aegis.definition import WorkflowDefinition
from aegis.models import (
    EventType,
    HistoryEvent,
    WorkflowExecution,
    WorkflowStatus,
)


class Client:
    """Client for interacting with Aegis workflows."""

    def __init__(self, database_url: str) -> None:
        """Initialize client with database URL."""
        self._database_url = database_url
        self._db: Database | None = None

    async def connect(self) -> None:
        """Connect to database."""
        self._db = await Database.connect(self._database_url)
        await self._db.init_schema()

    async def close(self) -> None:
        """Close database connection."""
        if self._db:
            await self._db.close()

    async def __aenter__(self) -> "Client":
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, *args: Any) -> None:
        """Async context manager exit."""
        await self.close()

    async def start_workflow[**P, T](
        self,
        workflow: WorkflowDefinition[P, T],
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        correlation_id: str | None = None,
    ) -> UUID:
        """Start a new workflow execution.

        Args:
            workflow: The workflow definition to execute.
            args: Positional arguments for the workflow.
            kwargs: Keyword arguments for the workflow.
            correlation_id: Optional business identifier for tracing.

        Returns:
            The execution ID of the started workflow.
        """
        if self._db is None:
            raise RuntimeError("Client not connected. Call connect() first.")

        _kwargs = kwargs or {}

        # Create execution record
        execution = WorkflowExecution.create(
            workflow_name=workflow.name,
            input_data={"args": list(args), "kwargs": _kwargs},
            correlation_id=correlation_id,
        )
        await self._db.create_execution(execution)

        # Log WorkflowStarted event
        event = HistoryEvent.create(
            execution_id=execution.execution_id,
            event_type=EventType.WORKFLOW_STARTED,
            event_data={"args": list(args), "kwargs": _kwargs},
            sequence_number=1,
        )
        await self._db.append_event(event)

        return execution.execution_id

    async def get_workflow_status(self, execution_id: UUID) -> WorkflowStatus | None:
        """Get the current status of a workflow execution.

        Args:
            execution_id: The execution ID to query.

        Returns:
            The workflow status or None if not found.
        """
        if self._db is None:
            raise RuntimeError("Client not connected. Call connect() first.")

        execution = await self._db.get_execution(execution_id)
        if execution is None:
            return None
        return execution.status

    async def get_workflow_result(self, execution_id: UUID) -> Any | None:
        """Get the result of a completed workflow.

        Args:
            execution_id: The execution ID to query.

        Returns:
            The workflow result or None if not completed.
        """
        if self._db is None:
            raise RuntimeError("Client not connected. Call connect() first.")

        execution = await self._db.get_execution(execution_id)
        if execution is None:
            return None
        if execution.result:
            return execution.result.get("value")
        return None

    async def wait_for_result(
        self,
        execution_id: UUID,
        timeout: float = 300.0,
        poll_interval: float = 0.5,
    ) -> Any:
        """Wait for workflow completion and return result.

        Args:
            execution_id: The execution ID to wait for.
            timeout: Maximum time to wait in seconds.
            poll_interval: Time between status checks in seconds.

        Returns:
            The workflow result.

        Raises:
            TimeoutError: If workflow doesn't complete within timeout.
            RuntimeError: If workflow fails.
        """
        if self._db is None:
            raise RuntimeError("Client not connected. Call connect() first.")

        elapsed = 0.0
        while elapsed < timeout:
            execution = await self._db.get_execution(execution_id)
            if execution is None:
                raise RuntimeError(f"Workflow {execution_id} not found")

            if execution.status == WorkflowStatus.COMPLETED:
                if execution.result:
                    return execution.result.get("value")
                return None

            if execution.status == WorkflowStatus.FAILED:
                error = execution.last_failure_reason or "Workflow failed"
                raise RuntimeError(f"Workflow failed: {error}")

            if execution.status == WorkflowStatus.CANCELLED:
                raise RuntimeError("Workflow was cancelled")

            await asyncio.sleep(poll_interval)
            elapsed += poll_interval

        raise TimeoutError(
            f"Workflow {execution_id} did not complete within {timeout}s"
        )

    async def cancel_workflow(self, execution_id: UUID) -> bool:
        """Cancel a running workflow.

        Args:
            execution_id: The execution ID to cancel.

        Returns:
            True if cancelled, False if not found or already completed.
        """
        if self._db is None:
            raise RuntimeError("Client not connected. Call connect() first.")

        execution = await self._db.get_execution(execution_id)
        if execution is None:
            return False

        if execution.status in (
            WorkflowStatus.COMPLETED,
            WorkflowStatus.FAILED,
            WorkflowStatus.CANCELLED,
        ):
            return False

        return await self._db.update_execution_status(
            execution_id, WorkflowStatus.CANCELLED
        )

    async def get_workflow_history(self, execution_id: UUID) -> list[HistoryEvent]:
        """Get the event history of a workflow.

        Args:
            execution_id: The execution ID to query.

        Returns:
            List of history events.
        """
        if self._db is None:
            raise RuntimeError("Client not connected. Call connect() first.")

        return await self._db.get_events(execution_id)

    async def recover_workflow(self, execution_id: UUID) -> bool:
        """Attempt to recover a failed workflow.

        Args:
            execution_id: The execution ID to recover.

        Returns:
            True if recovery initiated, False if not found or not failed.
        """
        if self._db is None:
            raise RuntimeError("Client not connected. Call connect() first.")

        execution = await self._db.get_execution(execution_id)
        if execution is None or execution.status != WorkflowStatus.FAILED:
            return False

        # Update status to RUNNING to allow worker to pick it up
        return await self._db.update_execution_status(
            execution_id, WorkflowStatus.RUNNING
        )
