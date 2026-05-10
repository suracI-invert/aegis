"""Aegis - Lightweight Durable Execution Library for Python."""

from aegis.client import Client
from aegis.models import RetryPolicy, WorkflowStatus, TaskStatus, EventType
from aegis.worker import Worker, WorkerConfig
from aegis.definition import workflow, activity, WorkflowDefinition, ActivityDefinition

__version__ = "0.1.0"

__all__ = [
    # Core decorators
    "workflow",
    "activity",
    # Classes
    "Client",
    "Worker",
    "WorkerConfig",
    "WorkflowDefinition",
    "ActivityDefinition",
    # Models
    "RetryPolicy",
    "WorkflowStatus",
    "TaskStatus",
    "EventType",
]
