# Aegis

**Lightweight Durable Execution Library for Python**

Aegis provides core Temporal-like capabilities for workflow orchestration without the operational complexity of a distributed service cluster. By using PostgreSQL as the single source of truth and implementing event sourcing with deterministic replay, Aegis enables workflows to survive process crashes and automatically recover.

## Features

- ✅ **Durable Execution** - Workflows survive process crashes
- ✅ **Automatic Retry** - Configurable retry with exponential backoff
- ✅ **Deterministic Replay** - Resume from last checkpoint without re-executing completed activities
- ✅ **Minimal Dependencies** - Only requires PostgreSQL
- ✅ **Library-based** - Embed directly into your application
- ✅ **Python Native** - Async/await, type hints, decorators

## Installation

```bash
pip install aegis
# or
uv add aegis
```

## Quick Start

### 1. Define Activities and Workflows

```python
from aegis import workflow, activity, RetryPolicy
from datetime import timedelta

@activity(
    retry_policy=RetryPolicy(
        max_attempts=3,
        initial_interval=timedelta(seconds=1),
        backoff_coefficient=2.0,
    )
)
async def process_payment(order_id: str, amount: float) -> dict:
    """Activity with side effect - calls external payment API."""
    # Your payment processing logic here
    return {"transaction_id": "TXN-123", "status": "SUCCESS"}

@activity
async def send_notification(email: str, message: str) -> None:
    """Send email notification."""
    # Your notification logic here
    pass

@workflow
async def order_workflow(order: dict) -> dict:
    """Process an order through payment and notification."""
    # Step 1: Process payment (with automatic retry)
    payment = await process_payment(order["id"], order["amount"])
    
    if payment["status"] != "SUCCESS":
        return {"status": "FAILED", "reason": "Payment failed"}
    
    # Step 2: Send confirmation
    await send_notification(order["email"], "Payment successful!")
    
    return {"status": "COMPLETED", "transaction_id": payment["transaction_id"]}
```

### 2. Start Worker

```python
from aegis import Worker, WorkerConfig

config = WorkerConfig(
    database_url="postgresql://localhost/aegis",
    max_concurrent_activities=10,
)

worker = Worker(config)
worker.register_workflow(order_workflow)
worker.register_activity(process_payment)
worker.register_activity(send_notification)

# Run worker (blocking)
await worker.run()
```

### 3. Start Workflows via Client

```python
from aegis import Client

async with Client("postgresql://localhost/aegis") as client:
    # Start workflow
    execution_id = await client.start_workflow(
        order_workflow,
        args=({"id": "ORD-123", "amount": 99.99, "email": "customer@example.com"},),
    )
    
    # Wait for result
    result = await client.wait_for_result(execution_id, timeout=60.0)
    print(f"Workflow completed: {result}")
```

## How It Works

### Event Sourcing

Aegis records every significant event during workflow execution:

```
WorkflowStarted → ActivityScheduled → ActivityCompleted → ... → WorkflowCompleted
```

This enables:
- **Complete audit trail** of workflow execution
- **Deterministic replay** for crash recovery
- **Point-in-time debugging**

### Crash Recovery

When a worker restarts:
1. It queries for incomplete workflows
2. Loads event history from PostgreSQL
3. Replays events to reconstruct state
4. Resumes execution from last checkpoint

**Key insight:** Completed activities are NOT re-executed during replay - their recorded results are used instead.

### PostgreSQL as Source of Truth

All state is stored in PostgreSQL:
- `workflow_executions` - Workflow lifecycle and status
- `history_events` - Event sourcing log (append-only)
- `activity_tasks` - Distributed task queue

## Requirements

- Python 3.11+
- PostgreSQL 12+

## Comparison with Temporal

| Aspect | Aegis | Temporal |
|--------|-------|----------|
| **State Storage** | PostgreSQL only | Cassandra/PostgreSQL + Elasticsearch |
| **Architecture** | Embedded library | Distributed cluster (4+ services) |
| **Setup Time** | < 30 minutes | Several hours |
| **Operational Overhead** | Minimal | Significant |
| **Scaling** | Vertical | Horizontal |
| **Use Case** | Small-to-medium workloads | Large-scale enterprise |

## License

MIT
