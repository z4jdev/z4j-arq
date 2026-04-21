"""End-to-end smoke for z4j-arq.

Exercises submit_task + middleware + reconcile_task without
needing a real Redis (we drive the WorkerSettings hook chain
directly to simulate worker execution).

If you want a true Redis run, set ``Z4J_ARQ_TEST_REDIS_URL`` and
add a marker that requires it.
"""

from __future__ import annotations

import asyncio

import pytest

pytest.importorskip("arq")

pytestmark = pytest.mark.integration

from z4j_core.models.event import EventKind  # noqa: E402

from z4j_arq import ArqEngineAdapter, attach_to_worker_settings  # noqa: E402


class _FakePool:
    def __init__(self):
        self.enqueued: list[tuple] = []

    async def enqueue_job(self, name, *a, **kw):
        # Mimic arq.connections.Job: returns object with ``job_id``.
        self.enqueued.append((name, a, kw))

        class _Job:
            job_id = f"job-{len(self.enqueued)}"

        return _Job()


class _WorkerSettings:
    @staticmethod
    async def on_job_start(ctx):
        pass

    @staticmethod
    async def on_job_end(ctx):
        pass


@pytest.mark.asyncio
async def test_submit_then_capture_then_reconcile():
    pool = _FakePool()
    adapter = ArqEngineAdapter(
        redis_settings=pool, function_names=["myapp.send"],
    )
    capture = attach_to_worker_settings(_WorkerSettings, adapter=adapter)
    assert capture is not None

    # Discovery sees the registered function.
    defs = await adapter.discover_tasks()
    assert {d.name for d in defs} == {"myapp.send"}

    # Submit goes through the pool.
    res = await adapter.submit_task("myapp.send", args=("alice@example.com",))
    assert res.status == "success"
    assert res.result["task_id"] == "job-1"
    assert pool.enqueued[0][0] == "myapp.send"

    # Simulate a worker run firing the chained hooks.
    await _WorkerSettings.on_job_start(
        {"job_id": "job-1", "job_function": "myapp.send"},
    )
    await _WorkerSettings.on_job_end(
        {"job_id": "job-1", "job_function": "myapp.send", "success": True},
    )
    await asyncio.sleep(0)
    kinds = []
    while not adapter._event_queue.empty():
        kinds.append(adapter._event_queue.get_nowait().kind)
    assert EventKind.TASK_STARTED in kinds
    assert EventKind.TASK_SUCCEEDED in kinds

    # Reconcile against an unknown id returns "unknown" (no Redis).
    reconcile = await adapter.reconcile_task("not-real")
    assert reconcile.status == "success"
    assert reconcile.result["engine_state"] == "unknown"
