"""arq event capture via WorkerSettings hook chaining."""

from __future__ import annotations

import asyncio

import pytest

pytest.importorskip("arq")

from z4j_core.models.event import EventKind  # noqa: E402

from z4j_arq import ArqEngineAdapter, attach_to_worker_settings  # noqa: E402


class _WorkerSettings:
    """Stand-in for a real arq WorkerSettings class."""

    @staticmethod
    async def on_job_start(ctx):  # pragma: no cover - replaced by chain
        pass

    @staticmethod
    async def on_job_end(ctx):  # pragma: no cover
        pass


class _FakePool:
    async def enqueue_job(self, *_a, **_kw):  # pragma: no cover
        return None


@pytest.fixture
def adapter():
    return ArqEngineAdapter(
        redis_settings=_FakePool(),
        function_names=["myapp.send"],
    )


@pytest.mark.asyncio
async def test_attach_chains_hooks(adapter):
    capture = attach_to_worker_settings(_WorkerSettings, adapter=adapter)
    # The class's hooks are now z4j-wrapped.
    await _WorkerSettings.on_job_start({"job_id": "j1", "job_function": "myapp.send"})
    await _WorkerSettings.on_job_end(
        {"job_id": "j1", "job_function": "myapp.send", "success": True},
    )
    await asyncio.sleep(0)  # let queue drain
    items = []
    while not adapter._event_queue.empty():
        items.append(adapter._event_queue.get_nowait())
    kinds = [i.kind for i in items]
    assert EventKind.TASK_STARTED in kinds
    assert EventKind.TASK_SUCCEEDED in kinds


@pytest.mark.asyncio
async def test_failure_path_emits_failed(adapter):
    attach_to_worker_settings(_WorkerSettings, adapter=adapter)
    await _WorkerSettings.on_job_end(
        {"job_id": "j2", "job_function": "myapp.send", "success": False},
    )
    await asyncio.sleep(0)
    evts = []
    while not adapter._event_queue.empty():
        evts.append(adapter._event_queue.get_nowait())
    assert any(e.kind == EventKind.TASK_FAILED for e in evts)


@pytest.mark.asyncio
async def test_user_hooks_still_run_when_chained(adapter):
    user_calls = []

    class UserWS:
        @staticmethod
        async def on_job_start(ctx):
            user_calls.append(("start", ctx["job_id"]))

        @staticmethod
        async def on_job_end(ctx):
            user_calls.append(("end", ctx["job_id"]))

    attach_to_worker_settings(UserWS, adapter=adapter)
    await UserWS.on_job_start({"job_id": "u1", "job_function": "x"})
    await UserWS.on_job_end({"job_id": "u1", "job_function": "x", "success": True})
    assert user_calls == [("start", "u1"), ("end", "u1")]


@pytest.mark.asyncio
async def test_subscribe_events_drains_queue(adapter):
    attach_to_worker_settings(_WorkerSettings, adapter=adapter)
    await _WorkerSettings.on_job_start({"job_id": "z1", "job_function": "x"})
    async def _take():
        async for e in adapter.subscribe_events():
            return e
    evt = await asyncio.wait_for(_take(), timeout=0.5)
    assert evt.kind == EventKind.TASK_STARTED
