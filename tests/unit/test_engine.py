"""ArqEngineAdapter tests using a fake pool - no real Redis."""

from __future__ import annotations

import pytest

pytest.importorskip("arq")

from z4j_arq import ArqEngineAdapter  # noqa: E402


class _FakePool:
    """Minimal duck-type that satisfies ``ArqRedis``-shaped checks."""

    def __init__(self):
        self._jobs = {}

    async def enqueue_job(self, *_a, **_kw):  # pragma: no cover
        return None


@pytest.fixture
def adapter():
    pool = _FakePool()
    return ArqEngineAdapter(
        redis_settings=pool,
        function_names=["myapp.send_email", "myapp.process_image"],
    )


@pytest.mark.asyncio
async def test_capabilities_honest(adapter):
    caps = adapter.capabilities()
    # v1.0: universal primitives
    assert "submit_task" in caps
    assert "cancel_task" in caps
    # native retry_task is absent - brain polyfills via submit_task
    assert "retry_task" not in caps
    assert "bulk_retry" not in caps
    assert "purge_queue" not in caps


@pytest.mark.asyncio
async def test_discover_returns_caller_supplied_functions(adapter):
    defs = await adapter.discover_tasks()
    names = {d.name for d in defs}
    assert names == {"myapp.send_email", "myapp.process_image"}
    assert all(d.engine == "arq" for d in defs)


@pytest.mark.asyncio
async def test_list_queues_empty(adapter):
    assert await adapter.list_queues() == []


@pytest.mark.asyncio
async def test_list_workers_empty(adapter):
    assert await adapter.list_workers() == []


@pytest.mark.asyncio
async def test_retry_task_clearly_unsupported(adapter):
    # Native retry_task explicitly fails - brain polyfills the
    # action via submit_task using captured args. The error text
    # documents the polyfill expectation.
    res = await adapter.retry_task("any-id")
    assert res.status == "failed"
    assert "polyfill" in res.error or "submit_task" in res.error


@pytest.mark.asyncio
async def test_unsupported_actions_return_failed(adapter):
    for coro in [
        adapter.bulk_retry({}),
        adapter.purge_queue("q"),
        adapter.requeue_dead_letter("id"),
        adapter.rate_limit("t", "5/s"),
        adapter.restart_worker("w"),
    ]:
        res = await coro
        assert res.status == "failed"


@pytest.mark.asyncio
async def test_arq_status_mapping():
    from z4j_arq.engine import _arq_status_to_canonical

    class S:
        def __init__(self, v):
            self.value = v

    assert _arq_status_to_canonical(S("deferred")) == "pending"
    assert _arq_status_to_canonical(S("queued")) == "pending"
    assert _arq_status_to_canonical(S("in_progress")) == "started"
    assert _arq_status_to_canonical(S("complete")) == "success"
    assert _arq_status_to_canonical(S("not_found")) == "unknown"
