"""Tests for ``ArqEngineAdapter.submit_task``.

The bare-agent dispatcher's v1.1.0 ``schedule.fire`` path routes
brain-side scheduler ticks to ``engine.submit_task(...)``. arq is the
brain-side scheduler's only viable async-Python engine pairing for
FastAPI / async stacks, so this contract MUST hold.

Pinned behaviour:
- ``submit_task`` is in ``capabilities()`` (so the dispatcher accepts it).
- The adapter calls ``pool.enqueue_job(name, *args, **kwargs)`` and
  returns the resulting ``job_id`` as ``result.task_id``.
- ``queue=`` translates to arq's ``_queue_name`` keyword.
- ``eta=`` (epoch seconds) translates to arq's ``_defer_until``
  (timezone-aware datetime).
- A pool-side exception becomes ``CommandResult(status="failed")``
  rather than bubbling out of the dispatcher loop.
"""

from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("arq")

from z4j_arq import ArqEngineAdapter  # noqa: E402


class _RecordingPool:
    """Duck-types the bits of ``ArqRedis`` the adapter touches."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def enqueue_job(self, name, *args, **kwargs):  # noqa: ANN001
        self.calls.append({
            "name": name,
            "args": tuple(args),
            "kwargs": dict(kwargs),
        })

        class _Job:
            job_id = f"arq-job-{len(self.calls)}"

        return _Job()


class _ExplodingPool:
    async def enqueue_job(self, *_a, **_k):  # noqa: ANN001
        raise RuntimeError("redis unreachable")


@pytest.fixture
def pool() -> _RecordingPool:
    return _RecordingPool()


@pytest.fixture
def adapter(pool: _RecordingPool) -> ArqEngineAdapter:
    return ArqEngineAdapter(
        redis_settings=pool,
        function_names=["myapp.send_email", "myapp.process_image"],
    )


@pytest.mark.asyncio
class TestSubmitTask:
    async def test_capability_advertised(self, adapter) -> None:
        assert "submit_task" in adapter.capabilities()

    async def test_basic_enqueue_routes_to_pool(
        self, adapter, pool,
    ) -> None:
        result = await adapter.submit_task(
            "myapp.send_email",
            args=("alice@example.com",),
            kwargs={"template": "welcome"},
        )
        assert result.status == "success"
        assert result.result["engine"] == "arq"
        assert result.result["task_id"] == "arq-job-1"
        assert pool.calls == [
            {
                "name": "myapp.send_email",
                "args": ("alice@example.com",),
                "kwargs": {"template": "welcome"},
            },
        ]

    async def test_queue_kwarg_translates_to_arq_queue_name(
        self, adapter, pool,
    ) -> None:
        await adapter.submit_task(
            "myapp.process_image",
            args=(),
            kwargs={"path": "/tmp/x"},
            queue="batch",
        )
        assert pool.calls[-1]["kwargs"]["_queue_name"] == "batch"
        assert pool.calls[-1]["kwargs"]["path"] == "/tmp/x"

    async def test_eta_translates_to_defer_until(
        self, adapter, pool,
    ) -> None:
        from datetime import UTC, datetime, timedelta

        before = datetime.now(UTC)
        await adapter.submit_task(
            "myapp.send_email",
            args=(),
            kwargs={},
            eta=60.0,  # 60s deferral
        )
        after = datetime.now(UTC)
        defer = pool.calls[-1]["kwargs"]["_defer_until"]
        # _defer_until = now + 60s, so it should sit in
        # [before+60s, after+60s].
        assert before + timedelta(seconds=59) <= defer <= after + timedelta(seconds=61)

    async def test_pool_exception_returns_failed(self) -> None:
        adapter = ArqEngineAdapter(
            redis_settings=_ExplodingPool(),
            function_names=["x"],
        )
        result = await adapter.submit_task("x")
        assert result.status == "failed"
        assert "redis unreachable" in (result.error or "")
