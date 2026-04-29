"""End-to-end dispatcher integration: real arq engine + bare dispatcher."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("arq")

from z4j_bare.buffer import BufferStore  # noqa: E402
from z4j_bare.dispatcher import CommandDispatcher  # noqa: E402
from z4j_core.transport.frames import CommandFrame, CommandPayload  # noqa: E402

from z4j_arq import ArqEngineAdapter  # noqa: E402


class _RecordingPool:
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


@pytest.fixture
def buf(tmp_path: Path) -> BufferStore:
    store = BufferStore(path=tmp_path / "buf.sqlite")
    yield store
    store.close()


@pytest.mark.asyncio
async def test_schedule_fire_end_to_end_through_dispatcher(
    buf: BufferStore,
) -> None:
    pool = _RecordingPool()
    engine = ArqEngineAdapter(
        redis_settings=pool, function_names=["myapp.send_email"],
    )
    dispatcher = CommandDispatcher(
        engines={"arq": engine},
        schedulers={},
        buffer=buf,
    )

    frame = CommandFrame(
        id="cmd_e2e_arq_01",
        payload=CommandPayload(
            action="schedule.fire",
            target={},
            parameters={
                "schedule_id": "s1",
                "schedule_name": "nightly-emails",
                "task_name": "myapp.send_email",
                "engine": "arq",
                "queue": "high-priority",
                "args": ["alice@example.com"],
                "kwargs": {"template": "welcome"},
                "fire_id": "f1",
            },
        ),
        hmac="deadbeef" * 8,
    )

    await dispatcher.handle(frame)

    assert len(pool.calls) == 1
    assert pool.calls[0]["name"] == "myapp.send_email"
    assert pool.calls[0]["args"] == ("alice@example.com",)
    assert pool.calls[0]["kwargs"]["template"] == "welcome"
    assert pool.calls[0]["kwargs"]["_queue_name"] == "high-priority"

    results = [e for e in buf.drain(10) if e.kind == "command_result"]
    parsed = json.loads(results[0].payload.decode("utf-8"))
    assert parsed["payload"]["status"] == "success"
    assert parsed["payload"]["result"]["engine"] == "arq"
    assert parsed["payload"]["result"]["task_id"] == "arq-job-1"
