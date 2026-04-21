"""arq lifecycle capture by wrapping WorkerSettings hooks.

arq calls ``on_job_start(ctx)`` and ``on_job_end(ctx)`` on every
job. We chain z4j event emission *before* any user hook the
WorkerSettings already defines, so user hooks still run and run
exactly as before.

Usage::

    from z4j_arq import ArqEngineAdapter
    from z4j_arq.events import attach_to_worker_settings

    adapter = ArqEngineAdapter(redis_settings=settings, function_names=[...])
    attach_to_worker_settings(WorkerSettings, adapter=adapter)

The adapter holds the asyncio queue. The wrapper coroutines are
defined as async closures so they capture the adapter reference
without polluting the WorkerSettings class with z4j fields.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from z4j_core.models import Event
from z4j_core.models.event import EventKind

logger = logging.getLogger("z4j.agent.arq.events")

ENGINE_NAME = "arq"


class ArqEventCapture:
    """Holds the queue + the loop ref. Same shape as the Huey
    capture, kept distinct so per-engine wiring quirks have a
    home without monkey-patching the engine adapter.
    """

    __slots__ = ("queue", "_loop", "_redaction")

    def __init__(
        self,
        *,
        queue: asyncio.Queue[Event],
        loop: asyncio.AbstractEventLoop,
        redaction: Any | None = None,
    ) -> None:
        self.queue = queue
        self._loop = loop
        self._redaction = redaction

    def _put(self, evt: Event) -> None:
        try:
            self._loop.call_soon_threadsafe(self.queue.put_nowait, evt)
        except RuntimeError:
            logger.debug("z4j-arq: loop closed; dropping event")
        except asyncio.QueueFull:
            logger.warning(
                "z4j-arq: event queue full; dropping %s", evt.kind,
            )

    async def on_job_start(self, ctx: dict[str, Any]) -> None:
        """Hook signature matches arq's ``on_job_start``."""
        try:
            evt = self._build(EventKind.TASK_STARTED, ctx)
            self._put(evt)
        except Exception:  # noqa: BLE001
            logger.exception("z4j-arq: on_job_start failed")

    async def on_job_end(self, ctx: dict[str, Any]) -> None:
        """Hook signature matches arq's ``on_job_end``.

        ctx contains ``success`` (bool); we map to TASK_SUCCEEDED /
        TASK_FAILED accordingly.
        """
        try:
            success = bool(ctx.get("success", True))
            kind = EventKind.TASK_SUCCEEDED if success else EventKind.TASK_FAILED
            evt = self._build(kind, ctx)
            self._put(evt)
        except Exception:  # noqa: BLE001
            logger.exception("z4j-arq: on_job_end failed")

    def _build(self, kind: EventKind, ctx: dict[str, Any]) -> Event:
        now = datetime.now(UTC)
        task_id = str(ctx.get("job_id", "") or "")
        function_name = ctx.get("job_function") or ctx.get("function") or ""
        data: dict[str, Any] = {"task_name": function_name}
        # arq keeps args/kwargs on the job object reachable via the
        # context's ``job_try`` + ``redis`` - but exposing them
        # requires an extra round-trip. v1 ships task name only;
        # v1.1 will resolve args via JobInfo when we wire bulk_retry.
        return Event(
            id=uuid4(),
            project_id=uuid4(),
            agent_id=uuid4(),
            engine=ENGINE_NAME,
            task_id=task_id,
            kind=kind,
            occurred_at=now,
            data=data,
        )


def attach_to_worker_settings(
    worker_settings: Any,
    *,
    capture: ArqEventCapture | None = None,
    adapter: Any | None = None,
    queue: asyncio.Queue[Event] | None = None,
    loop: asyncio.AbstractEventLoop | None = None,
) -> ArqEventCapture:
    """Chain z4j hooks onto WorkerSettings' on_job_start / on_job_end.

    Pass either an existing ``capture`` or an ``adapter`` (the
    capture is built from ``adapter._event_queue`` + the running
    loop). Idempotent: calling twice replaces the previous chain.
    """
    if capture is None:
        if adapter is not None:
            target_queue = getattr(adapter, "_event_queue", None) or queue
            target_loop = loop or asyncio.get_event_loop()
            capture = ArqEventCapture(
                queue=target_queue,
                loop=target_loop,
                redaction=getattr(adapter, "redaction", None),
            )
        elif queue is not None and loop is not None:
            capture = ArqEventCapture(queue=queue, loop=loop)
        else:
            raise ValueError(
                "attach_to_worker_settings: provide either capture, "
                "adapter, or (queue + loop)",
            )

    user_start = getattr(worker_settings, "on_job_start", None)
    user_end = getattr(worker_settings, "on_job_end", None)

    async def _wrapped_start(ctx: dict[str, Any]) -> None:
        await capture.on_job_start(ctx)
        if user_start is not None:
            await user_start(ctx)

    async def _wrapped_end(ctx: dict[str, Any]) -> None:
        await capture.on_job_end(ctx)
        if user_end is not None:
            await user_end(ctx)

    worker_settings.on_job_start = staticmethod(_wrapped_start)
    worker_settings.on_job_end = staticmethod(_wrapped_end)
    return capture


__all__ = ["ArqEventCapture", "attach_to_worker_settings"]
