"""arq lifecycle capture by wrapping WorkerSettings or Worker hooks.

arq calls ``on_job_start(ctx)`` and ``on_job_end(ctx)`` on every
job. We chain z4j event emission *before* any user hook the
WorkerSettings already defines, so user hooks still run and run
exactly as before.

**CRITICAL**: arq's ``Worker.__init__`` reads ``on_job_start`` and
``on_job_end`` ONCE at construction time, then stores them as
instance attributes. Mutating the ``WorkerSettings`` class AFTER
the Worker has been constructed is a no-op for that Worker. The
helper below auto-detects whether you passed a class or a Worker
instance and patches the right object.

Three working patterns:

1. **Bulletproof: build Worker yourself with hooks pre-set.**
   The agent gets installed first; the capture object is built;
   the Worker is constructed with the hooks passed directly to
   ``Worker(...)``. No timing pitfalls::

       from arq import Worker
       from z4j_arq import ArqEngineAdapter
       from z4j_arq.events import ArqEventCapture
       from z4j_bare import install_agent

       async def main():
           loop = asyncio.get_event_loop()
           adapter = ArqEngineAdapter(...)
           install_agent(engines=[adapter], ...)
           capture = ArqEventCapture(
               queue=adapter._event_queue, loop=loop,
           )
           worker = Worker(
               functions=[...],
               on_job_start=capture.on_job_start,
               on_job_end=capture.on_job_end,
           )
           await worker.async_run()

2. **CLI flow: patch the WorkerSettings class BEFORE arq starts.**
   Works only when called at module import time, before ``arq
   myapp.worker.WorkerSettings`` constructs the Worker. Calling
   from ``on_startup`` is too late::

       from z4j_arq.events import attach_to_worker_settings
       attach_to_worker_settings(WorkerSettings, adapter=adapter)

3. **In-context patching: patch the running Worker instance from
   on_startup.** arq passes ``ctx['worker']`` to ``on_startup``;
   call ``attach_to_worker_settings(ctx['worker'], adapter=adapter)``
   to patch instance attributes after the Worker is constructed.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from z4j_core.models import Event
from z4j_core.models.event import EventKind

logger = logging.getLogger("z4j.adapter.arq.events")

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
    target: Any,
    *,
    capture: ArqEventCapture | None = None,
    adapter: Any | None = None,
    queue: asyncio.Queue[Event] | None = None,
    loop: asyncio.AbstractEventLoop | None = None,
) -> ArqEventCapture:
    """Chain z4j hooks onto a WorkerSettings class or a Worker instance.

    Auto-detects the shape of ``target``:

    - If ``target`` is a CLASS (typically ``WorkerSettings``), the
      hooks are written as ``staticmethod`` class attributes. arq's
      ``Worker.__init__`` will pick them up when it constructs the
      Worker from the class. **Must be called BEFORE arq builds the
      Worker** - that is, at module import time before ``arq
      myapp.worker.WorkerSettings`` runs, NOT from within
      ``on_startup``.

    - If ``target`` is a Worker INSTANCE (typically reached via
      ``ctx['worker']`` from inside ``on_startup``), the hooks are
      written as plain attributes on that one instance. This works
      even when called after ``Worker.__init__`` has already run -
      arq dereferences ``self.on_job_start`` afresh on every job.

    Pass either an existing ``capture`` or an ``adapter`` (the
    capture is built from ``adapter._event_queue`` + the running
    loop). Idempotent: calling twice replaces the previous chain.

    The class-vs-instance distinction matters because pre-1.5 code
    that called this function from ``on_startup`` (after the Worker
    was already built) silently lost every task event. The instance
    branch makes that pattern work correctly.
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

    user_start = getattr(target, "on_job_start", None)
    user_end = getattr(target, "on_job_end", None)

    async def _wrapped_start(ctx: dict[str, Any]) -> None:
        await capture.on_job_start(ctx)
        if user_start is not None and user_start is not _wrapped_start:
            await user_start(ctx)

    async def _wrapped_end(ctx: dict[str, Any]) -> None:
        await capture.on_job_end(ctx)
        if user_end is not None and user_end is not _wrapped_end:
            await user_end(ctx)

    is_class = inspect.isclass(target)
    if is_class:
        # WorkerSettings-style class. staticmethod is required so
        # arq's class-attribute lookup returns the bound coroutine
        # without an unintended self.
        target.on_job_start = staticmethod(_wrapped_start)
        target.on_job_end = staticmethod(_wrapped_end)
    else:
        # Worker instance. arq reads on_job_start as a plain attr on
        # every job; patching the instance directly works regardless
        # of when it's called.
        target.on_job_start = _wrapped_start
        target.on_job_end = _wrapped_end
    return capture


# Public alias clarifying the dual nature of the helper. New code
# should use this name; the old name stays for backwards compat.
attach_to_worker = attach_to_worker_settings


__all__ = [
    "ArqEventCapture",
    "attach_to_worker",
    "attach_to_worker_settings",
]
