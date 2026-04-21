"""The :class:`ArqEngineAdapter` - z4j's arq queue engine adapter.

Implements :class:`z4j_core.protocols.QueueEngineAdapter` against
an arq Redis pool. Wires up:

- Task discovery from a caller-supplied ``function_names`` list
  (arq decorators don't auto-register into a discoverable registry).
- ``get_task`` + ``reconcile_task`` via ``arq.jobs.Job.status()`` /
  ``Job.result_info()``.
- ``cancel_task`` via ``Job.abort()``.

v0 scope: discovery + cancel + reconciliation. Event streaming,
retry-by-id, and bulk operations are deferred to v1.1 (see
``capabilities.py`` for the honest non-support list).
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Iterable
from typing import Any

from z4j_core.models import (
    CommandResult,
    DiscoveryHints,
    Event,
    Queue,
    Task,
    TaskDefinition,
    TaskRegistryDelta,
    Worker,
)
from z4j_core.redaction.engine import RedactionEngine
from z4j_core.version import PROTOCOL_VERSION

from z4j_arq.capabilities import DEFAULT_CAPABILITIES

logger = logging.getLogger("z4j.agent.arq.engine")

ENGINE_NAME = "arq"


class ArqEngineAdapter:
    """Queue-engine adapter for arq.

    Args:
        redis_settings: An ``arq.connections.RedisSettings`` instance
                        OR an already-created ``arq.connections.ArqRedis``
                        pool. The adapter accepts either; the pool is
                        opened lazily on first use when settings are
                        passed.
        function_names: Iterable of fully-qualified function names
                        the user has registered with arq (the
                        ``WorkerSettings.functions`` list). arq
                        decorators don't auto-register into a
                        discoverable global, so the caller has to
                        tell us what's there.
        queue_name: arq queue name (defaults to ``"arq:queue"``).
        redaction: Shared :class:`RedactionEngine`.
    """

    name: str = ENGINE_NAME
    protocol_version: str = PROTOCOL_VERSION

    def __init__(
        self,
        *,
        redis_settings: Any,
        function_names: Iterable[str] = (),
        queue_name: str = "arq:queue",
        redaction: RedactionEngine | None = None,
    ) -> None:
        self._redis_settings = redis_settings
        self._function_names: list[str] = list(function_names)
        self._queue_name = queue_name
        self.redaction = redaction or RedactionEngine()
        self._pool: Any = None
        # Event queue populated by ArqEventCapture's WorkerSettings
        # hooks. Drained by ``subscribe_events``.
        import asyncio as _aio

        self._event_queue: _aio.Queue[Event] = _aio.Queue(maxsize=10_000)

    async def _get_pool(self) -> Any:
        """Return a live ``ArqRedis`` pool, opening one if needed."""
        if self._pool is not None:
            return self._pool
        # Already-created pool path - duck-typed via ``.enqueue_job``.
        if hasattr(self._redis_settings, "enqueue_job"):
            self._pool = self._redis_settings
            return self._pool
        from arq.connections import create_pool

        self._pool = await create_pool(self._redis_settings)
        return self._pool

    # ------------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------------

    async def discover_tasks(
        self,
        hints: DiscoveryHints | None = None,  # noqa: ARG002
    ) -> list[TaskDefinition]:
        """Return one TaskDefinition per registered function.

        arq has no auto-registry - the caller must pass the names
        when constructing the adapter (mirrors what they pass to
        ``WorkerSettings.functions``).
        """
        return [
            TaskDefinition(
                engine=self.name,
                name=fn_name,
                queue=self._queue_name,
            )
            for fn_name in self._function_names
        ]

    async def subscribe_registry_changes(
        self,
    ) -> AsyncIterator[TaskRegistryDelta]:
        """No native change signal - registry is constructor-defined."""
        if False:  # pragma: no cover
            yield  # type: ignore[unreachable]
        return

    # ------------------------------------------------------------------
    # Observation
    # ------------------------------------------------------------------

    async def subscribe_events(self) -> AsyncIterator[Event]:
        """Drain the internal event queue populated by
        ``z4j_arq.events.attach_to_worker_settings``.

        Until the user wires the capture into their WorkerSettings,
        this stream is empty (no hooks fire). The agent runtime
        calls ``subscribe_events`` regardless and just sees no
        events flow.
        """
        while True:
            evt = await self._event_queue.get()
            yield evt

    async def list_queues(self) -> list[Queue]:
        """v1 leaves this empty - brain synthesizes from events."""
        return []

    async def list_workers(self) -> list[Worker]:
        """arq's workers don't post heartbeats to a queryable index."""
        return []

    async def get_task(self, task_id: str) -> Task | None:
        from arq.jobs import Job, JobStatus

        try:
            pool = await self._get_pool()
            job = Job(task_id, pool, _queue_name=self._queue_name)
            status = await job.status()
        except Exception:  # noqa: BLE001
            return None
        if status == JobStatus.not_found:
            return None
        return Task(
            engine=self.name,
            task_id=task_id,
            name="",
            state=_arq_status_to_canonical(status),
        )

    async def reconcile_task(self, task_id: str) -> CommandResult:
        """Read arq's Redis-backed job state for ``task_id``.

        arq exposes a clean status enum - perfect for reconciliation.
        Maps ``deferred`` / ``queued`` → ``"pending"``,
        ``in_progress`` → ``"started"``, ``complete`` → ``"success"``
        (or ``"failure"`` if ``result_info().success is False``),
        ``not_found`` → ``"unknown"``.
        """
        from arq.jobs import Job, JobStatus

        try:
            pool = await self._get_pool()
            job = Job(task_id, pool, _queue_name=self._queue_name)
            status = await job.status()
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "z4j arq reconcile: status read failed", exc_info=exc,
            )
            return CommandResult(
                status="success",
                result={
                    "task_id": task_id,
                    "engine_state": "unknown",
                    "finished_at": None,
                    "exception": None,
                },
            )

        if status == JobStatus.not_found:
            return CommandResult(
                status="success",
                result={
                    "task_id": task_id,
                    "engine_state": "unknown",
                    "finished_at": None,
                    "exception": None,
                },
            )

        engine_state = _arq_status_to_canonical(status)
        finished_at: str | None = None
        exception_text: str | None = None
        if status == JobStatus.complete:
            try:
                info = await job.result_info()
            except Exception:  # noqa: BLE001
                info = None
            if info is not None:
                finished_at = (
                    info.finish_time.isoformat()
                    if getattr(info, "finish_time", None)
                    else None
                )
                if not getattr(info, "success", True):
                    engine_state = "failure"
                    exc = getattr(info, "result", None)
                    if isinstance(exc, BaseException):
                        exception_text = f"{type(exc).__name__}: {exc}"
        return CommandResult(
            status="success",
            result={
                "task_id": task_id,
                "engine_state": engine_state,
                "finished_at": finished_at,
                "exception": exception_text,
            },
        )

    # ------------------------------------------------------------------
    # Actions
    # ------------------------------------------------------------------

    async def submit_task(
        self,
        name: str,
        *,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        queue: str | None = None,
        eta: float | None = None,
        priority: int | None = None,  # noqa: ARG002 - arq doesn't expose priority
    ) -> CommandResult:
        """Enqueue an arq job by function name + args.

        ``eta`` is forwarded as ``_defer_until`` (arq's deferred-job
        primitive). ``queue`` overrides the adapter's default queue
        for this one job.
        """
        from datetime import UTC, datetime, timedelta

        try:
            pool = await self._get_pool()
            extra: dict[str, Any] = {}
            if queue:
                extra["_queue_name"] = queue
            if eta is not None:
                extra["_defer_until"] = datetime.now(UTC) + timedelta(seconds=eta)
            job = await pool.enqueue_job(name, *args, **(kwargs or {}), **extra)
            new_id = getattr(job, "job_id", None) or getattr(job, "id", None)
        except Exception as exc:  # noqa: BLE001
            return CommandResult(status="failed", error=str(exc))
        return CommandResult(
            status="success",
            result={"task_id": new_id, "engine": self.name},
        )

    async def retry_task(
        self,
        task_id: str,  # noqa: ARG002
        *,
        override_args: tuple[Any, ...] | None = None,  # noqa: ARG002
        override_kwargs: dict[str, Any] | None = None,  # noqa: ARG002
        eta: float | None = None,  # noqa: ARG002
        priority: int | None = None,  # noqa: ARG002
    ) -> CommandResult:
        # Native retry-by-id is impossible in arq (the original args
        # aren't kept after completion). The brain polyfills retry
        # via ``submit_task`` with the args it already captured on
        # task.received.
        return CommandResult(
            status="failed",
            error=(
                "z4j-arq has no native retry_task; the brain polyfills "
                "retry by calling submit_task with the original args "
                "from its tasks table"
            ),
        )

    async def cancel_task(self, task_id: str) -> CommandResult:
        from arq.jobs import Job

        try:
            pool = await self._get_pool()
            job = Job(task_id, pool, _queue_name=self._queue_name)
            ok = await job.abort()
        except Exception as exc:  # noqa: BLE001
            return CommandResult(status="failed", error=str(exc))
        return CommandResult(
            status="success",
            result={"task_id": task_id, "aborted": bool(ok)},
        )

    async def bulk_retry(
        self, filter: dict[str, Any], *, max: int = 1000,  # noqa: A002, ARG002
    ) -> CommandResult:
        return CommandResult(
            status="failed",
            error="bulk_retry not implemented in z4j-arq v1",
        )

    async def purge_queue(
        self,
        queue_name: str,  # noqa: ARG002
        *,
        confirm_token: str | None = None,  # noqa: ARG002
        force: bool = False,  # noqa: ARG002
    ) -> CommandResult:
        return CommandResult(
            status="failed",
            error="purge_queue not implemented in z4j-arq v1",
        )

    async def requeue_dead_letter(self, task_id: str) -> CommandResult:  # noqa: ARG002
        return CommandResult(
            status="failed",
            error="arq has no dead-letter concept",
        )

    async def rate_limit(
        self,
        task_name: str,  # noqa: ARG002
        rate: str,  # noqa: ARG002
        *,
        worker_name: str | None = None,  # noqa: ARG002
    ) -> CommandResult:
        return CommandResult(
            status="failed",
            error="rate_limit not supported by arq",
        )

    async def restart_worker(self, worker_id: str) -> CommandResult:  # noqa: ARG002
        return CommandResult(
            status="failed",
            error="arq workers expose no remote restart",
        )

    # ------------------------------------------------------------------
    # Capabilities
    # ------------------------------------------------------------------

    def capabilities(self) -> set[str]:
        return set(DEFAULT_CAPABILITIES)


def _arq_status_to_canonical(status: Any) -> str:
    """Map ``arq.jobs.JobStatus`` to the canonical z4j state set."""
    s = str(status.value if hasattr(status, "value") else status)
    return {
        "deferred": "pending",
        "queued": "pending",
        "in_progress": "started",
        "complete": "success",
        "not_found": "unknown",
    }.get(s, "unknown")


__all__ = ["ENGINE_NAME", "ArqEngineAdapter"]
