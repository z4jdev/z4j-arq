"""Capability tokens for the arq engine adapter."""

from __future__ import annotations

DEFAULT_CAPABILITIES: frozenset[str] = frozenset(
    {
        "submit_task",
        "cancel_task",
    },
)
"""Actions implemented by :class:`z4j_arq.engine.ArqEngineAdapter`.

Honest absences:

- ``retry_task`` - arq has no "re-enqueue an existing completed
  job by id" primitive. Re-enqueueing requires the function name +
  args, which the brain knows but the adapter would have to be
  passed explicitly. Deferred to v1.1.
- ``bulk_retry`` / ``purge_queue`` / ``requeue_dead_letter`` -
  arq exposes no enumeration / DLQ surface.
- ``restart_worker`` / ``rate_limit`` - arq workers expose no
  remote-control channel.
"""


__all__ = ["DEFAULT_CAPABILITIES"]
