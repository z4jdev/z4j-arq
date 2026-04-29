"""z4j-arq CLI: ``z4j-arq doctor | check | status | version``."""

from __future__ import annotations

from z4j_bare.cli import make_engine_main

# arq is Redis-only; the broker URL is read from ``REDIS_SETTINGS``
# in arq.connections.RedisSettings, which is a Python object not
# an env var. The framework's doctor checks it via the resolved
# settings instead.
main = make_engine_main(
    "arq",
    upstream_package="arq",
    broker_env=None,
)


__all__ = ["main"]
