# z4j-arq

[![PyPI version](https://img.shields.io/pypi/v/z4j-arq.svg)](https://pypi.org/project/z4j-arq/)
[![Python](https://img.shields.io/pypi/pyversions/z4j-arq.svg)](https://pypi.org/project/z4j-arq/)
[![License](https://img.shields.io/pypi/l/z4j-arq.svg)](https://github.com/z4jdev/z4j-arq/blob/main/LICENSE)


z4j queue-engine adapter for [arq](https://github.com/python-arq/arq).

```python
from arq.connections import RedisSettings
from z4j_arq import ArqEngineAdapter

settings = RedisSettings(host="redis", port=6379)

# In your z4j-bare bootstrap (async context):
from z4j_bare import install_agent
install_agent(
    engines=[ArqEngineAdapter(redis_settings=settings, function_names=["myapp.send_email"])],
)
```

## Capabilities

- ✅ Per-task `cancel_task` (via Redis abort key)
- ✅ Result-backend reconciliation - arq stores every job under
  `arq:result:<id>` and `arq:in-progress:<id>`; we read both to
  classify the canonical state.
- ❌ `retry_task` - arq has no native "re-enqueue this completed
  job" primitive; callers have to re-enqueue with the original
  function name + args. Deferred to v1.1.
- ❌ `bulk_retry`, `purge_queue`, `restart_worker`, `rate_limit` -
  arq exposes no remote-control surface for these.

## Cron jobs

arq's cron jobs are configured via `WorkerSettings.cron_jobs`.
Pair with `z4j-arqcron` to surface them on the Schedules page.

Apache 2.0.

## License

Apache 2.0 - see [LICENSE](LICENSE). This package is deliberately permissively licensed so that proprietary Django / Flask / FastAPI applications can import it without any license concerns.

## Links

- Homepage: <https://z4j.com>
- Documentation: <https://z4j.dev>
- Source: <https://github.com/z4jdev/z4j-arq>
- Issues: <https://github.com/z4jdev/z4j-arq/issues>
- Changelog: [CHANGELOG.md](CHANGELOG.md)
- Security: `security@z4j.com` (see [SECURITY.md](SECURITY.md))
