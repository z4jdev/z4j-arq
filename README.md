# z4j-arq

[![PyPI version](https://img.shields.io/pypi/v/z4j-arq.svg)](https://pypi.org/project/z4j-arq/)
[![Python](https://img.shields.io/pypi/pyversions/z4j-arq.svg)](https://pypi.org/project/z4j-arq/)
[![License](https://img.shields.io/pypi/l/z4j-arq.svg)](https://github.com/z4jdev/z4j-arq/blob/main/LICENSE)

The arq engine adapter for [z4j](https://z4j.com).

Streams every arq job lifecycle event from your async workers to the
z4j brain and accepts operator control actions from the dashboard.
Pair with z4j-arqcron to surface arq cron jobs.

## What it ships

| Capability | Notes |
|---|---|
| Job lifecycle events | enqueued, in-progress, complete, failed, retried |
| Job discovery | runtime function-list merge + static scan |
| Submit / retry / cancel | direct against the arq Redis pool |
| Bulk retry | filter-driven; re-enqueues matching jobs |
| Purge queue | with confirm-token guard |
| Reconcile task | via arq's Redis-backed result store |

Async-native — uses arq's existing on_job_start / on_job_end hooks.

## Install

```bash
pip install z4j-arq z4j-arqcron
```

Pair with a framework adapter (FastAPI is the most common pairing for
arq):

```bash
pip install z4j-fastapi z4j-arq z4j-arqcron
pip install z4j-bare    z4j-arq z4j-arqcron   # framework-free worker
```

## Pairs with

- [`z4j-arqcron`](https://github.com/z4jdev/z4j-arqcron) — schedule adapter for arq cron jobs

## Reliability

- No exception from the adapter ever propagates back into arq's worker
  loop or your job code.
- Events buffer locally when the brain is unreachable; workers never
  block on network I/O.

## Documentation

Full docs at [z4j.dev/engines/arq/](https://z4j.dev/engines/arq/).

## License

Apache-2.0 — see [LICENSE](LICENSE).

## Links

- Homepage: https://z4j.com
- Documentation: https://z4j.dev
- PyPI: https://pypi.org/project/z4j-arq/
- Issues: https://github.com/z4jdev/z4j-arq/issues
- Changelog: [CHANGELOG.md](CHANGELOG.md)
- Security: security@z4j.com (see [SECURITY.md](SECURITY.md))
