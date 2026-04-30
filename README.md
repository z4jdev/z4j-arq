# z4j-arq

[![PyPI version](https://img.shields.io/pypi/v/z4j-arq.svg)](https://pypi.org/project/z4j-arq/)
[![Python](https://img.shields.io/pypi/pyversions/z4j-arq.svg)](https://pypi.org/project/z4j-arq/)
[![License](https://img.shields.io/pypi/l/z4j-arq.svg)](https://github.com/z4jdev/z4j-arq/blob/main/LICENSE)

The arq engine adapter for [z4j](https://z4j.com).

Streams arq job lifecycle events to the z4j brain and accepts
control actions (retry, cancel, bulk retry, purge) from the
dashboard. Pair with z4j-arqcron to surface arq cron jobs.

## Install

```bash
pip install z4j-arq z4j-arqcron
```

## Pairs with

- [`z4j-arqcron`](https://github.com/z4jdev/z4j-arqcron) — schedule adapter for arq cron jobs

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
