# Changelog

All notable changes to Pulse are documented here. This project follows the
architecture and design principles laid out in the README and `docs/ARCHITECTURE.md`.

## [1.0.1] — Render deployment fix

### Added
- `consumer/health_server.py`: a minimal, dependency-free HTTP health endpoint the
  consumer binds only when a `PORT` environment variable is present. Exists solely so
  the consumer can run on Render's free Web Service tier — Render's Background Worker
  product (the "correct" fit for a process that accepts no HTTP traffic) has no free
  tier. Fully inert under `docker-compose.yml` or a plain `python -m consumer.main`,
  where `PORT` is never set.
- `render.yaml`'s `pulse-consumer` service is now declared as `type: web` (was
  `type: worker`) with `healthCheckPath: /`, matching the above.

## [1.0.0] — Initial release

The first complete, benchmarked, documented release: a self-hosted API observability
platform with a fast-path/slow-path pipeline, hybrid anomaly detection, a live
dashboard, and webhook alerting.

### Added

**Core pipeline**
- Fast path: `POST /events`, strict Pydantic validation, `XADD` onto a Redis Stream,
  `202 Accepted`; fails closed with `503` if the buffer is unreachable.
- Slow path: a consumer-group-based `XREADGROUP` loop with crash-safe pending-entry
  drain on restart, in-memory per-minute rollup accumulation, and batched
  `ON CONFLICT DO UPDATE` upserts into Postgres (one transaction per batch).
- Fixed two-table schema (`metrics`, `anomalies`) with indexes verified against a
  realistic synthetic dataset via `EXPLAIN ANALYZE`.

**Read API**
- `GET /services`, `GET /endpoints` — paginated, filterable, additive-only summaries.
- `GET /services/{service}/metrics` — exact, never-merged per-minute percentile series.
- `GET /health` (pure liveness) and `GET /health/consumer` (Redis-sourced consumer-group
  lag, backlog, and per-consumer liveness).

**Anomaly detection**
- Hybrid detection: rolling z-score and EWMA statistical detectors plus a
  scikit-learn IsolationForest over the full multivariate feature vector, fused by
  flagging on any detector firing.
- Every finding carries a plain-language `reason`, not a bare score.

**Live dashboard and alerting**
- Single-file (`dashboard/index.html`) frontend: service health tiles, per-endpoint
  latency/error charts, a live anomaly feed, and a consumer-lag indicator.
- `GET /ws` WebSocket: one bootstrap snapshot, then incremental, sequenced,
  `schema_version`-tagged `metric_point`/`anomaly`/`lag` messages via a Redis Pub/Sub
  bridge between the consumer and API processes.
- Async, retried, idempotent webhook delivery on anomaly detection.

**Benchmarking**
- A load-generation harness (`simulator/`, `benchmark/`) that drives warmup,
  unpaced-throughput-burst, and labeled-live-traffic phases entirely through the real
  ingestion API — never seeding Postgres directly.
- Measures (never asserts) API/consumer throughput, ingest latency percentiles,
  detector precision/recall against labeled ground truth, throughput variance,
  peak consumer-group lag, and Redis/Postgres resource usage; exports both
  `benchmark_report.md` and `benchmark_report.json` from one shared data source.

**Operations and release engineering**
- `docker-compose.yml`: `postgres`, `redis`, `api`, `consumer`, plus a Compose-profile-
  gated `benchmark` service that never runs on the default `docker compose up`.
- `.github/workflows/ci.yml`: `ruff check`, `ruff format --check`, `mypy`, and the full
  pytest suite (with a `>90%` coverage gate on `detector/`) on every push.
- `scripts/seed_sample_data.py` — populates a running deployment with realistic traffic
  without running the full benchmark.
- Fully pinned dependencies (`requirements.txt`, `requirements-dev.txt`).
- Public documentation: `README.md`, `docs/ARCHITECTURE.md`, `docs/DEPLOY.md`
  (Railway/Render/Docker/local), OpenAPI request/response examples.
- MIT license.

### Fixed during release hardening

- A concurrency bug in the WebSocket live-update fan-out: a client disconnecting
  mid-broadcast could permanently crash the fan-out task (`RuntimeError: Set changed
  size during iteration`), silently breaking live updates for every connected client
  until the API process restarted.
- A non-deterministic shutdown race between the fan-out task's own cleanup and the
  Redis connection it used being closed out from under it.
- The `benchmark` Compose service's default output path never reached the host
  filesystem — the documented `docker compose --profile benchmark run --rm benchmark`
  workflow would silently lose its report the moment the container was removed.
- `core/redis_lag`'s consumer-group pending-entry count now sources from `XPENDING`'s
  canonical summary form rather than `XINFO GROUPS`' own `pending` field.
- Containers now run as a non-root user (`api`, `consumer` images).
- Several type-accuracy issues (a stale unused import, two under-typed return values,
  one mistyped ASGI callback) surfaced by wiring up `ruff`/`mypy` for the first time.

See `docs/private/ARCHITECTURE_LEDGER.md` and `docs/private/FINAL_ENGINEERING_REVIEW.md`
for the full, dated design-decision and review history behind this release.
