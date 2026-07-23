# Pulse

[![CI](https://github.com/hardikOG/pulse/actions/workflows/ci.yml/badge.svg)](https://github.com/hardikOG/pulse/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/docker-compose-2496ED?logo=docker&logoColor=white)](https://docs.docker.com/compose/)
[![Coverage: detector 100%](https://img.shields.io/badge/coverage-detector%20100%25-brightgreen)](#testing)
[![License: MIT](https://img.shields.io/badge/license-MIT-lightgrey)](LICENSE)

Pulse is a self-hosted API observability platform: services POST request events to it,
and it turns that stream into per-endpoint latency/error metrics, hybrid statistical +
ML anomaly detection, a live dashboard, and webhook alerts — all running from a single
`docker compose up`, with no external SaaS dependency.

It is built around a **fast path / slow path split**: an API process that does nothing
but validate and buffer events as fast as possible, and a separate consumer process
that does all the expensive work (aggregation, persistence, detection) off the request
path, connected by Redis Streams.

## Contents

- [Architecture](#architecture)
- [Why these choices](#why-these-choices)
- [Features](#features)
- [Screenshots](#screenshots)
- [Installation](#installation)
- [Quickstart](#quickstart)
- [Docker usage](#docker-usage)
- [Environment variables](#environment-variables)
- [API reference](#api-reference)
- [Dashboard](#dashboard)
- [Benchmark results](#benchmark-results)
- [Design decisions](#design-decisions)
- [Repository structure](#repository-structure)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)
- [Future improvements](#future-improvements)
- [License](#license)

## Architecture

```
                         FAST PATH (api process)
                         ────────────────────────
  client service  ──POST /events──▶  FastAPI  ──validate──▶  XADD  ──▶  Redis Stream
                                        │                                    │
                                        └── 202 Accepted (buffered)          │
                                                                              │
                         ────────────────────────────────────────────────────┘
                         SLOW PATH (consumer process)
                         ────────────────────────
                         XREADGROUP (consumer group, batched)
                                 │
                                 ▼
                   in-memory per-minute rollup accumulation
                                 │
                                 ▼
                   Postgres UPSERT (per-batch transaction) ── metrics table
                                 │
                                 ▼
                   hybrid anomaly detection (zscore + EWMA + IsolationForest)
                                 │
                        ┌────────┴────────┐
                        ▼                 ▼
                  anomalies table    Redis Pub/Sub ──▶ WebSocket ──▶ dashboard
                        │
                        ▼
                  webhook delivery (async, retried, idempotent)

  read API (GET /services, /endpoints, /services/{s}/metrics) ──▶ Postgres
```

### Fast path

`POST /events` does exactly one thing beyond validation: `XADD` the event onto a Redis
Stream and return `202 Accepted`. No database write, no aggregation, no detection logic
runs on the request path — the entire handler is O(1) and depends on nothing but Redis
being reachable. If Redis is unavailable, the request fails closed with `503` rather
than silently dropping the event.

### Slow path

A separate `consumer` process reads the stream via a **consumer group**
(`XREADGROUP`), which gives at-least-once delivery: unacknowledged entries are
redelivered on restart, so a consumer crash never silently loses in-flight work. Each
batch is folded into in-memory per-`(minute, service, endpoint)` accumulators, upserted
into Postgres as a single transaction per batch, then run through anomaly detection —
all before the batch is `XACK`ed. Live-update messages and webhook alerts are
best-effort and never block acking metrics data.

## Why these choices

**Why Redis Streams (not Kafka, not a plain queue).** Streams give consumer groups,
per-consumer pending-entry tracking, and replay — the same durability/replay
guarantees Kafka is usually reached for — without adding a JVM-based broker, Zookeeper,
or a second piece of infrastructure to operate for a project this size. A plain
list-based queue (`LPUSH`/`BRPOP`) would lose the ability to track *which* consumer has
which entries pending, which is what makes crash recovery correct rather than
best-effort.

**Why Postgres (not a time-series DB).** The data model is two small, well-indexed
tables with a handful of read patterns (range scans by service/time, `GROUP BY`
aggregates) — nothing here needs a purpose-built TSDB's compression or downsampling
machinery. Postgres also makes exact percentiles trivial to store correctly (see
below) and is something almost every backend team already knows how to operate.

**Why FastAPI.** Async-native (matters directly for the fast path's Redis I/O),
automatic request validation via Pydantic (the entire trust boundary for untrusted
event data), and free OpenAPI docs generation. The one deliberate departure from
FastAPI defaults: request logging is implemented as a raw ASGI middleware rather than
`BaseHTTPMiddleware`, because `BaseHTTPMiddleware` measurably adds latency to
POST-with-body requests (it buffers the body through an internal stream) — caught by
benchmarking the fast path directly, not assumed.

**Why hybrid (statistical + ML) anomaly detection, not just one.** Two rolling
statistical detectors (z-score and EWMA) catch sudden, large deviations from a recent
baseline cheaply and explainably. IsolationForest, run over the multivariate feature
set (request count, error rate, p50/p95/p99 together), catches the anomalies that only
show up as an unusual *combination* of features rather than any single metric spiking
on its own — the two approaches are fused by flagging on **either** firing, and the
`reason` field always states which one triggered and why, so no alert is a black box.

**Why exact, never-merged percentiles.** Percentiles are not associative: averaging or
summing per-bucket p95 values does not produce a mathematically valid combined p95.
Every place Pulse reports a percentile, it is computed once, directly from the raw
sample, and never re-derived by combining other percentiles — the read API's
list/summary endpoints deliberately report only additive fields (request/error counts)
for exactly this reason, and only the per-minute, per-endpoint series exposes p50/p95/p99.

## Features

- **Sub-millisecond-logic fast path** — event validation and buffering with no
  synchronous database or ML work in the request path.
- **At-least-once ingestion** with automatic crash recovery — a killed consumer resumes
  exactly where it left off via Redis consumer-group pending-entry replay.
- **Hybrid anomaly detection** — rolling z-score, EWMA, and IsolationForest fused
  together, each finding explained in plain language.
- **Live dashboard** — service health tiles, per-endpoint latency/error charts, and a
  live anomaly feed, updated over WebSocket with no polling.
- **Webhook alerting** — async, retried with backoff, deduplicated by anomaly ID.
- **Operational visibility** — consumer-group lag/backlog and per-consumer liveness
  exposed via `/health/consumer`, and folded into the dashboard and benchmark report.
- **Honest benchmarking** — a real load-generation harness that measures (never
  asserts) throughput, latency percentiles, detector precision/recall against labeled
  ground truth, and resource usage, exporting both a Markdown and a JSON report.
- **Runs anywhere Docker does** — one `docker compose up`, no managed cloud services
  required.

## Screenshots

<!-- TODO: add a dashboard screenshot, e.g. docs/img/dashboard.png -->
<!-- TODO: add a short GIF showing a live anomaly arriving on the dashboard -->

## Installation

Requirements: [Docker](https://docs.docker.com/get-docker/) and Docker Compose (bundled
with Docker Desktop). No local Python installation is required to run Pulse — it's only
needed for local development against the source directly.

```bash
git clone https://github.com/hardikOG/pulse.git
cd pulse
cp .env.example .env
docker compose up --build
```

Once every service reports healthy:

```bash
curl http://localhost:8000/health
# {"status":"ok"}
```

## Quickstart

Send a real event through the fast path:

```bash
curl -X POST http://localhost:8000/events \
  -H "Content-Type: application/json" \
  -d '{
    "service": "checkout",
    "endpoint": "/api/pay",
    "status_code": 200,
    "latency_ms": 42.5,
    "ts": "2026-01-01T12:00:00Z"
  }'
```

Open the dashboard at [http://localhost:8000/](http://localhost:8000/) — it will be
empty until the consumer has aggregated at least one minute bucket. To populate it
quickly with realistic traffic (services, endpoints, a warmup baseline, and a short
burst of live traffic with a couple of labeled anomalies) instead of sending events by
hand:

```bash
pip install -r requirements.txt
python scripts/seed_sample_data.py
```

## Docker usage

`docker compose up --build` starts four services: `postgres`, `redis`, `api`
(port `8000`), and `consumer`. Each has a healthcheck, and `api`/`consumer` both wait
on `postgres`/`redis` reporting healthy before starting.

A fifth service, `benchmark`, is deliberately excluded from the default `up` — it runs
a multi-minute load test and should never fire accidentally. Run it explicitly via its
Compose profile:

```bash
docker compose --profile benchmark run --rm benchmark
```

This writes `benchmark_report.md` and `benchmark_report.json` to the repo root (see
[Benchmark results](#benchmark-results)).

To tear everything down (keeping the Postgres volume):

```bash
docker compose down
```

Add `-v` to also drop the Postgres volume and start fully fresh.

## Environment variables

All configuration is environment-driven (`core/config.py`); every value has a sane
local default, and `.env.example` documents each one. The full list:

| Variable | Default | Purpose |
|---|---|---|
| `DATABASE_URL` | `postgresql+psycopg://pulse:pulse@postgres:5432/pulse` | Postgres connection string |
| `REDIS_URL` | `redis://redis:6379/0` | Redis connection string |
| `EVENT_STREAM` | `pulse:events` | Redis Stream key for incoming events |
| `CONSUMER_GROUP` | `pulse-consumers` | Redis consumer-group name |
| `CONSUMER_NAME` | `consumer-1` | This consumer instance's name within the group |
| `STREAM_BATCH_SIZE` | `200` | Max entries read per `XREADGROUP` call |
| `STREAM_BLOCK_TIMEOUT_MS` | `5000` | `XREADGROUP` block timeout |
| `REDIS_SOCKET_TIMEOUT_SECONDS` | `2.0` | Redis client socket timeout |
| `AGGREGATION_WINDOW_SECONDS` | `60` | Rollup bucket width (fixed at one minute by design) |
| `BUCKET_EVICTION_GRACE_SECONDS` | `120` | How long a bucket stays in memory after its window closes |
| `POSTGRES_MAX_RETRIES` | `3` | Batch upsert retry count |
| `POSTGRES_RETRY_BACKOFF_SECONDS` | `1.0` | Linear backoff base between upsert retries |
| `ZSCORE_THRESHOLD` | `3.0` | Standard-deviation threshold for the z-score detector |
| `EWMA_ALPHA` | `0.3` | EWMA smoothing factor |
| `ISOLATION_FOREST_CONTAMINATION` | `0.05` | Expected outlier fraction passed to IsolationForest |
| `MIN_HISTORY_BUCKETS` | `10` | Minimum history required before the statistical detectors evaluate |
| `ISOLATION_FOREST_MIN_HISTORY` | `30` | Minimum history required before IsolationForest evaluates (higher — see [Design decisions](#design-decisions)) |
| `WEBHOOK_URL` | *(unset)* | Optional URL to POST anomaly alerts to; alerting is disabled if unset |
| `WEBHOOK_MAX_RETRIES` | `3` | Webhook delivery retry count |
| `WEBHOOK_BACKOFF_BASE_SECONDS` | `1.0` | Exponential backoff base between webhook retries |
| `WEBHOOK_TIMEOUT_SECONDS` | `5.0` | Per-attempt webhook HTTP timeout |
| `LIVE_UPDATES_CHANNEL` | `pulse:live` | Redis Pub/Sub channel bridging the consumer to dashboard WebSocket clients |
| `LOG_LEVEL` | `INFO` | Structured logger level for both processes |

## API reference

Full interactive documentation (with request/response examples) is generated
automatically and served at `/docs` (Swagger UI) and `/redoc` while the API is running.

### `POST /events`

Validate and buffer one request event onto the fast path.

**Request body:**

```json
{
  "service": "checkout",
  "endpoint": "/api/pay",
  "status_code": 200,
  "latency_ms": 42.5,
  "ts": "2026-01-01T12:00:00Z"
}
```

**Response — `202 Accepted`:**

```json
{"status": "accepted", "stream_id": "1735732800000-0"}
```

**Errors:**
- `422` — validation failed (missing/mistyped field, `status_code` outside 100–599,
  negative `latency_ms`, blank `service`/`endpoint`, or `ts` more than a minute in the
  future).
- `503` — the Redis stream buffer is unreachable; the event was **not** silently
  dropped or accepted without being durably buffered.

### `GET /services`

List services with additive request/error totals over a time range.

**Query params:** `start`, `end` (ISO datetimes, default: last 60 minutes),
`status_class` (`all` | `healthy` | `error`), `limit` (1–100, default 20), `offset`.

**Response:**

```json
{
  "items": [
    {"service": "checkout", "request_count": 5230, "error_count": 12, "error_rate": 0.0023, "last_seen": "2026-01-01T12:34:00Z"}
  ],
  "pagination": {"total": 1, "limit": 20, "offset": 0}
}
```

No percentile field appears here — see [Design decisions](#design-decisions) for why.

### `GET /endpoints`

Same shape as `/services`, one row per `(service, endpoint)` pair. Accepts an
additional optional `service` filter.

### `GET /services/{service}/metrics`

One service's endpoints, each with its own exact per-minute series (the only place
percentiles are exposed, unmerged, one row per minute per endpoint).

**Query params:** `start`, `end`.

**Errors:** `404` if the service has no rows in the requested range.

### `GET /health`

Pure liveness probe — `{"status": "ok"}`, always `200` as long as the process is up.
Does not check Redis/Postgres connectivity by design (a Docker healthcheck target
should be fast and independent of downstream dependencies).

### `GET /health/consumer`

Operational visibility into the slow path, using the API's own Redis client — stream
backlog/lag and per-consumer idle time, so a total consumer outage is distinguishable
from "nothing has been produced recently" (zero lag alone can't tell the two apart).
Reports raw measured numbers, not a computed up/down verdict — consistent with this
project's stance that health/performance numbers are always reported, never asserted.

```json
{
  "stream_length": 42,
  "pending_count": 3,
  "lag": 7,
  "consumers": [{"name": "consumer-1", "pending": 3, "idle_ms": 120}]
}
```

### `GET /ws`

WebSocket. Sends one `{"type": "snapshot", ...}` bootstrap message on connect (current
service tiles), then incremental `metric_point` / `anomaly` / `lag` messages as they
happen. Every message carries a `schema_version` field so a client can detect a
breaking protocol change instead of silently misinterpreting a differently-shaped
message.

## Dashboard

A single-file (`dashboard/index.html`), no-build-step frontend served at `/`. Left
panel: service health tiles (color-coded by error rate), updated live. Right panel: a
live anomaly feed and per-endpoint p95 latency charts (Chart.js) for whichever service
is selected. A queue-health indicator in the header shows live consumer-group pending
count and lag, sourced from the same `/health/consumer` data via the WebSocket `lag`
message.

## Benchmark results

Measured by running `docker compose --profile benchmark run --rm benchmark`, which
drives real traffic through the full stack (never seeds Postgres directly) and reports
what actually happened — see `benchmark_report.md` / `benchmark_report.json` in the
repo root for the full, current report, or re-run it yourself. Numbers below are from
the committed run; **these are measured, not targets** — see
[Design decisions](#design-decisions).

**Environment:** Linux 6.18.33.2-microsoft-standard-WSL2 (inside the Docker network),
Python 3.11.15, 12 CPU cores, 7793 MB total memory.

**Pipeline throughput** (unpaced concurrent burst, 50-way concurrency, 15.1s):
- API ingest (= Redis `XADD` completion): **540.3 events/sec**
- Consumer processed (durably aggregated into Postgres): **540.3 events/sec** — matches
  API ingest exactly at this load, i.e. no measurable backpressure.
- Per-second throughput: mean 542.7, stddev 31.9, min 460, max 591.
- Ingest latency: avg 92.28ms, p50 62.24ms, **p95 267.58ms**, p99 428.89ms (n=8140).

**Labeled live phase** (steady scenario, 240s, realistic paced traffic):
- Ingest latency: avg 2.54ms, p50 2.48ms, p95 3.04ms, p99 3.32ms (n=4542) — an order of
  magnitude lower than the unpaced burst, since it isn't saturating the pipeline.

**Detector precision/recall** (against labeled ground-truth anomalies):
- True positives: 3, false positives: 21, false negatives: 0
- **Precision: 12.5%, Recall: 100.0%**

Recall is perfect: every deliberately injected anomaly was caught. Precision is low —
this is a real, reproducible finding tied to low-request-volume endpoints in the test
scenario producing noisy statistical baselines, not a bug (see
[Design decisions](#design-decisions) and the future-improvements list below for the
concrete fix: an absolute-threshold detector alongside the adaptive baselines).

**Operational health:**
- Redis used memory: 2.3 MB · Postgres database size: 8.8 MB
- Peak consumer-group pending entries observed: 7 · peak lag observed: 6 (both cleared
  fully by the end of the run)

## Design decisions

A few of the load-bearing ones (see [Why these choices](#why-these-choices) above for
the top-level stack decisions):

- **At-least-once, not exactly-once, with a documented bounded gap.** A batch's stream
  entries are only `XACK`ed after every rollup they touched is durably upserted. The
  one gap this does *not* close: if the process crashes between a bucket's Postgres
  commit and the `XACK` for that same batch, the redelivered replay can under-represent
  that one bucket by at most one batch's worth of events (the in-memory sample from
  before the crash is gone). Closing this fully would need a durable per-bucket
  watermark or a transactional outbox — tracked as a future improvement, not silently
  worked around.
- **Only 5xx counts as an "error"** for aggregation — 4xx responses are legitimate
  client behavior (bad input, not-found), not service health signal.
- **IsolationForest needs a higher minimum history than the statistical detectors** —
  found empirically (n=10 demonstrably missed an obvious injected spike that z-score
  and EWMA both caught), not assumed from theory.
- **Detection currently runs once per batch that touches a bucket**, not once per
  bucket-close — cheap at this scale, but confirmed live during testing to occasionally
  produce near-duplicate anomaly rows when one anomalous burst happens to span two
  Redis batches. Named as the first thing to fix if monitored-endpoint count grows
  significantly (see future improvements).
- **Incremental WebSocket updates, not full-snapshot broadcasts** — one per-bucket
  `metric_point` message rather than resending the whole dashboard's state on every
  change.

## Repository structure

```
pulse/
├── api/            fast path — FastAPI app: /events, read endpoints, /health, /ws
├── consumer/        slow path — XREADGROUP loop, aggregation, detection, alerting glue
├── detector/         pure, unit-tested anomaly detection functions (no I/O)
├── dashboard/       single-file frontend, no build step
├── benchmark/        load-test harness: orchestration, metrics, report rendering
├── simulator/         traffic generation used by both the benchmark and seed script
├── db/                SQLAlchemy models + session/engine factory
├── core/              cross-cutting: config, logging, Redis client, lag introspection,
│                       wire-protocol constants shared across Docker images
├── scripts/            operational scripts (sample-data seeding)
├── tests/              full test suite (hermetic — fakeredis + SQLite, no live services)
├── docs/               architecture and deployment documentation
├── docker/              per-service Dockerfiles
├── docker-compose.yml
├── schemas.py           the input trust boundary (wire-JSON validation)
└── requirements*.txt
```

## Testing

The full suite is hermetic — `fakeredis` and an in-memory SQLite database stand in for
Redis and Postgres, so no live services are required to run it:

```bash
pip install -r requirements-dev.txt
pytest tests/ --cov=detector --cov-report=term-missing
```

`detector/` (the anomaly detection core) specifically targets >90% line coverage.
`ruff check`, `ruff format --check`, and `mypy` also run on every push via GitHub
Actions (`.github/workflows/ci.yml`).

Live-stack verification (crash recovery, real Redis/Postgres behavior, end-to-end
dashboard/webhook behavior) is done separately against the real `docker compose up`
stack — see `docs/ARCHITECTURE.md`.

## Troubleshooting

**`docker compose up` hangs on a healthcheck.** Check `docker compose logs <service>`.
Postgres/Redis usually just need a few seconds; `api`/`consumer` failing their
healthcheck almost always means they can't reach Postgres/Redis yet, or `.env` wasn't
copied from `.env.example`.

**Dashboard is empty.** The consumer aggregates in one-minute buckets — nothing
appears until at least one bucket has closed and been upserted. Run
`python scripts/seed_sample_data.py` for an immediate, realistic dataset instead of
waiting on real traffic.

**`POST /events` returns 503.** Redis is unreachable from the `api` container. Check
`docker compose ps` for the `redis` service's health status and `REDIS_URL`.

**Consumer isn't processing anything.** Check `GET /health/consumer` — a
`pending_count` that only grows, or a `consumers` list with a large or growing
`idle_ms`, means the consumer process has stalled or crashed; check
`docker compose logs consumer`.

**Webhook alerts aren't arriving.** `WEBHOOK_URL` is unset by default (alerting is
opt-in). Set it in `.env` and restart the `consumer` service.

## Future improvements

Concrete, scoped items already identified during development, not vague roadmap items:

1. **Per-bucket watermark column** to close the at-least-once crash-window gap
   documented above, for exact-once consistency.
2. **Streaming percentile approximation** (t-digest or HDRHistogram) in the consumer,
   trading exactness for bounded memory at very high per-bucket cardinality.
3. **Cursor-based pagination** for the read API once result sets are large enough that
   offset pagination's scan cost matters.
4. **An absolute-threshold detector** (e.g. sustained error rate above a fixed
   threshold, regardless of recent history) alongside the adaptive baselines,
   specifically to catch sustained outages that a purely adaptive baseline eventually
   "forgets," and to reduce the false-positive rate the benchmark measured on
   low-volume endpoints.
5. **Seasonality-aware baselines** (day-of-week/hour-of-day stratified comparison, or a
   change-point detector for gradual drift) — the current rolling window has no concept
   of expected periodic patterns.
6. **Detection debounced to once per bucket-close**, not once per batch that touches a
   bucket — the first thing that gets wasteful as monitored-endpoint count grows.

## License

MIT — see [LICENSE](LICENSE).
