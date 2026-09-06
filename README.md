# Pulse

[![CI](https://github.com/hardikOG/pulse/actions/workflows/ci.yml/badge.svg)](https://github.com/hardikOG/pulse/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/docker-compose-2496ED?logo=docker&logoColor=white)](https://docs.docker.com/compose/)
[![Coverage: detector 100%](https://img.shields.io/badge/coverage-detector%20100%25-brightgreen)](#testing)
[![License: MIT](https://img.shields.io/badge/license-MIT-lightgrey)](LICENSE)

**[Live demo](https://pulse-api-g7w3.onrender.com)** · [API docs](https://pulse-api-g7w3.onrender.com/docs) · [Architecture](docs/ARCHITECTURE.md) · [Deploy guide](docs/DEPLOY.md)

> The live demo runs on a free-tier instance that spins down after inactivity — the first request can take 30-60s to wake it up.

Pulse is a self-hosted API observability platform built around a **fast-path / slow-path
architecture**: services push request events into a Redis Stream on the hot path, and a
separate consumer process asynchronously aggregates metrics, runs hybrid statistical +
ML anomaly detection, persists everything to PostgreSQL, and pushes live updates to a
WebSocket dashboard.

No synchronous database or ML work on the request path. No external SaaS dependency.
One `docker compose up`.

![Pulse dashboard](docs/img/dashboard.png)

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

`POST /events` does exactly one thing beyond validation: `XADD` onto a Redis Stream and
return `202 Accepted` — no database write, no aggregation, no detection on the request
path. A separate `consumer` process reads the stream via a **consumer group**
(`XREADGROUP`), which gives at-least-once delivery with automatic crash recovery:
unacknowledged entries are redelivered on restart, so a consumer crash never silently
loses in-flight work. See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full
data flow, sequence diagrams, and failure-recovery details.

## Why Pulse?

- **Fast-path ingestion** — `POST /events` validates and buffers to Redis; nothing
  slower ever runs in the request path. Measured at 540 events/sec with p95 267ms
  under concurrent load (see [Performance](#performance)).
- **Crash recovery, not just crash tolerance** — Redis consumer groups give
  pending-entry tracking and replay. Verified empirically: hard-killing the consumer
  mid-traffic produced zero data loss and zero double-counting across independent runs.
- **Explainable anomaly detection** — z-score + EWMA + IsolationForest, fused by
  flagging on any detector firing. Every finding's `reason` states which signal
  triggered and why — never a bare score.
- **Exact percentiles, never merged** — p50/p95/p99 are computed once from the raw
  sample and never re-derived by combining other percentiles, which is mathematically
  invalid. This is why summary endpoints report only additive fields.
- **Live operations** — consumer-group lag, pending entries, per-consumer liveness,
  metrics, and anomalies all reach the dashboard over one WebSocket, incrementally.
- **Measured, not claimed** — a real load-testing harness drives labeled traffic
  through the actual pipeline and reports throughput, latency percentiles, and
  detector precision/recall against ground truth — including the results that don't
  flatter it (see [Detection quality](#detection-quality)).

## Stack

**Backend:** FastAPI · Python 3.11 · Pydantic
**Streaming:** Redis Streams · Redis Pub/Sub
**Storage:** PostgreSQL · SQLAlchemy
**Detection:** z-score · EWMA · scikit-learn IsolationForest
**Frontend:** vanilla JS · Chart.js · WebSockets (no build step)
**Infrastructure:** Docker Compose · Railway/Render
**Quality:** pytest · ruff · mypy · GitHub Actions CI

## Quickstart

```bash
git clone https://github.com/hardikOG/pulse.git
cd pulse
cp .env.example .env
docker compose up --build
```

```bash
curl http://localhost:8000/health        # {"status":"ok"}
open http://localhost:8000/              # dashboard
```

Want a populated dashboard without generating traffic by hand?

```bash
pip install -r requirements.txt
python scripts/seed_sample_data.py
```

Or send one real event yourself:

```bash
curl -X POST http://localhost:8000/events \
  -H "Content-Type: application/json" \
  -d '{"service":"checkout","endpoint":"/api/pay","status_code":200,"latency_ms":42.5,"ts":"2026-01-01T12:00:00Z"}'
```

## Dashboard

![Pulse dashboard — per-endpoint latency chart](docs/img/dashboard_detail.png)

Single-file (`dashboard/index.html`), no build step, served at `/`. Service health
tiles on the left, updated live; a live anomaly feed and per-endpoint p95 latency
charts on the right for whichever service is selected. The header shows live
consumer-group pending count and lag, sourced from the same data `/health/consumer`
exposes.

## Performance

Measured by running `docker compose --profile benchmark run --rm benchmark` — real
traffic through the full stack, never seeded directly into Postgres. **These are
measurements from the committed run, not targets** (see
[Design decisions](#design-decisions)); re-run it yourself or see
`benchmark_report.md` / `benchmark_report.json` for the full report.

| Metric | Result |
|---|---:|
| Sustained ingest throughput (50-way concurrent burst) | **540.3 events/sec** |
| Consumer-processed throughput | **540.3 events/sec** (matches ingest exactly) |
| Burst ingest latency — p50 / p95 / p99 | 62.24ms / **267.58ms** / 428.89ms |
| Realistic paced-traffic latency — p50 / p95 / p99 | 2.48ms / **3.04ms** / 3.32ms |
| Redis memory used | 2.3 MB |
| Postgres database size | 8.8 MB |
| Peak consumer-group pending / lag | 7 / 6 (fully cleared) |

**Environment:** Linux (Docker), Python 3.11.15, 12 CPU cores, 7793 MB RAM.

### Detection quality

**100% recall · 12.5% precision** (3 true positives, 0 false negatives, 21 false
positives) against labeled ground-truth anomalies.

The benchmark deliberately injects labeled anomalies into a live traffic scenario.
Pulse caught every single one — zero missed. It also flagged 21 false positives,
concentrated on low-request-volume endpoints where a percentile computed from a small
sample is a noisy estimate, making real excursions past the z-score threshold more
common than a textbook baseline would predict.

This is a real, investigated finding, not a bug and not tuned away: raising the
threshold or narrowing the traffic distribution would have made the number look
better without changing anything true about the detector's behavior at modest volume.
For an alerting use case, recall matters more than precision — a missed real incident
is worse than an extra page — and it directly motivated the next concrete improvement:
an absolute-threshold detector alongside the adaptive z-score/EWMA baselines (see
[Future improvements](#future-improvements)).

## API reference

Full interactive docs (with request/response examples) at `/docs` (Swagger UI) and
`/redoc` while the API is running.

| Endpoint | Purpose |
|---|---|
| `POST /events` | Validate and buffer one event onto the fast path → `202`, or `503` if Redis is unreachable |
| `GET /services` | Paginated service list with additive request/error totals |
| `GET /endpoints` | Same, one row per `(service, endpoint)` pair |
| `GET /services/{service}/metrics` | Exact per-minute p50/p95/p99 series for one service |
| `GET /health` | Pure liveness probe |
| `GET /health/consumer` | Stream backlog/lag + per-consumer liveness — see below |
| `GET /ws` | WebSocket: one snapshot, then incremental `metric_point`/`anomaly`/`lag` messages |

**Example — send an event, see it aggregated:**

```bash
curl -X POST http://localhost:8000/events -H "Content-Type: application/json" -d '
  {"service":"checkout","endpoint":"/api/pay","status_code":200,"latency_ms":42.5,"ts":"2026-01-01T12:00:00Z"}'
# → {"status":"accepted","stream_id":"1735732800000-0"}

curl http://localhost:8000/services
# → {"items":[{"service":"checkout","request_count":1,"error_count":0,"error_rate":0.0,...}],...}
```

`GET /health/consumer` is worth calling out specifically: a total consumer outage looks
identical to "nothing has been produced recently" if you only look at stream lag, so
it also reports per-consumer idle time via `XINFO CONSUMERS`. It reports raw measured
numbers, never a computed up/down verdict — consistent with this project's stance that
performance and health numbers are reported, not asserted.

```json
{"stream_length": 42, "pending_count": 3, "lag": 7,
 "consumers": [{"name": "consumer-1", "pending": 3, "idle_ms": 120}]}
```

## Environment variables

Every value is environment-driven (`core/config.py`) with a sane local default;
`.env.example` documents each one. The full list:

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
| `MIN_HISTORY_BUCKETS` | `10` | Minimum history before the statistical detectors evaluate |
| `ISOLATION_FOREST_MIN_HISTORY` | `30` | Minimum history before IsolationForest evaluates (higher — see [Design decisions](#design-decisions)) |
| `WEBHOOK_URL` | *(unset)* | Optional URL to POST anomaly alerts to; alerting is disabled if unset |
| `WEBHOOK_MAX_RETRIES` | `3` | Webhook delivery retry count |
| `WEBHOOK_BACKOFF_BASE_SECONDS` | `1.0` | Exponential backoff base between webhook retries |
| `WEBHOOK_TIMEOUT_SECONDS` | `5.0` | Per-attempt webhook HTTP timeout |
| `LIVE_UPDATES_CHANNEL` | `pulse:live` | Redis Pub/Sub channel bridging the consumer to dashboard WebSocket clients |
| `LOG_LEVEL` | `INFO` | Structured logger level for both processes |

## Design decisions

The load-bearing ones — full tradeoff tables and rejected alternatives in
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#tradeoffs):

- **Redis Streams over Kafka** — consumer-group replay gives the same at-least-once
  guarantee without a second distributed system to operate at this scale.
- **At-least-once, not exactly-once, with one documented bounded gap** — a batch is
  only `XACK`ed after its rollup is durably upserted; the one gap left open (a crash
  between commit and ack) can under-represent one bucket by at most one batch's worth
  of events. Closing it needs a durable watermark column, tracked as future work.
- **Only 5xx counts as an "error"** — 4xx is legitimate client behavior, not a health
  signal.
- **IsolationForest needs a higher minimum history than the statistical detectors** —
  found empirically (n=10 missed an obvious injected spike z-score/EWMA both caught),
  not assumed from theory.
- **Incremental WebSocket updates, not full-snapshot broadcasts** — one per-bucket
  message rather than resending the whole dashboard's state on every change.

## Docker usage

`docker compose up --build` starts `postgres`, `redis`, `api` (port `8000`), and
`consumer`, each gated on a healthcheck. A fifth service, `benchmark`, is excluded from
the default `up` behind a Compose profile — a multi-minute load test should never fire
by accident:

```bash
docker compose --profile benchmark run --rm benchmark
```

writes `benchmark_report.md`/`.json` to the repo root. `docker compose down -v` tears
everything down including the Postgres volume.

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

Hermetic — `fakeredis` and in-memory SQLite stand in for Redis/Postgres, no live
services required:

```bash
pip install -r requirements-dev.txt
pytest tests/ --cov=detector --cov-report=term-missing
```

`detector/` targets >90% line coverage (currently 100%). `ruff check`, `ruff format
--check`, and `mypy` run on every push via `.github/workflows/ci.yml`.

## Troubleshooting

**`docker compose up` hangs on a healthcheck.** Check `docker compose logs <service>` —
`api`/`consumer` failing almost always means `.env` wasn't copied from `.env.example`.

**Dashboard is empty.** The consumer aggregates in one-minute buckets. Run
`python scripts/seed_sample_data.py` for an immediate dataset.

**`POST /events` returns 503.** Redis is unreachable from `api` — check
`docker compose ps` and `REDIS_URL`.

**Consumer isn't processing anything.** Check `GET /health/consumer` — a growing
`pending_count` or `idle_ms` means the consumer stalled; check its logs.

Full deployment troubleshooting (Railway, Render, the `DATABASE_URL` scheme gotcha
every cloud deploy hits) is in [docs/DEPLOY.md](docs/DEPLOY.md).

## Future improvements

Concrete, scoped items already identified, not a vague roadmap:

1. **Per-bucket watermark column** to close the at-least-once crash-window gap.
2. **Streaming percentile approximation** (t-digest) for bounded memory at very high
   per-bucket cardinality.
3. **Cursor-based pagination** once offset scan cost matters.
4. **An absolute-threshold detector** alongside the adaptive baselines, to catch
   sustained outages a purely adaptive baseline eventually "forgets," and to reduce
   the false-positive rate measured on low-volume endpoints.
5. **Seasonality-aware baselines** — the current rolling window has no concept of
   expected periodic patterns.
6. **Detection debounced to once per bucket-close**, not once per batch that touches
   a bucket.

## License

MIT — see [LICENSE](LICENSE).
