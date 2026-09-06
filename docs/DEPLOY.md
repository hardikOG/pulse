# Deployment

Pulse runs anywhere Docker does. This document covers local Docker (the primary,
fully-verified path), Railway, and Render, plus a no-Docker local development setup.

**A note on the two cloud platforms:** the exact click-path and free-tier limits on
Railway and Render change over time, and this project cannot verify a live deployment
without an account on each. Treat the Railway/Render sections as an accurate,
best-effort walkthrough of the mechanics involved (what services to create, what env
vars to wire, and the one non-obvious gotcha every deploy on either platform hits — see
[The DATABASE_URL scheme gotcha](#the-database_url-scheme-gotcha) below) rather than a
guaranteed sequence of exact button labels.

## Contents

- [Local (Docker)](#local-docker)
- [Local (no Docker)](#local-no-docker)
- [Railway](#railway)
- [Render](#render)
- [The DATABASE_URL scheme gotcha](#the-database_url-scheme-gotcha)
- [Environment variables](#environment-variables)
- [Verification checklist](#verification-checklist)
- [Troubleshooting](#troubleshooting)
- [Rollback](#rollback)

## Local (Docker)

The primary, fully-verified deployment path — see the [README](../README.md#quickstart).

```bash
git clone https://github.com/hardikOG/pulse.git
cd pulse
cp .env.example .env
docker compose up --build
```

## Local (no Docker)

For developing against the source directly. Requires a local Redis and Postgres
(e.g. installed natively, or run just those two via
`docker compose up postgres redis`).

```bash
python -m venv .venv
source .venv/bin/activate   # or .venv\Scripts\activate on Windows
pip install -r requirements-dev.txt
cp .env.example .env
# edit .env: DATABASE_URL/REDIS_URL should point at localhost, not the
# postgres/redis Docker service hostnames
```

Run each process in its own terminal:

```bash
uvicorn api.main:app --reload --port 8000
python -m consumer.main
```

## Railway

Railway does not run `docker-compose.yml` directly — each process is its own Railway
**service** within one **project**, each built from its own Dockerfile.

1. Create a new Railway project from this GitHub repo.
2. Add a **PostgreSQL** database plugin to the project.
3. Add a **Redis** database plugin to the project.
4. Add a service for the API:
   - Source: this repo.
   - Settings → Build → **Dockerfile Path**: `docker/Dockerfile.api`, **Root
     Directory**: `/` (repo root — the Dockerfile's `COPY` paths are relative to the
     build context, which must stay the repo root, not `docker/`).
   - Settings → Networking → generate a public domain, port `8000`.
   - Variables: `DATABASE_URL` and `REDIS_URL` referencing the plugins (Railway
     exposes these as reference variables, e.g. `${{Postgres.DATABASE_URL}}` and
     `${{Redis.REDIS_URL}}` in the Railway UI's variable picker) — see the
     [scheme gotcha](#the-database_url-scheme-gotcha) below before assuming the
     referenced value works as-is.
5. Add a second service for the consumer, same repo, **Dockerfile Path**:
   `docker/Dockerfile.consumer`, same `DATABASE_URL`/`REDIS_URL` variables, **no**
   public domain (it serves no HTTP traffic).
6. Deploy both services.

## Render

A best-effort [`render.yaml`](../render.yaml) Blueprint is included at the repo root —
review it against Render's current [Blueprint spec](https://render.com/docs/blueprint-spec)
before use, since Render's managed-Redis product naming (`redis` vs. "Key Value") and
free-tier availability have both changed over time.

1. In the Render dashboard: **New → Blueprint**, point it at this repo.
2. Render will parse `render.yaml` and propose four resources: a Postgres database, a
   Redis (or Key Value) instance, and two **Web Services** — `pulse-api`
   (`docker/Dockerfile.api`) and `pulse-consumer` (`docker/Dockerfile.consumer`).
3. Apply the blueprint.
4. After the Postgres instance is provisioned, open its connection string and apply
   the [scheme fix](#the-database_url-scheme-gotcha) to both the `pulse-api` and
   `pulse-consumer` services' `DATABASE_URL` environment variable.

**Why `pulse-consumer` is declared as a Web Service, not a Background Worker:**
Render's Background Worker product has no free tier (a paid Starter instance is the
minimum, currently $7/mo) — confirmed directly against the live account this project
was deployed from, not assumed from documentation. The consumer accepts no HTTP
traffic by design, but `consumer/health_server.py` binds a trivial
`{"status": "ok"}` HTTP endpoint to Render's injected `$PORT` env var purely so the
platform's port-scan/health-check for its free Web Service tier passes. That module
is completely inert everywhere else — `docker-compose.yml`, a plain
`python -m consumer.main` — since `PORT` is never set in either of those contexts.
5. Deploy.

## The DATABASE_URL scheme gotcha

Both Railway's and Render's managed Postgres addons hand out a connection string with
scheme `postgres://` or `postgresql://`. Pulse's `DATABASE_URL` (see `db/session.py`,
which passes it straight to SQLAlchemy's `create_engine`) expects the **psycopg3**
driver explicitly in the scheme: `postgresql+psycopg://...`. Without the `+psycopg`
part, SQLAlchemy falls back to looking for `psycopg2` (not installed — this project
pins `psycopg[binary]`, the v3 package) and the API/consumer will fail to start with an
import error.

**Fix:** after either platform generates `DATABASE_URL`, edit it in the
service's environment variable settings to change the scheme prefix from
`postgres://` or `postgresql://` to `postgresql+psycopg://`, leaving the rest of the
URL (host, port, user, password, database) unchanged.

`REDIS_URL` from either platform's managed Redis does not need this treatment — the
`redis://` scheme both platforms provide matches what Pulse expects directly.

## Environment variables

See the [README's environment variable table](../README.md#environment-variables) for
the full list and defaults. In a cloud deployment, only `DATABASE_URL` and `REDIS_URL`
need to be overridden from the platform's provisioned values (with the scheme fix
above); every other variable can be left at its default or tuned as needed.

## Verification checklist

After deploying (any platform):

```bash
curl https://<your-api-host>/health
# {"status":"ok"}

curl https://<your-api-host>/health/consumer
# {"stream_length":0,"pending_count":0,"lag":null,"consumers":[]}
# (consumers stays empty until the consumer service has started and read at least once)
```

- Open `https://<your-api-host>/` — the dashboard should load (even if empty).
- Open the browser devtools Network tab and confirm the `/ws` WebSocket connects
  (status `101 Switching Protocols`) and receives a `snapshot` message.
- Send one real event and confirm it's accepted:
  ```bash
  curl -X POST https://<your-api-host>/events \
    -H "Content-Type: application/json" \
    -d '{"service":"smoke-test","endpoint":"/ping","status_code":200,"latency_ms":10,"ts":"<current UTC ISO time>"}'
  ```
- If `WEBHOOK_URL` is configured, trigger a real anomaly (e.g. via
  `scripts/seed_sample_data.py` pointed at the live URL with `--url`) and confirm
  delivery at the webhook endpoint.

**Do not run the full `benchmark` service against a live free-tier deployment** — it
is a multi-minute sustained load test and will likely exceed free-tier resource or
request limits. Use `scripts/seed_sample_data.py` (a much lighter one-shot seed) to
verify the live pipeline instead; only run the real benchmark against a local or
self-hosted deployment you control the resource limits of.

## Troubleshooting

**Service fails to start with a driver/import error.** Almost always the
[DATABASE_URL scheme gotcha](#the-database_url-scheme-gotcha) above.

**API is up but the dashboard never gets live updates.** Confirm both `api` and
`consumer` are pointed at the *same* `REDIS_URL` — live updates are bridged through
Redis Pub/Sub between the two processes; if they're on different Redis instances
(e.g. one service still has a stale/default value), nothing will arrive.

**Consumer service shows as "running" but never processes anything.** Check
`GET /health/consumer` — an empty `consumers` list after the consumer has had time to
boot usually means it crashed before creating the consumer group; check that
service's logs on the platform dashboard.

**429s / connection refused under `scripts/seed_sample_data.py`.** Free-tier compute
is typically far smaller than local Docker — reduce concurrency by seeding a shorter
scenario or `--no-live` (warmup history only, sent with bounded concurrency).

## Rollback

**Docker (local or self-hosted):** `docker compose down`, check out the previous
commit/tag, `docker compose up --build`.

**Railway:** each deploy is versioned per service — use the service's **Deployments**
tab to redeploy a previous build.

**Render:** each deploy is versioned per service — use the service's **Events**/
**Deploys** tab to roll back to a previous deploy.
