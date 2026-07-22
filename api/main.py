"""Pulse API — the fast path. Validates events and pushes them onto the Redis stream
buffer; all heavy work (aggregation, anomaly detection) happens downstream in the
consumer (the slow path).
"""

import time
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query, Request, status
from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy.orm import Session
from starlette.types import ASGIApp, Receive, Scope, Send

from api.queries import StatusClass, get_service_rows, list_endpoints, list_services
from api.responses import (
    EndpointListResponse,
    EndpointSeries,
    EndpointSummary,
    MetricPoint,
    PaginationMeta,
    ServiceDetailResponse,
    ServiceListResponse,
    ServiceSummary,
)
from core.config import get_settings
from core.logging import get_logger
from core.redis_client import make_redis_client
from db.session import make_engine, make_session_factory
from schemas import EventIn

settings = get_settings()
logger = get_logger("pulse.api", settings.log_level)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Construct and tear down the Redis and Postgres clients alongside the app.

    Purpose: owns both clients' lifetimes so they are created once per process (not
        per-request), without a module-level global — they live on app.state, scoped
        to this app instance. Deliberately does NOT run create_all here: the consumer
        is the sole schema owner (see consumer/main.py), and keeping API startup free
        of any Postgres dependency means the API stays responsive (and its healthcheck
        stays green) even if Postgres is briefly unreachable at boot — the same
        liveness-vs-readiness reasoning as GET /health. A read request arriving before
        the consumer's create_all has run will fail with a clear error and succeed
        moments later once tables exist, rather than the whole API refusing to start.
    Inputs: app — the FastAPI application being started.
    Outputs: yields control while the app serves requests; no return value.
    Complexity: O(1).
    Failure cases: none — both clients are constructed lazily and do not connect
        eagerly.
    """
    app.state.redis = make_redis_client(settings)
    engine = make_engine(settings)
    app.state.session_factory = make_session_factory(engine)
    try:
        yield
    finally:
        await app.state.redis.aclose()


app = FastAPI(title="Pulse API", lifespan=lifespan)


def get_redis(request: Request) -> Redis:
    """FastAPI dependency returning the process's shared Redis client.

    Purpose: dependency-injection seam for the Redis client — routes depend on this
        function rather than importing a global, which also lets tests override it.
    Inputs: request — the current Request (used only to reach app.state).
    Outputs: the Redis client constructed in lifespan().
    Complexity: O(1).
    Failure cases: none — raises AttributeError only if called outside a running app
        (i.e. lifespan never ran), which cannot happen via normal request handling.
    """
    return request.app.state.redis


def get_db(request: Request) -> Iterator[Session]:
    """FastAPI dependency yielding a Session for the duration of one request.

    Purpose: dependency-injection seam for Postgres access, mirroring get_redis.
        Read-only usage throughout the read API, so no commit is needed — the session
        is simply closed after the request.
    Inputs: request — used only to reach app.state.session_factory.
    Outputs: yields a Session; closes it once the route handler returns.
    Complexity: O(1) overhead beyond the wrapped queries.
    Failure cases: none raised here — query-level failures propagate to the route.
    """
    session = request.app.state.session_factory()
    try:
        yield session
    finally:
        session.close()


class RequestLoggingMiddleware:
    """Pure ASGI middleware that logs every request's method, path, status, and latency.

    Purpose: satisfy the project's logging requirement (every API request logged)
        without per-route boilerplate. Deliberately implemented as a raw ASGI
        middleware — NOT Starlette's `@app.middleware("http")` / BaseHTTPMiddleware —
        because BaseHTTPMiddleware buffers the request body through an internal
        anyio stream, which measurably adds latency to POST requests (see
        docs/private/ARCHITECTURE_LEDGER.md for the benchmark that caught this).
        A raw ASGI middleware wraps `send` directly with none of that overhead.
    Inputs: app — the next ASGI application in the chain.
    Outputs: n/a (ASGI callable).
    Complexity: O(1) overhead per request.
    Failure cases: if the wrapped app raises, the exception is logged and re-raised
        unchanged.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        start = time.perf_counter()
        status_code = 500

        async def send_wrapper(message: dict[str, Any]) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        except Exception:
            elapsed_ms = (time.perf_counter() - start) * 1000
            logger.error(
                "request failed",
                extra={
                    "extra_fields": {
                        "method": scope["method"],
                        "path": scope["path"],
                        "elapsed_ms": round(elapsed_ms, 2),
                    }
                },
            )
            raise
        else:
            elapsed_ms = (time.perf_counter() - start) * 1000
            logger.info(
                "request",
                extra={
                    "extra_fields": {
                        "method": scope["method"],
                        "path": scope["path"],
                        "status": status_code,
                        "elapsed_ms": round(elapsed_ms, 2),
                    }
                },
            )


app.add_middleware(RequestLoggingMiddleware)


@app.get("/health")
async def health() -> dict[str, str]:
    """Liveness probe for the API process.

    Purpose: confirm the API process is up and serving requests. Deliberately does
        NOT check Postgres/Redis connectivity — this is a process liveness check, not
        a dependency readiness check, so it stays fast and reliable as a Docker
        healthcheck target.
    Inputs: none.
    Outputs: {"status": "ok"} with HTTP 200.
    Complexity: O(1).
    Failure cases: none — this route cannot fail short of the process itself being down.
    """
    return {"status": "ok"}


@app.post("/events", status_code=status.HTTP_202_ACCEPTED)
async def ingest_event(
    event: EventIn, redis_client: Redis = Depends(get_redis)
) -> dict[str, str]:
    """Validate an incoming event and push it onto the Redis stream buffer.

    Purpose: the entire fast path — validate, buffer, return. No aggregation, no DB
        write, no anomaly detection happen here; that is the consumer's job.
    Inputs: event — a validated EventIn (FastAPI returns 422 automatically before this
        function runs if the request body fails validation); redis_client — injected
        Redis client.
    Outputs: {"status": "accepted", "stream_id": <redis stream entry id>} with HTTP 202.
    Complexity: O(1) — a single XADD call.
    Failure cases: if Redis is unreachable or times out, raises HTTPException(503) —
        the event is NOT silently dropped or accepted without being durably buffered.
    """
    fields = {
        "service": event.service,
        "endpoint": event.endpoint,
        "status_code": str(event.status_code),
        "latency_ms": str(event.latency_ms),
        "ts": event.ts.isoformat(),
    }
    try:
        stream_id = await redis_client.xadd(settings.event_stream, fields)
    except RedisError as exc:
        logger.error(
            "stream push failed",
            extra={"extra_fields": {"stream": settings.event_stream, "error": str(exc)}},
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="event buffer unavailable",
        ) from exc
    return {"status": "accepted", "stream_id": stream_id}


@app.get("/services", response_model=ServiceListResponse)
async def get_services(
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    status_class: StatusClass = Query(default="all"),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    session: Session = Depends(get_db),
) -> ServiceListResponse:
    """List services with additive request/error totals over a time range.

    Purpose: the top-level service-health tile list for the dashboard.
    Inputs: start/end — optional range bounds (default: last 60 minutes); status_class
        — "all"/"healthy"/"error" filter on whether any errors occurred in range;
        limit/offset — pagination; session — injected DB session.
    Outputs: ServiceListResponse — no percentile field here (see api/responses.py for
        why: percentiles can't be honestly merged across a service's endpoints).
    Complexity: two aggregate queries over the metrics table.
    Failure cases: none — an out-of-range time window just yields an empty list.
    """
    rows, total, _, _ = list_services(session, start, end, status_class, limit, offset)
    items = [
        ServiceSummary(
            service=row.service,
            request_count=row.request_count,
            error_count=row.error_count,
            error_rate=(row.error_count / row.request_count) if row.request_count else 0.0,
            last_seen=row.last_seen,
        )
        for row in rows
    ]
    return ServiceListResponse(
        items=items, pagination=PaginationMeta(total=total, limit=limit, offset=offset)
    )


@app.get("/endpoints", response_model=EndpointListResponse)
async def get_endpoints(
    service: str | None = Query(default=None),
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    status_class: StatusClass = Query(default="all"),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    session: Session = Depends(get_db),
) -> EndpointListResponse:
    """List (service, endpoint) pairs with additive request/error totals.

    Purpose: the browsable/searchable endpoint list across all services.
    Inputs: service — optional exact-match filter; start/end; status_class;
        limit/offset; session.
    Outputs: EndpointListResponse — again no percentile field; see
        /services/{service}/metrics for exact per-minute percentiles.
    Complexity: two aggregate queries over the metrics table.
    Failure cases: none.
    """
    rows, total, _, _ = list_endpoints(
        session, service, start, end, status_class, limit, offset
    )
    items = [
        EndpointSummary(
            service=row.service,
            endpoint=row.endpoint,
            request_count=row.request_count,
            error_count=row.error_count,
            error_rate=(row.error_count / row.request_count) if row.request_count else 0.0,
            last_seen=row.last_seen,
        )
        for row in rows
    ]
    return EndpointListResponse(
        items=items, pagination=PaginationMeta(total=total, limit=limit, offset=offset)
    )


@app.get("/services/{service}/metrics", response_model=ServiceDetailResponse)
async def get_service_detail(
    service: str,
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    session: Session = Depends(get_db),
) -> ServiceDetailResponse:
    """One service's endpoints, each with its own exact per-minute series.

    Purpose: backs the dashboard's per-endpoint latency/error charts for one service.
        Route is /services/{service}/metrics rather than /services/{service} directly
        so future service-scoped resources (/anomalies, /health, /endpoints) have a
        clean, consistent place to live alongside it.
    Inputs: service — exact service name (path param); start/end — optional range
        bounds; session.
    Outputs: ServiceDetailResponse with one EndpointSeries per endpoint, each carrying
        its own unmerged MetricPoint rows — no aggregation across endpoints or across
        buckets beyond simple addition for the summary totals.
    Complexity: O(n) in matching metrics rows, grouped in Python by endpoint.
    Failure cases: raises HTTPException(404) if the service has no rows in range.
    """
    rows, _, _ = get_service_rows(session, service, start, end)
    if not rows:
        raise HTTPException(status_code=404, detail="service not found")

    endpoints: dict[str, list[Any]] = {}
    for row in rows:
        endpoints.setdefault(row.endpoint, []).append(row)

    endpoint_series = []
    total_requests = 0
    total_errors = 0
    for endpoint, endpoint_rows in endpoints.items():
        request_count = sum(r.request_count for r in endpoint_rows)
        error_count = sum(r.error_count for r in endpoint_rows)
        total_requests += request_count
        total_errors += error_count
        endpoint_series.append(
            EndpointSeries(
                endpoint=endpoint,
                request_count=request_count,
                error_count=error_count,
                error_rate=(error_count / request_count) if request_count else 0.0,
                series=[
                    MetricPoint(
                        minute_bucket=r.minute_bucket,
                        request_count=r.request_count,
                        error_count=r.error_count,
                        p50=r.p50,
                        p95=r.p95,
                        p99=r.p99,
                    )
                    for r in endpoint_rows
                ],
            )
        )

    return ServiceDetailResponse(
        service=service,
        request_count=total_requests,
        error_count=total_errors,
        error_rate=(total_errors / total_requests) if total_requests else 0.0,
        endpoints=endpoint_series,
    )
