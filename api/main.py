"""Pulse API — the fast path. Validates events and pushes them onto the Redis stream
buffer; all heavy work (aggregation, anomaly detection) happens downstream in the
consumer (the slow path).
"""

import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Request, status
from redis.asyncio import Redis
from redis.exceptions import RedisError
from starlette.types import ASGIApp, Receive, Scope, Send

from core.config import get_settings
from core.logging import get_logger
from core.redis_client import make_redis_client
from schemas import EventIn

settings = get_settings()
logger = get_logger("pulse.api", settings.log_level)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Construct and tear down the Redis client alongside the app's lifecycle.

    Purpose: owns the Redis client's lifetime so it is created once per process
        (not per-request) and closed cleanly on shutdown, without a module-level
        global — the client lives on app.state, scoped to this app instance.
    Inputs: app — the FastAPI application being started.
    Outputs: yields control while the app serves requests; no return value.
    Complexity: O(1).
    Failure cases: none — client construction is lazy and does not connect eagerly.
    """
    app.state.redis = make_redis_client(settings)
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
