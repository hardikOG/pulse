"""Pulse API — the fast path. Phase 0: liveness only. POST /events lands in Phase 1."""

import time
from collections.abc import Awaitable, Callable

from fastapi import FastAPI, Request, Response

from core.config import get_settings
from core.logging import get_logger

settings = get_settings()
logger = get_logger("pulse.api", settings.log_level)

app = FastAPI(title="Pulse API")


@app.middleware("http")
async def log_requests(
    request: Request, call_next: Callable[[Request], Awaitable[Response]]
) -> Response:
    """Log every request's method, path, status, and latency.

    Purpose: satisfy the project's logging requirement (every API request logged)
        without per-route boilerplate.
    Inputs: request — the incoming Request; call_next — the next handler in the chain.
    Outputs: the Response produced by call_next, unmodified.
    Complexity: O(1) overhead per request.
    Failure cases: if call_next raises, the exception propagates after being logged
        with status "error"; FastAPI's own exception handling still applies.
    """
    start = time.perf_counter()
    try:
        response = await call_next(request)
    except Exception:
        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.error(
            "request failed",
            extra={
                "extra_fields": {
                    "method": request.method,
                    "path": request.url.path,
                    "elapsed_ms": round(elapsed_ms, 2),
                }
            },
        )
        raise
    elapsed_ms = (time.perf_counter() - start) * 1000
    logger.info(
        "request",
        extra={
            "extra_fields": {
                "method": request.method,
                "path": request.url.path,
                "status": response.status_code,
                "elapsed_ms": round(elapsed_ms, 2),
            }
        },
    )
    return response


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
