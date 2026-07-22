"""Webhook alert delivery — retried with backoff, dispatched as a background task so
a slow or failing third-party endpoint never blocks the consumer's main loop. See
docs/private/ARCHITECTURE_LEDGER.md for why this is fire-and-forget rather than
awaited inline, and why the anomaly id serves as a receiver-side idempotency key
instead of building full sender-side exactly-once delivery.
"""

import asyncio
from datetime import datetime
from typing import Any

import httpx

from core.config import Settings


def build_webhook_payload(
    anomaly_id: int,
    minute_bucket: datetime,
    service: str,
    endpoint: str,
    detector: str,
    score: float,
    reason: str,
) -> dict[str, Any]:
    """Build the JSON payload sent to the configured webhook URL.

    Purpose: single place defining the webhook wire format.
    Inputs: anomaly_id — the Anomaly row's DB primary key, included specifically as an
        idempotency key so the receiver can dedup a redelivery; the rest mirror the
        anomalies table's columns.
    Outputs: a JSON-serializable dict.
    Complexity: O(1).
    Failure cases: none.
    """
    return {
        "id": anomaly_id,
        "minute_bucket": minute_bucket.isoformat(),
        "service": service,
        "endpoint": endpoint,
        "detector": detector,
        "score": score,
        "reason": reason,
    }


async def deliver_webhook(
    client: httpx.AsyncClient,
    url: str,
    payload: dict[str, Any],
    max_retries: int,
    backoff_base_seconds: float,
    timeout_seconds: float,
    logger,
) -> bool:
    """POST payload to url, retrying with exponential backoff on failure.

    Purpose: the retry loop itself, separated from dispatch_webhook so it can be
        tested without needing asyncio.create_task involved.
    Inputs: client — a shared httpx.AsyncClient (reused across calls, not
        constructed per-webhook); url; payload; max_retries; backoff_base_seconds —
        delay before retry N is backoff_base_seconds * 2**(N-1); timeout_seconds;
        logger.
    Outputs: True if any attempt got a non-error (< 300) response, else False after
        exhausting all retries.
    Complexity: O(max_retries) HTTP attempts, each independently timed out.
    Failure cases: never raises — httpx.HTTPError (timeouts, connection errors) and
        non-2xx/3xx responses are both logged as "webhook delivery failed" (distinct
        from anomaly-detection failure logging — see ARCHITECTURE_LEDGER.md) and
        treated as a failed attempt eligible for retry.
    """
    for attempt in range(1, max_retries + 1):
        try:
            response = await client.post(url, json=payload, timeout=timeout_seconds)
            if response.status_code < 300:
                return True
            logger.error(
                "webhook delivery failed",
                extra={
                    "extra_fields": {
                        "attempt": attempt,
                        "status": response.status_code,
                        "anomaly_id": payload.get("id"),
                    }
                },
            )
        except httpx.HTTPError as exc:
            logger.error(
                "webhook delivery failed",
                extra={
                    "extra_fields": {
                        "attempt": attempt,
                        "error": str(exc),
                        "anomaly_id": payload.get("id"),
                    }
                },
            )
        if attempt < max_retries:
            await asyncio.sleep(backoff_base_seconds * (2 ** (attempt - 1)))
    return False


async def dispatch_webhook(
    client: httpx.AsyncClient, settings: Settings, payload: dict[str, Any], logger
) -> None:
    """Best-effort webhook dispatch, meant to be scheduled via asyncio.create_task.

    Purpose: the coroutine the caller fires-and-forgets — never awaited inline in the
        detection/ack flow, so retry backoff (up to several seconds) never stalls
        stream processing.
    Inputs: client; settings — provides webhook_url (None disables delivery entirely)
        and retry/backoff/timeout tuning; payload; logger.
    Outputs: None.
    Complexity: see deliver_webhook.
    Failure cases: never raises — logs and returns if webhook_url is unset or if
        deliver_webhook exhausts all retries.
    """
    if not settings.webhook_url:
        return
    delivered = await deliver_webhook(
        client,
        settings.webhook_url,
        payload,
        settings.webhook_max_retries,
        settings.webhook_backoff_base_seconds,
        settings.webhook_timeout_seconds,
        logger,
    )
    if not delivered:
        logger.error(
            "webhook delivery exhausted retries",
            extra={"extra_fields": {"anomaly_id": payload.get("id")}},
        )
