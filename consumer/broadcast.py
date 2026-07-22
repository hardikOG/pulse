"""Redis Pub/Sub publish helpers for live dashboard updates.

Deliberately best-effort (Pub/Sub, not Streams) — see docs/private/ARCHITECTURE_LEDGER.md
for why at-most-once is the right guarantee level here: the source of truth is
Postgres via the Phase 3 read API, so a dropped live-update message just means a
client's view is stale until its next refresh or reconnect, never wrong or lost.
"""

import json
from datetime import datetime
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import RedisError


def build_metric_point_message(
    service: str,
    endpoint: str,
    minute_bucket: datetime,
    request_count: int,
    error_count: int,
    p50: float,
    p95: float,
    p99: float,
) -> dict[str, Any]:
    """Build an incremental 'metric_point' live-update message for one bucket.

    Purpose: wire format for a single rollup update — the dashboard's steady-state
        broadcast unit (see the incremental-vs-snapshot ledger entry for why this is
        small and per-bucket rather than a full dashboard snapshot).
    Inputs: mirrors one metrics table row.
    Outputs: a JSON-serializable dict with a "type" discriminator for the client.
    Complexity: O(1).
    Failure cases: none.
    """
    return {
        "type": "metric_point",
        "service": service,
        "endpoint": endpoint,
        "minute_bucket": minute_bucket.isoformat(),
        "request_count": request_count,
        "error_count": error_count,
        "p50": p50,
        "p95": p95,
        "p99": p99,
    }


def build_anomaly_message(
    anomaly_id: int,
    service: str,
    endpoint: str,
    minute_bucket: datetime,
    detector: str,
    score: float,
    reason: str,
    created_at: datetime,
) -> dict[str, Any]:
    """Build an incremental 'anomaly' live-update message for one flagged finding.

    Purpose: wire format for a live anomaly notification. Includes id specifically so
        the dashboard client can dedup (a reconnecting client may see the same
        anomaly via both a bootstrap snapshot and a live message).
    Inputs: mirrors one anomalies table row.
    Outputs: a JSON-serializable dict with a "type" discriminator for the client.
    Complexity: O(1).
    Failure cases: none.
    """
    return {
        "type": "anomaly",
        "id": anomaly_id,
        "service": service,
        "endpoint": endpoint,
        "minute_bucket": minute_bucket.isoformat(),
        "detector": detector,
        "score": score,
        "reason": reason,
        "created_at": created_at.isoformat(),
    }


async def publish(redis_client: Redis, channel: str, message: dict[str, Any], logger) -> None:
    """Publish one live-update message, best-effort.

    Purpose: the single call site consumer/main.py and consumer/detection.py use to
        notify the dashboard — never allowed to affect the caller's own durability
        guarantees (metrics upsert, batch acking) if it fails.
    Inputs: redis_client; channel — settings.live_updates_channel; message — from
        build_metric_point_message or build_anomaly_message; logger.
    Outputs: None.
    Complexity: O(1) — PUBLISH does not block on subscriber delivery.
    Failure cases: never raises — RedisError is logged and swallowed.
    """
    try:
        await redis_client.publish(channel, json.dumps(message))
    except RedisError as exc:
        logger.error(
            "live update publish failed", extra={"extra_fields": {"error": str(exc)}}
        )
