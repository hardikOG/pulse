"""Unit tests for consumer/broadcast.py — Pub/Sub message shapes and best-effort publish."""

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
from redis.exceptions import RedisError

from consumer.broadcast import build_anomaly_message, build_metric_point_message, publish

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def test_build_metric_point_message_shape() -> None:
    message = build_metric_point_message("checkout", "/pay", NOW, 100, 5, 40.0, 90.0, 110.0)
    assert message == {
        "type": "metric_point",
        "service": "checkout",
        "endpoint": "/pay",
        "minute_bucket": NOW.isoformat(),
        "request_count": 100,
        "error_count": 5,
        "p50": 40.0,
        "p95": 90.0,
        "p99": 110.0,
    }


def test_build_anomaly_message_shape() -> None:
    message = build_anomaly_message(7, "checkout", "/pay", NOW, "zscore", 4.2, "p95 spike", NOW)
    assert message == {
        "type": "anomaly",
        "id": 7,
        "service": "checkout",
        "endpoint": "/pay",
        "minute_bucket": NOW.isoformat(),
        "detector": "zscore",
        "score": 4.2,
        "reason": "p95 spike",
        "created_at": NOW.isoformat(),
    }


@pytest.mark.asyncio
async def test_publish_sends_json_encoded_message_to_channel() -> None:
    redis_client = AsyncMock()
    logger = MagicMock()
    message = {"type": "metric_point", "service": "checkout"}

    await publish(redis_client, "pulse:live", message, logger)

    redis_client.publish.assert_called_once_with("pulse:live", json.dumps(message))
    logger.error.assert_not_called()


@pytest.mark.asyncio
async def test_publish_failure_is_logged_not_raised() -> None:
    redis_client = AsyncMock()
    redis_client.publish.side_effect = RedisError("connection refused")
    logger = MagicMock()

    await publish(redis_client, "pulse:live", {"type": "anomaly"}, logger)

    logger.error.assert_called_once()
