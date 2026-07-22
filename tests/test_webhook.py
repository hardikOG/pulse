"""Unit tests for consumer/webhook.py — retry/backoff and dispatch logic."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from consumer.webhook import build_webhook_payload, deliver_webhook, dispatch_webhook
from core.config import Settings

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _settings(**overrides) -> Settings:
    defaults = {
        "webhook_url": "https://example.test/hook",
        "webhook_max_retries": 3,
        "webhook_backoff_base_seconds": 0.001,
        "webhook_timeout_seconds": 1.0,
    }
    defaults.update(overrides)
    return Settings(**defaults)


def test_build_webhook_payload_shape() -> None:
    payload = build_webhook_payload(42, NOW, "checkout", "/pay", "zscore", 4.2, "p95 spike")
    assert payload == {
        "id": 42,
        "minute_bucket": NOW.isoformat(),
        "service": "checkout",
        "endpoint": "/pay",
        "detector": "zscore",
        "score": 4.2,
        "reason": "p95 spike",
    }


@pytest.mark.asyncio
async def test_deliver_webhook_succeeds_first_try() -> None:
    client = AsyncMock(spec=httpx.AsyncClient)
    client.post.return_value = httpx.Response(200, request=httpx.Request("POST", "https://x"))
    logger = MagicMock()

    ok = await deliver_webhook(client, "https://example.test", {"id": 1}, 3, 0.001, 1.0, logger)

    assert ok is True
    assert client.post.call_count == 1
    logger.error.assert_not_called()


@pytest.mark.asyncio
async def test_deliver_webhook_retries_then_succeeds() -> None:
    client = AsyncMock(spec=httpx.AsyncClient)
    fail = httpx.Response(500, request=httpx.Request("POST", "https://x"))
    ok_response = httpx.Response(200, request=httpx.Request("POST", "https://x"))
    client.post.side_effect = [fail, fail, ok_response]
    logger = MagicMock()

    ok = await deliver_webhook(client, "https://example.test", {"id": 1}, 3, 0.001, 1.0, logger)

    assert ok is True
    assert client.post.call_count == 3
    assert logger.error.call_count == 2


@pytest.mark.asyncio
async def test_deliver_webhook_exhausts_retries() -> None:
    client = AsyncMock(spec=httpx.AsyncClient)
    client.post.return_value = httpx.Response(500, request=httpx.Request("POST", "https://x"))
    logger = MagicMock()

    ok = await deliver_webhook(client, "https://example.test", {"id": 1}, 3, 0.001, 1.0, logger)

    assert ok is False
    assert client.post.call_count == 3


@pytest.mark.asyncio
async def test_deliver_webhook_handles_connection_error() -> None:
    client = AsyncMock(spec=httpx.AsyncClient)
    client.post.side_effect = httpx.ConnectError("refused")
    logger = MagicMock()

    ok = await deliver_webhook(client, "https://example.test", {"id": 1}, 2, 0.001, 1.0, logger)

    assert ok is False
    assert client.post.call_count == 2


@pytest.mark.asyncio
async def test_dispatch_webhook_noop_when_url_unset() -> None:
    client = AsyncMock(spec=httpx.AsyncClient)
    logger = MagicMock()

    await dispatch_webhook(client, _settings(webhook_url=None), {"id": 1}, logger)

    client.post.assert_not_called()


@pytest.mark.asyncio
async def test_dispatch_webhook_logs_when_exhausted() -> None:
    client = AsyncMock(spec=httpx.AsyncClient)
    client.post.return_value = httpx.Response(500, request=httpx.Request("POST", "https://x"))
    logger = MagicMock()

    await dispatch_webhook(client, _settings(), {"id": 7}, logger)

    exhausted_calls = [
        call for call in logger.error.call_args_list if call.args[0] == "webhook delivery exhausted retries"
    ]
    assert len(exhausted_calls) == 1
