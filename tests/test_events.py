"""Phase 1 gate: POST /events validates strictly, buffers to Redis on success, and
fails closed (503) rather than dropping data silently when Redis is unavailable.
"""

import asyncio
from datetime import datetime, timedelta, timezone

import fakeredis.aioredis
import pytest
from fastapi.testclient import TestClient
from redis.exceptions import ConnectionError as RedisConnectionError

from api.main import app, get_redis

VALID_EVENT = {
    "service": "checkout",
    "endpoint": "/api/pay",
    "status_code": 200,
    "latency_ms": 42.5,
    "ts": datetime.now(timezone.utc).isoformat(),
}


class _BrokenRedis:
    """A fake Redis client whose xadd always fails, for exercising the 503 path."""

    async def xadd(self, *args: object, **kwargs: object) -> None:
        raise RedisConnectionError("connection refused")


@pytest.fixture
def fake_redis() -> fakeredis.aioredis.FakeRedis:
    return fakeredis.aioredis.FakeRedis(decode_responses=True)


def test_valid_event_accepted_and_lands_on_stream(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    app.dependency_overrides[get_redis] = lambda: fake_redis
    try:
        with TestClient(app) as client:
            response = client.post("/events", json=VALID_EVENT)
        assert response.status_code == 202
        body = response.json()
        assert body["status"] == "accepted"
        assert "stream_id" in body

        entries = asyncio.run(fake_redis.xrange("pulse:events"))
        assert len(entries) == 1
        _, fields = entries[0]
        assert fields["service"] == "checkout"
        assert fields["endpoint"] == "/api/pay"
        assert fields["status_code"] == "200"
        assert fields["latency_ms"] == "42.5"
    finally:
        app.dependency_overrides.clear()


def test_invalid_status_code_returns_422() -> None:
    payload = {**VALID_EVENT, "status_code": 999}
    with TestClient(app) as client:
        response = client.post("/events", json=payload)
    assert response.status_code == 422


def test_missing_field_returns_422() -> None:
    payload = {k: v for k, v in VALID_EVENT.items() if k != "service"}
    with TestClient(app) as client:
        response = client.post("/events", json=payload)
    assert response.status_code == 422


def test_negative_latency_returns_422() -> None:
    payload = {**VALID_EVENT, "latency_ms": -1}
    with TestClient(app) as client:
        response = client.post("/events", json=payload)
    assert response.status_code == 422


def test_blank_service_returns_422() -> None:
    payload = {**VALID_EVENT, "service": ""}
    with TestClient(app) as client:
        response = client.post("/events", json=payload)
    assert response.status_code == 422


def test_future_timestamp_returns_422() -> None:
    future_ts = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    payload = {**VALID_EVENT, "ts": future_ts}
    with TestClient(app) as client:
        response = client.post("/events", json=payload)
    assert response.status_code == 422


def test_redis_unavailable_returns_503() -> None:
    app.dependency_overrides[get_redis] = lambda: _BrokenRedis()
    try:
        with TestClient(app) as client:
            response = client.post("/events", json=VALID_EVENT)
        assert response.status_code == 503
    finally:
        app.dependency_overrides.clear()
