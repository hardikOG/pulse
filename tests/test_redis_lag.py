"""Unit tests for core/redis_lag.py — best-effort stream/group lag introspection."""

from unittest.mock import AsyncMock

import pytest

from core.redis_lag import ConsumerInfo, get_consumer_info, get_lag_info


@pytest.mark.asyncio
async def test_get_lag_info_happy_path() -> None:
    redis_client = AsyncMock()
    redis_client.xlen.return_value = 42
    redis_client.xpending.return_value = {"pending": 3, "min": "1-0", "max": "3-0", "consumers": []}
    redis_client.xinfo_groups.return_value = [
        {"name": "other-group", "lag": 999},
        {"name": "pulse-consumers", "lag": 7},
    ]

    info = await get_lag_info(redis_client, "pulse:events", "pulse-consumers")

    assert info.stream_length == 42
    assert info.pending_count == 3
    assert info.lag == 7
    redis_client.xpending.assert_awaited_once_with("pulse:events", "pulse-consumers")


@pytest.mark.asyncio
async def test_get_lag_info_missing_group_gives_zero_pending() -> None:
    redis_client = AsyncMock()
    redis_client.xlen.return_value = 10
    # Real Redis raises a NOGROUP error for XPENDING against a group that doesn't
    # exist; fakeredis raises IndexError for the same case — either way, best-effort.
    redis_client.xpending.side_effect = Exception("NOGROUP no such consumer group")
    redis_client.xinfo_groups.return_value = [{"name": "unrelated-group", "lag": 1}]

    info = await get_lag_info(redis_client, "pulse:events", "pulse-consumers")

    assert info.stream_length == 10
    assert info.pending_count == 0
    assert info.lag is None


@pytest.mark.asyncio
async def test_get_lag_info_never_raises_when_stream_missing() -> None:
    redis_client = AsyncMock()
    redis_client.xlen.side_effect = Exception("no such key")
    redis_client.xpending.side_effect = Exception("no such key")
    redis_client.xinfo_groups.side_effect = Exception("no such key")

    info = await get_lag_info(redis_client, "pulse:events", "pulse-consumers")

    assert info.stream_length == 0
    assert info.pending_count == 0
    assert info.lag is None


@pytest.mark.asyncio
async def test_get_lag_info_missing_lag_field_defaults_to_none() -> None:
    redis_client = AsyncMock()
    redis_client.xlen.return_value = 5
    redis_client.xpending.return_value = {"pending": 2, "min": "1-0", "max": "2-0", "consumers": []}
    redis_client.xinfo_groups.return_value = [{"name": "pulse-consumers"}]

    info = await get_lag_info(redis_client, "pulse:events", "pulse-consumers")

    assert info.pending_count == 2
    assert info.lag is None


@pytest.mark.asyncio
async def test_get_lag_info_handles_empty_xpending_result() -> None:
    """XPENDING returns a falsy/empty summary (no `pending` key, or None) when a
    group exists but has never had any entries delivered to it."""
    redis_client = AsyncMock()
    redis_client.xlen.return_value = 0
    redis_client.xpending.return_value = {"pending": 0, "min": None, "max": None, "consumers": []}
    redis_client.xinfo_groups.return_value = [{"name": "pulse-consumers", "lag": 0}]

    info = await get_lag_info(redis_client, "pulse:events", "pulse-consumers")

    assert info.pending_count == 0
    assert info.lag == 0


@pytest.mark.asyncio
async def test_get_consumer_info_happy_path() -> None:
    redis_client = AsyncMock()
    redis_client.xinfo_consumers.return_value = [
        {"name": "consumer-1", "pending": 3, "idle": 120, "inactive": 50},
        {"name": "consumer-2", "pending": 0, "idle": 4000, "inactive": 4000},
    ]

    consumers = await get_consumer_info(redis_client, "pulse:events", "pulse-consumers")

    assert consumers == [
        ConsumerInfo(name="consumer-1", pending=3, idle_ms=120),
        ConsumerInfo(name="consumer-2", pending=0, idle_ms=4000),
    ]


@pytest.mark.asyncio
async def test_get_consumer_info_never_raises_when_group_missing() -> None:
    redis_client = AsyncMock()
    redis_client.xinfo_consumers.side_effect = Exception("NOGROUP")

    consumers = await get_consumer_info(redis_client, "pulse:events", "pulse-consumers")

    assert consumers == []
