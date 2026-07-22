"""Unit tests for core/redis_lag.py — best-effort stream/group lag introspection."""

from unittest.mock import AsyncMock

import pytest

from core.redis_lag import get_lag_info


@pytest.mark.asyncio
async def test_get_lag_info_happy_path() -> None:
    redis_client = AsyncMock()
    redis_client.xlen.return_value = 42
    redis_client.xinfo_groups.return_value = [
        {"name": "other-group", "pending": 999, "lag": 999},
        {"name": "pulse-consumers", "pending": 3, "lag": 7},
    ]

    info = await get_lag_info(redis_client, "pulse:events", "pulse-consumers")

    assert info.stream_length == 42
    assert info.pending_count == 3
    assert info.lag == 7


@pytest.mark.asyncio
async def test_get_lag_info_missing_group_gives_zero_pending() -> None:
    redis_client = AsyncMock()
    redis_client.xlen.return_value = 10
    redis_client.xinfo_groups.return_value = [{"name": "unrelated-group", "pending": 5, "lag": 1}]

    info = await get_lag_info(redis_client, "pulse:events", "pulse-consumers")

    assert info.stream_length == 10
    assert info.pending_count == 0
    assert info.lag is None


@pytest.mark.asyncio
async def test_get_lag_info_never_raises_when_stream_missing() -> None:
    redis_client = AsyncMock()
    redis_client.xlen.side_effect = Exception("no such key")
    redis_client.xinfo_groups.side_effect = Exception("no such key")

    info = await get_lag_info(redis_client, "pulse:events", "pulse-consumers")

    assert info.stream_length == 0
    assert info.pending_count == 0
    assert info.lag is None


@pytest.mark.asyncio
async def test_get_lag_info_missing_lag_field_defaults_to_none() -> None:
    redis_client = AsyncMock()
    redis_client.xlen.return_value = 5
    redis_client.xinfo_groups.return_value = [{"name": "pulse-consumers", "pending": 2}]

    info = await get_lag_info(redis_client, "pulse:events", "pulse-consumers")

    assert info.pending_count == 2
    assert info.lag is None
