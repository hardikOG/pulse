"""Redis stream / consumer-group lag introspection — shared by the consumer's
periodic logging, the dashboard's live lag indicator, and the benchmark report's
peak-lag sampling. Answers the operational question "is the consumer keeping up,
or is the stream backing up behind it?"
"""

from dataclasses import dataclass

from redis.asyncio import Redis


@dataclass(frozen=True)
class LagInfo:
    """A snapshot of stream backlog and consumer-group lag at one point in time."""

    stream_length: int
    pending_count: int
    lag: int | None


async def get_lag_info(redis_client: Redis, stream: str, group: str) -> LagInfo:
    """Fetch current stream length, pending-entry count, and consumer-group lag.

    Purpose: exposes backpressure/backlog health — if the consumer falls behind,
        stream_length grows unbounded and lag increases, which is exactly the
        failure mode ("does Redis Stream grow forever, is there monitoring?") this
        module answers.
    Inputs: redis_client; stream — settings.event_stream; group —
        settings.consumer_group.
    Outputs: LagInfo. `lag` is Redis's own entries-not-yet-delivered count (Redis
        7+'s XINFO GROUPS `lag` field) when available, else None — older Redis
        versions or a group that doesn't track entries-added don't report it, and
        `pending_count` (entries delivered but not yet acked) remains a reliable
        fallback signal either way.
    Complexity: O(1) — XLEN and XINFO GROUPS are both O(1)/O(groups) in Redis.
    Failure cases: never raises — this is a best-effort observability read; if the
        stream/group doesn't exist yet (e.g. queried before the consumer's first
        boot) or Redis is unreachable, returns LagInfo(0, 0, None) rather than
        propagating an error into whatever is just trying to display health.
    """
    try:
        stream_length = await redis_client.xlen(stream)
    except Exception:  # noqa: BLE001 - best-effort observability read
        stream_length = 0

    pending_count = 0
    lag: int | None = None
    try:
        groups = await redis_client.xinfo_groups(stream)
        for candidate in groups:
            if candidate.get("name") == group:
                pending_count = candidate.get("pending", 0)
                lag = candidate.get("lag")
                break
    except Exception:  # noqa: BLE001 - best-effort observability read
        pass

    return LagInfo(stream_length=stream_length, pending_count=pending_count, lag=lag)
