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


@dataclass(frozen=True)
class ConsumerInfo:
    """A snapshot of one named consumer's activity within its group."""

    name: str
    pending: int
    idle_ms: int


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
        fallback signal either way. `pending_count` is sourced from XPENDING's
        summary form, not XINFO GROUPS' own `pending` field — both are meant to
        report the same count in real Redis, but XPENDING is the purpose-built,
        canonical command for it (and is what this project's hermetic tests exercise
        directly, since fakeredis's XINFO GROUPS does not compute `pending`
        correctly while its XPENDING does).
    Complexity: O(1) — XLEN, the summary form of XPENDING, and XINFO GROUPS are all
        O(1)/O(groups) in Redis.
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
    try:
        pending_summary = await redis_client.xpending(stream, group)
        pending_count = pending_summary.get("pending", 0) if pending_summary else 0
    except Exception:  # noqa: BLE001 - best-effort observability read
        pass

    lag: int | None = None
    try:
        groups = await redis_client.xinfo_groups(stream)
        for candidate in groups:
            if candidate.get("name") == group:
                lag = candidate.get("lag")
                break
    except Exception:  # noqa: BLE001 - best-effort observability read
        pass

    return LagInfo(stream_length=stream_length, pending_count=pending_count, lag=lag)


async def get_consumer_info(redis_client: Redis, stream: str, group: str) -> list[ConsumerInfo]:
    """Fetch per-consumer pending count and idle time within a consumer group.

    Purpose: answers "is the consumer process actually alive and reading," which
        stream-level lag alone cannot — a consumer group can show zero lag simply
        because nothing has been produced recently, even if the consumer itself has
        crashed. `idle_ms` (Redis's time since this consumer's last XREADGROUP call)
        is the closest available proxy for "last processed" without adding new
        state: a live consumer's idle_ms stays bounded by its poll's block timeout,
        while a dead one's grows unboundedly.
    Inputs: redis_client; stream — settings.event_stream; group —
        settings.consumer_group.
    Outputs: one ConsumerInfo per consumer Redis currently knows about in the group
        (empty list if the group/stream doesn't exist yet or none have ever
        connected).
    Complexity: O(c), c = number of consumers in the group.
    Failure cases: never raises — best-effort observability read; returns [] if the
        stream/group is missing or Redis is unreachable.
    """
    try:
        consumers = await redis_client.xinfo_consumers(stream, group)
    except Exception:  # noqa: BLE001 - best-effort observability read
        return []
    return [
        ConsumerInfo(
            name=c.get("name", ""),
            pending=c.get("pending", 0),
            idle_ms=c.get("idle", 0),
        )
        for c in consumers
    ]
