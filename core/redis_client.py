"""Async Redis client factory — dependency-injected, not a global (mirrors db/session.py)."""

from redis.asyncio import Redis

from core.config import Settings


def make_redis_client(settings: Settings, socket_timeout_seconds: float | None = None) -> Redis:
    """Build an async Redis client for the given settings.

    Purpose: construct a Redis client without a module-level global, so the api and
        consumer processes (and tests) can each own an independently configured client.
    Inputs: settings — a Settings instance providing redis_url and
        redis_socket_timeout_seconds; socket_timeout_seconds — optional override for the
        read/write socket timeout, defaulting to settings.redis_socket_timeout_seconds.
        The consumer needs a larger override here: XREADGROUP's BLOCK argument can hold
        the socket open for up to settings.stream_block_timeout_ms waiting for new
        entries, and if the socket timeout were shorter than that BLOCK duration, every
        idle poll would raise a spurious redis.exceptions.TimeoutError.
    Outputs: a configured redis.asyncio.Redis client with decode_responses=True (so
        callers get str, not bytes) and bounded connect/socket timeouts — a dead or
        unreachable Redis fails fast instead of hanging the fast path indefinitely.
    Complexity: O(1) — does not itself open a connection (lazy, on first command).
    Failure cases: none at construction time; connection errors surface on first use.
    """
    timeout = (
        socket_timeout_seconds
        if socket_timeout_seconds is not None
        else settings.redis_socket_timeout_seconds
    )
    return Redis.from_url(
        settings.redis_url,
        decode_responses=True,
        socket_connect_timeout=settings.redis_socket_timeout_seconds,
        socket_timeout=timeout,
    )
