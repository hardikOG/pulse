"""Pulse consumer — the slow path. Reads events from the Redis stream via a consumer
group, aggregates them into per-minute rollups, and upserts into Postgres.

At-least-once, with a documented bounded gap (not exactly-once): a batch's stream
messages are only XACKed after every rollup they touched has been durably upserted,
and acking happens immediately per batch (not deferred) to keep the crash window as
small as possible. On restart, this consumer first drains its own previously-delivered-
but-unacked (pending) entries before reading anything new, so a crash never silently
abandons in-flight work. What is NOT solved: if a crash happens between a bucket's
Postgres commit and the XACK for that same batch, the redelivered replay reconstructs
that bucket from only the redelivered messages (the in-memory sample from before the
crash is gone) and re-upserts, which can under-represent that one bucket by at most one
batch's worth of events. Closing this completely would require a durable per-bucket
watermark (a schema change) or a transactional outbox — out of scope for the fixed
schema and lightweight-infra constraints (see docs/private/ARCHITECTURE_LEDGER.md).
"""

import asyncio
import time
from datetime import datetime, timedelta, timezone

from pydantic import ValidationError
from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from consumer.aggregator import Rollup, bucket_for, compute_rollup, is_error
from core.config import Settings, get_settings
from core.logging import get_logger
from core.redis_client import make_redis_client
from db.models import Base, Metric
from db.session import make_engine, make_session_factory, session_scope
from schemas import EventIn

BucketKey = tuple[datetime, str, str]


class _BucketAccumulator:
    """In-memory running sample for one (minute_bucket, service, endpoint) rollup."""

    __slots__ = ("latencies", "error_count")

    def __init__(self) -> None:
        self.latencies: list[float] = []
        self.error_count = 0

    def add(self, latency_ms: float, error: bool) -> None:
        self.latencies.append(latency_ms)
        if error:
            self.error_count += 1


def _parse_event(fields: dict[str, str], logger) -> EventIn | None:
    """Parse a raw Redis stream entry's fields back into a validated EventIn.

    Purpose: defensive re-validation at the consumer boundary — the API already
        validated before XADD, but the consumer should never crash on a malformed or
        hand-crafted stream entry.
    Inputs: fields — the string->string field dict from one stream entry.
    Outputs: a validated EventIn, or None if the entry is malformed (a "poison
        message").
    Complexity: O(1).
    Failure cases: never raises — catches KeyError/ValueError/ValidationError and
        returns None, logging the reason.
    """
    try:
        return EventIn(
            service=fields["service"],
            endpoint=fields["endpoint"],
            status_code=int(fields["status_code"]),
            latency_ms=float(fields["latency_ms"]),
            ts=datetime.fromisoformat(fields["ts"]),
        )
    except (KeyError, ValueError, ValidationError) as exc:
        logger.error(
            "malformed stream entry, skipping",
            extra={"extra_fields": {"error": str(exc), "fields": fields}},
        )
        return None


def _upsert_rollup(
    session_factory: sessionmaker[Session],
    bucket_key: BucketKey,
    rollup: Rollup,
    settings: Settings,
    logger,
) -> bool:
    """Upsert one bucket's rollup into the metrics table, retrying on transient failure.

    Purpose: durable persistence step for one bucket's aggregate — the boundary after
        which a message covering this bucket is safe to XACK.
    Inputs: session_factory — SQLAlchemy sessionmaker; bucket_key — (minute_bucket,
        service, endpoint); rollup — the computed Rollup; settings — provides retry
        count/backoff; logger.
    Outputs: True if the upsert committed, False if all retries were exhausted.
    Complexity: O(1) per attempt, up to settings.postgres_max_retries attempts.
    Failure cases: never raises — SQLAlchemyError is caught and retried with linear
        backoff; returns False (not an exception) so the caller can decide not to ack.
    """
    minute_bucket, service, endpoint = bucket_key
    stmt = pg_insert(Metric).values(
        minute_bucket=minute_bucket,
        service=service,
        endpoint=endpoint,
        request_count=rollup.request_count,
        error_count=rollup.error_count,
        p50=rollup.p50,
        p95=rollup.p95,
        p99=rollup.p99,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=["minute_bucket", "service", "endpoint"],
        set_={
            "request_count": stmt.excluded.request_count,
            "error_count": stmt.excluded.error_count,
            "p50": stmt.excluded.p50,
            "p95": stmt.excluded.p95,
            "p99": stmt.excluded.p99,
        },
    )
    for attempt in range(1, settings.postgres_max_retries + 1):
        try:
            with session_scope(session_factory) as session:
                session.execute(stmt)
            return True
        except SQLAlchemyError as exc:
            logger.error(
                "postgres upsert failed",
                extra={
                    "extra_fields": {
                        "attempt": attempt,
                        "service": service,
                        "endpoint": endpoint,
                        "error": str(exc),
                    }
                },
            )
            time.sleep(settings.postgres_retry_backoff_seconds * attempt)
    return False


def _evict_stale_buckets(
    buckets: dict[BucketKey, _BucketAccumulator], settings: Settings, logger
) -> None:
    """Drop buckets from memory once their minute is well past, to bound memory use.

    Purpose: prevents unbounded growth of the in-memory accumulator dict over a long
        consumer uptime — a bucket that's several minutes old is done accumulating.
    Inputs: buckets — the live accumulator dict (mutated in place); settings —
        provides aggregation_window_seconds and bucket_eviction_grace_seconds; logger.
    Outputs: None (mutates buckets in place).
    Complexity: O(n) in the number of open buckets.
    Failure cases: none.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(
        seconds=settings.aggregation_window_seconds + settings.bucket_eviction_grace_seconds
    )
    stale_keys = [key for key in buckets if key[0] < cutoff]
    for key in stale_keys:
        del buckets[key]
    if stale_keys:
        logger.info(
            "evicted stale buckets", extra={"extra_fields": {"count": len(stale_keys)}}
        )


async def _ensure_consumer_group(redis_client: Redis, settings: Settings, logger) -> None:
    """Create the consumer group (and stream, if missing), tolerating it already existing.

    Purpose: idempotent startup step — safe to call on every consumer boot.
    Inputs: redis_client; settings — provides event_stream/consumer_group; logger.
    Outputs: None.
    Complexity: O(1).
    Failure cases: re-raises any RedisError other than "already exists" (BUSYGROUP).
    """
    try:
        await redis_client.xgroup_create(
            name=settings.event_stream, groupname=settings.consumer_group, id="0", mkstream=True
        )
        logger.info(
            "consumer group created",
            extra={"extra_fields": {"group": settings.consumer_group}},
        )
    except RedisError as exc:
        if "BUSYGROUP" in str(exc):
            logger.info(
                "consumer group already exists",
                extra={"extra_fields": {"group": settings.consumer_group}},
            )
        else:
            raise


def _process_entries(
    entries: list[tuple[str, dict[str, str]]],
    buckets: dict[BucketKey, _BucketAccumulator],
    settings: Settings,
    logger,
) -> tuple[list[str], set[BucketKey]]:
    """Parse and fold a list of stream entries into the in-memory bucket accumulators.

    Purpose: shared logic between the startup pending-drain and the steady-state read
        loop — both need to parse entries and update accumulators identically.
    Inputs: entries — (message_id, fields) pairs from one XREADGROUP call; buckets —
        the live accumulator dict (mutated in place); settings; logger.
    Outputs: (message_ids, touched_bucket_keys) — every message ID seen (including
        poison messages, which are counted as "seen" so they get acked and never
        retried forever) and the set of bucket keys that gained new data.
    Complexity: O(n) in len(entries).
    Failure cases: none — malformed entries are logged and skipped via _parse_event.
    """
    message_ids: list[str] = []
    touched: set[BucketKey] = set()
    for message_id, fields in entries:
        message_ids.append(message_id)
        event = _parse_event(fields, logger)
        if event is None:
            continue
        bucket_key = (
            bucket_for(event.ts, settings.aggregation_window_seconds),
            event.service,
            event.endpoint,
        )
        buckets.setdefault(bucket_key, _BucketAccumulator()).add(
            event.latency_ms, is_error(event.status_code)
        )
        touched.add(bucket_key)
    return message_ids, touched


async def _flush_and_ack(
    redis_client: Redis,
    session_factory: sessionmaker[Session],
    settings: Settings,
    logger,
    buckets: dict[BucketKey, _BucketAccumulator],
    message_ids: list[str],
    touched: set[BucketKey],
) -> None:
    """Upsert every touched bucket's full current rollup, then ack iff all succeeded.

    Purpose: the durability boundary — a message is only acked once every bucket it
        contributed to has been durably persisted, giving at-least-once semantics.
    Inputs: redis_client; session_factory; settings; logger; buckets — accumulator
        dict (read, not mutated here); message_ids — this batch's stream entry IDs;
        touched — bucket keys this batch added data to.
    Outputs: None.
    Complexity: O(b) Postgres upserts where b = len(touched), plus one XACK call.
    Failure cases: if any bucket's upsert fails after retries, none of this batch's
        messages are acked — they remain pending and are redelivered on the next
        startup's pending-drain (or, if the consumer keeps running, on a future
        XREADGROUP with id="0"), so the batch is retried as a whole rather than
        silently losing the failed bucket's contribution.
    """
    all_ok = True
    for bucket_key in touched:
        rollup = compute_rollup(
            buckets[bucket_key].latencies, buckets[bucket_key].error_count
        )
        if not _upsert_rollup(session_factory, bucket_key, rollup, settings, logger):
            all_ok = False

    if not message_ids:
        return

    if all_ok:
        try:
            await redis_client.xack(settings.event_stream, settings.consumer_group, *message_ids)
        except RedisError as exc:
            logger.error("xack failed", extra={"extra_fields": {"error": str(exc)}})
    else:
        logger.error(
            "batch not acked due to upsert failure; will be redelivered",
            extra={"extra_fields": {"message_count": len(message_ids)}},
        )


async def _drain_pending(
    redis_client: Redis,
    session_factory: sessionmaker[Session],
    settings: Settings,
    logger,
    buckets: dict[BucketKey, _BucketAccumulator],
) -> None:
    """Replay this consumer's own previously-delivered-but-unacked entries.

    Purpose: crash recovery. XREADGROUP with id=">" only ever returns messages never
        before delivered to this group — a restarted consumer that skipped straight to
        "read new" would silently abandon whatever was in-flight when it died. Reading
        with id="0" for this same consumer name returns exactly its own pending
        entries, oldest first, so restart resumes correctly instead of losing track of
        them (see the module docstring for the residual, documented gap this does NOT
        close).
    Inputs: redis_client; session_factory; settings; logger; buckets — accumulator
        dict, populated in place.
    Outputs: None. Runs until the pending list is exhausted.
    Complexity: O(p) in the number of pending entries, in batches of stream_batch_size.
    Failure cases: RedisError during the read is logged and the drain is abandoned for
        this startup — a real Redis outage at boot is surfaced via the steady-state
        loop's own retry/backoff instead of blocking startup forever.
    """
    drained = 0
    while True:
        try:
            response = await redis_client.xreadgroup(
                groupname=settings.consumer_group,
                consumername=settings.consumer_name,
                streams={settings.event_stream: "0"},
                count=settings.stream_batch_size,
            )
        except RedisError as exc:
            logger.error(
                "pending drain read failed, continuing to steady state",
                extra={"extra_fields": {"error": str(exc)}},
            )
            return

        entries = response[0][1] if response else []
        if not entries:
            break

        message_ids, touched = _process_entries(entries, buckets, settings, logger)
        await _flush_and_ack(
            redis_client, session_factory, settings, logger, buckets, message_ids, touched
        )
        drained += len(entries)

    if drained:
        logger.info("drained pending entries", extra={"extra_fields": {"count": drained}})


async def _run_async(settings: Settings, logger) -> None:
    """The consumer's main coroutine: setup, pending-drain, then the steady-state loop.

    Purpose: owns the full lifecycle — Redis/Postgres client construction, consumer
        group setup, crash-recovery drain, and the ongoing read/aggregate/persist/ack
        cycle.
    Inputs: settings; logger.
    Outputs: never returns under normal operation (runs until the process is killed).
    Complexity: O(1) steady-state overhead per poll, dominated by batch size.
    Failure cases: RedisError during steady-state reads is logged and retried with
        backoff rather than crashing the process.
    """
    redis_client = make_redis_client(
        settings, socket_timeout_seconds=(settings.stream_block_timeout_ms / 1000) + 5
    )
    engine = make_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = make_session_factory(engine)

    await _ensure_consumer_group(redis_client, settings, logger)

    buckets: dict[BucketKey, _BucketAccumulator] = {}
    await _drain_pending(redis_client, session_factory, settings, logger, buckets)

    logger.info("consumer entering steady state", extra={"extra_fields": {}})

    while True:
        try:
            response = await redis_client.xreadgroup(
                groupname=settings.consumer_group,
                consumername=settings.consumer_name,
                streams={settings.event_stream: ">"},
                count=settings.stream_batch_size,
                block=settings.stream_block_timeout_ms,
            )
        except RedisError as exc:
            logger.error("redis read failed", extra={"extra_fields": {"error": str(exc)}})
            await asyncio.sleep(settings.redis_socket_timeout_seconds)
            continue

        if not response:
            _evict_stale_buckets(buckets, settings, logger)
            continue

        batch_start = time.perf_counter()
        entries = response[0][1]
        message_ids, touched = _process_entries(entries, buckets, settings, logger)
        await _flush_and_ack(
            redis_client, session_factory, settings, logger, buckets, message_ids, touched
        )
        elapsed_ms = (time.perf_counter() - batch_start) * 1000
        logger.info(
            "batch processed",
            extra={
                "extra_fields": {
                    "size": len(message_ids),
                    "buckets_touched": len(touched),
                    "elapsed_ms": round(elapsed_ms, 2),
                }
            },
        )
        _evict_stale_buckets(buckets, settings, logger)


def run() -> None:
    """Process entrypoint for the consumer (the slow path).

    Purpose: synchronous entrypoint invoked by `python -m consumer.main`, wrapping the
        async main loop.
    Inputs: none (reads configuration from the environment via get_settings()).
    Outputs: none — runs until the process is terminated.
    Complexity: n/a.
    Failure cases: unexpected (non-Redis, non-SQLAlchemy) exceptions propagate and
        crash the process; docker-compose's restart policy brings it back up, and the
        pending-drain on the next boot resumes any unacked work.
    """
    settings = get_settings()
    logger = get_logger("pulse.consumer", settings.log_level)
    asyncio.run(_run_async(settings, logger))


if __name__ == "__main__":
    run()
