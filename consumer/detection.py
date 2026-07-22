"""I/O boundary for anomaly detection: fetches history from Postgres, calls into the
pure detector/ package, and persists any findings. Best-effort — a failure here is
logged and swallowed, never allowed to block acking the batch (see consumer/main.py's
module docstring: the at-least-once durability guarantee covers raw metrics data, not
this derived analysis).

Live-update publish and webhook dispatch happen only AFTER an anomaly is durably
committed (never before) — findings are only ever notified about once they're real,
persisted rows, consistent with the "don't announce what might not have happened" rule
already applied to acking.
"""

import asyncio
from datetime import datetime, timezone

import httpx
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from consumer.aggregator import Rollup
from consumer.broadcast import build_anomaly_message, publish
from consumer.webhook import build_webhook_payload, dispatch_webhook
from core.config import Settings
from db.models import Anomaly, Metric
from db.session import session_scope
from detector.fusion import detect_anomalies
from detector.types import BucketSample


def fetch_history(
    session: Session, service: str, endpoint: str, before: datetime, limit: int
) -> list[BucketSample]:
    """Fetch the most recent buckets strictly before `before` for one (service, endpoint).

    Purpose: builds the baseline window detect_anomalies compares the current bucket
        against — deliberately reads from Postgres (not an in-memory cache) so
        detection needs no crash-recovery story of its own; the data it needs is
        already durable.
    Inputs: session; service/endpoint — exact match; before — exclusive upper bound
        (the current bucket's own minute_bucket, never included in its own baseline);
        limit — max buckets to fetch.
    Outputs: BucketSample list in chronological order (oldest first), as required by
        the statistical detectors. May be shorter than `limit` (or empty) if the
        (service, endpoint) doesn't have that much history yet.
    Complexity: O(limit) via the existing ix_metrics_service_bucket index.
    Failure cases: propagates SQLAlchemyError to the caller, which treats detection as
        best-effort and catches it.
    """
    stmt = (
        select(Metric)
        .where(
            Metric.service == service, Metric.endpoint == endpoint, Metric.minute_bucket < before
        )
        .order_by(Metric.minute_bucket.desc())
        .limit(limit)
    )
    rows = session.execute(stmt).scalars().all()
    chronological = list(reversed(rows))
    return [
        BucketSample(
            request_count=row.request_count,
            error_count=row.error_count,
            p50=row.p50,
            p95=row.p95,
            p99=row.p99,
        )
        for row in chronological
    ]


async def run_detection_for_bucket(
    session_factory: sessionmaker[Session],
    bucket_key: tuple[datetime, str, str],
    rollup: Rollup,
    settings: Settings,
    logger,
    redis_client: Redis,
    httpx_client: httpx.AsyncClient,
    background_tasks: set[asyncio.Task],
) -> None:
    """Best-effort: fetch history, run the fused detectors, persist and notify findings.

    Purpose: the single call consumer/main.py makes per touched bucket after its
        rollup has been durably upserted.
    Inputs: session_factory; bucket_key — (minute_bucket, service, endpoint); rollup —
        the just-persisted Rollup for this bucket; settings — detector thresholds;
        logger; redis_client — for the live-update Pub/Sub publish; httpx_client —
        shared client for webhook delivery; background_tasks — the consumer's set of
        in-flight webhook tasks (see consumer/main.py — tasks add themselves and are
        discarded on completion, preventing the asyncio "task garbage-collected
        mid-flight" pitfall).
    Outputs: None. Any Finding objects are persisted as Anomaly rows; each committed
        anomaly is then published to the live-updates channel and dispatched to the
        configured webhook (if any) as an untracked-by-caller background task.
    Complexity: dominated by fetch_history + detect_anomalies (see their docstrings),
        plus O(f) publishes for f findings.
    Failure cases: never raises — any exception (Postgres error, detector bug) is
        logged and swallowed, since a detection failure must not block XACKing the
        batch that already durably persisted the underlying metrics. Publish/webhook
        failures are handled (logged, non-raising) inside publish()/dispatch_webhook()
        themselves.
    """
    minute_bucket, service, endpoint = bucket_key
    fetch_limit = max(settings.min_history_buckets, settings.isolation_forest_min_history)
    try:
        anomalies: list[Anomaly] = []
        with session_scope(session_factory) as session:
            history = fetch_history(session, service, endpoint, minute_bucket, fetch_limit)
            current = BucketSample(
                request_count=rollup.request_count,
                error_count=rollup.error_count,
                p50=rollup.p50,
                p95=rollup.p95,
                p99=rollup.p99,
            )
            findings = detect_anomalies(
                current,
                history,
                settings.zscore_threshold,
                settings.ewma_alpha,
                settings.isolation_forest_contamination,
                settings.min_history_buckets,
                settings.isolation_forest_min_history,
            )
            if not findings:
                return
            for finding in findings:
                anomaly = Anomaly(
                    minute_bucket=minute_bucket,
                    service=service,
                    endpoint=endpoint,
                    detector=finding.detector,
                    score=finding.score,
                    reason=finding.reason,
                    created_at=datetime.now(timezone.utc),
                )
                session.add(anomaly)
                anomalies.append(anomaly)
            # session_scope commits here; expire_on_commit=False (db/session.py) keeps
            # the anomaly objects' attributes, including their now-DB-assigned ids,
            # readable after the `with` block exits below.

        logger.info(
            "anomalies flagged",
            extra={
                "extra_fields": {
                    "service": service,
                    "endpoint": endpoint,
                    "minute_bucket": minute_bucket.isoformat(),
                    "detectors": [a.detector for a in anomalies],
                }
            },
        )

        for anomaly in anomalies:
            live_message = build_anomaly_message(
                anomaly.id,
                anomaly.service,
                anomaly.endpoint,
                anomaly.minute_bucket,
                anomaly.detector,
                anomaly.score,
                anomaly.reason,
                anomaly.created_at,
            )
            await publish(redis_client, settings.live_updates_channel, live_message, logger)

            webhook_payload = build_webhook_payload(
                anomaly.id,
                anomaly.minute_bucket,
                anomaly.service,
                anomaly.endpoint,
                anomaly.detector,
                anomaly.score,
                anomaly.reason,
            )
            task = asyncio.create_task(
                dispatch_webhook(httpx_client, settings, webhook_payload, logger)
            )
            background_tasks.add(task)
            task.add_done_callback(background_tasks.discard)
    except Exception as exc:  # noqa: BLE001 - detection is best-effort by design
        logger.error(
            "anomaly detection failed, skipping",
            extra={
                "extra_fields": {"service": service, "endpoint": endpoint, "error": str(exc)}
            },
        )
