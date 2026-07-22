"""I/O boundary for anomaly detection: fetches history from Postgres, calls into the
pure detector/ package, and persists any findings. Best-effort — a failure here is
logged and swallowed, never allowed to block acking the batch (see consumer/main.py's
module docstring: the at-least-once durability guarantee covers raw metrics data, not
this derived analysis).
"""

from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from consumer.aggregator import Rollup
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


def run_detection_for_bucket(
    session_factory: sessionmaker[Session],
    bucket_key: tuple[datetime, str, str],
    rollup: Rollup,
    settings: Settings,
    logger,
) -> None:
    """Best-effort: fetch history, run the fused detectors, persist any findings.

    Purpose: the single call consumer/main.py makes per touched bucket after its
        rollup has been durably upserted.
    Inputs: session_factory; bucket_key — (minute_bucket, service, endpoint); rollup —
        the just-persisted Rollup for this bucket; settings — detector thresholds;
        logger.
    Outputs: None. Any Finding objects are persisted as Anomaly rows; if there are
        none, nothing is written.
    Complexity: dominated by fetch_history + detect_anomalies (see their docstrings).
    Failure cases: never raises — any exception (Postgres error, detector bug) is
        logged and swallowed, since a detection failure must not block XACKing the
        batch that already durably persisted the underlying metrics.
    """
    minute_bucket, service, endpoint = bucket_key
    fetch_limit = max(settings.min_history_buckets, settings.isolation_forest_min_history)
    try:
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
                session.add(
                    Anomaly(
                        minute_bucket=minute_bucket,
                        service=service,
                        endpoint=endpoint,
                        detector=finding.detector,
                        score=finding.score,
                        reason=finding.reason,
                        created_at=datetime.now(timezone.utc),
                    )
                )
            logger.info(
                "anomalies flagged",
                extra={
                    "extra_fields": {
                        "service": service,
                        "endpoint": endpoint,
                        "minute_bucket": minute_bucket.isoformat(),
                        "detectors": [f.detector for f in findings],
                    }
                },
            )
    except Exception as exc:  # noqa: BLE001 - detection is best-effort by design
        logger.error(
            "anomaly detection failed, skipping",
            extra={
                "extra_fields": {"service": service, "endpoint": endpoint, "error": str(exc)}
            },
        )
