"""Pure, unit-tested aggregation logic — no Redis, no Postgres. Kept separate from
consumer/main.py's I/O loop so the actual math (bucketing, percentiles, error
classification) can be tested exhaustively without any external dependency.
"""

from dataclasses import dataclass
from datetime import datetime, timezone

import numpy as np

# Only server-side (5xx) responses count as errors for aggregation/anomaly purposes.
# 4xx responses (bad auth, validation failures, not-found) often reflect expected
# client behavior rather than a service health problem, and counting them as errors
# would make the error-rate signal noisy and prone to false-positive anomaly alerts.
_ERROR_STATUS_THRESHOLD = 500


@dataclass(frozen=True)
class Rollup:
    """The aggregated stats for one (minute_bucket, service, endpoint) combination."""

    request_count: int
    error_count: int
    p50: float
    p95: float
    p99: float


def bucket_for(ts: datetime, window_seconds: int) -> datetime:
    """Truncate a timestamp down to its aggregation window boundary, in UTC.

    Purpose: maps an event's timestamp to the minute (or other window) bucket it
        belongs to, so events can be grouped for rollup.
    Inputs: ts — a tz-aware datetime (schemas.EventIn always normalizes to UTC-aware
        before this is called — see schemas.py); window_seconds — the bucket width.
    Outputs: a UTC datetime truncated down to the nearest window_seconds boundary.
    Complexity: O(1).
    Failure cases: raises ValueError if ts is naive (no tzinfo). This is checked
        explicitly rather than left implicit: datetime.timestamp() on a naive value
        does NOT raise — it silently assumes the *system's local* timezone, which
        would misbucket events without any visible error.
    """
    if ts.tzinfo is None:
        raise ValueError("bucket_for requires a tz-aware datetime, got a naive one")
    epoch = ts.timestamp()
    bucket_epoch = epoch - (epoch % window_seconds)
    return datetime.fromtimestamp(bucket_epoch, tz=timezone.utc)


def is_error(status_code: int) -> bool:
    """Classify a status code as an error for aggregation purposes.

    Purpose: single source of truth for what counts as an "error" in request_count/
        error_count and, later, the error-rate anomaly detector.
    Inputs: status_code — the HTTP status code of one request.
    Outputs: True if status_code >= 500 (server error), else False.
    Complexity: O(1).
    Failure cases: none.
    """
    return status_code >= _ERROR_STATUS_THRESHOLD


def compute_rollup(latencies: list[float], error_count: int) -> Rollup:
    """Compute exact request_count/error_count/p50/p95/p99 from a bucket's full sample.

    Purpose: the "honest" aggregation step — percentiles are computed directly from
        every latency value observed for the bucket, not estimated or approximated.
    Inputs: latencies — every latency_ms value observed for this bucket so far (the
        caller is responsible for accumulating the full set across batches before
        calling this, so percentiles stay exact as more data arrives); error_count —
        the count of those events classified as errors by is_error().
    Outputs: a Rollup with request_count = len(latencies) and linear-interpolated
        p50/p95/p99 over the full sample.
    Complexity: O(n log n) — numpy.percentile sorts the sample.
    Failure cases: raises ValueError if latencies is empty (a bucket must have at
        least one event to be worth computing).
    """
    if not latencies:
        raise ValueError("cannot compute a rollup from zero events")
    p50, p95, p99 = np.percentile(latencies, [50, 95, 99], method="linear")
    return Rollup(
        request_count=len(latencies),
        error_count=error_count,
        p50=float(p50),
        p95=float(p95),
        p99=float(p99),
    )
