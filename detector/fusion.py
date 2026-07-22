"""Combines the statistical and ML detectors into one fused anomaly check.

Flags on EITHER any statistical detector or the ML detector firing — this is a pure
orchestration function; it takes plain scalar parameters rather than a Settings object
so detector/ has no dependency on core/config.py.
"""

from detector.ml import isolation_forest_detect
from detector.statistical import ewma_detect, zscore_detect
from detector.types import BucketSample, Finding


def detect_anomalies(
    current: BucketSample,
    history: list[BucketSample],
    zscore_threshold: float,
    ewma_alpha: float,
    isolation_forest_contamination: float,
    min_history: int,
    isolation_forest_min_history: int,
) -> list[Finding]:
    """Run all five checks (zscore x2, ewma x2, isolation forest x1) and collect hits.

    Purpose: the single entry point consumer/detection.py calls after each rollup.
    Inputs: current — the bucket just upserted; history — prior buckets for the same
        (service, endpoint), chronological order, sized to cover the LARGER of the two
        min_history requirements (the caller, consumer/detection.py, fetches enough
        for whichever detector needs more); zscore_threshold, ewma_alpha,
        isolation_forest_contamination — detector parameters; min_history — window
        size for the statistical detectors; isolation_forest_min_history — separate,
        larger window for IsolationForest (see detector/ml.py and
        docs/private/ARCHITECTURE_LEDGER.md for why it needs more samples than
        zscore/ewma to reliably isolate an outlier — measured, not assumed).
    Outputs: a list of Finding, zero or more — zero if nothing fired or history is
        too short for any detector to evaluate. detector is always one of "zscore",
        "ewma", "isolation_forest" (the fixed schema's three values); which *signal*
        fired (latency vs error-rate) is described in reason, not a separate field.
    Complexity: O(n) for the statistical checks, O(n log n) for isolation forest,
        n = len(history).
    Failure cases: never raises — each underlying detector already returns None rather
        than raising when it can't evaluate.
    """
    findings: list[Finding] = []
    # Statistical detectors use only the most recent min_history buckets (their
    # designed window), even if the caller supplied a longer history for isolation
    # forest's sake — history is chronological, so the tail is the most recent.
    stat_history = history[-min_history:] if min_history else history
    latency_history = [sample.p95 for sample in stat_history]
    error_rate_history = [sample.error_rate for sample in stat_history]

    z_latency = zscore_detect(current.p95, latency_history, zscore_threshold, min_history)
    if z_latency is not None:
        findings.append(
            Finding(
                detector="zscore",
                score=z_latency.score,
                reason=f"p95 latency {current.p95:.1f}ms is {z_latency.score:.2f} "
                f"std devs from the {len(stat_history)}-bucket baseline",
            )
        )

    z_error = zscore_detect(
        current.error_rate, error_rate_history, zscore_threshold, min_history
    )
    if z_error is not None:
        findings.append(
            Finding(
                detector="zscore",
                score=z_error.score,
                reason=f"error rate {current.error_rate:.3f} is {z_error.score:.2f} "
                f"std devs from the {len(stat_history)}-bucket baseline",
            )
        )

    e_latency = ewma_detect(
        current.p95, latency_history, ewma_alpha, zscore_threshold, min_history
    )
    if e_latency is not None:
        findings.append(
            Finding(
                detector="ewma",
                score=e_latency.score,
                reason=f"p95 latency {current.p95:.1f}ms is {e_latency.score:.2f} "
                f"std devs from the EWMA baseline (alpha={ewma_alpha})",
            )
        )

    e_error = ewma_detect(
        current.error_rate, error_rate_history, ewma_alpha, zscore_threshold, min_history
    )
    if e_error is not None:
        findings.append(
            Finding(
                detector="ewma",
                score=e_error.score,
                reason=f"error rate {current.error_rate:.3f} is {e_error.score:.2f} "
                f"std devs from the EWMA baseline (alpha={ewma_alpha})",
            )
        )

    iso = isolation_forest_detect(
        current, history, isolation_forest_contamination, isolation_forest_min_history
    )
    if iso is not None:
        findings.append(
            Finding(
                detector="isolation_forest",
                score=iso.score,
                reason=f"multivariate outlier across request_count/error_rate/p50/p95/p99 "
                f"(isolation forest score {iso.score:.3f}, lower = more anomalous)",
            )
        )

    return findings
