"""Shared data shapes for the detector module. No DB/ORM dependency here on purpose —
detector/ stays pure and testable without Postgres; consumer/detection.py is the
boundary that converts between db.models.Metric rows and BucketSample.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class BucketSample:
    """One minute bucket's aggregate values, decoupled from the ORM row that holds them.

    Purpose: the input shape every detector function operates on — both the "current"
        bucket being evaluated and each historical bucket used as baseline.
    Inputs: n/a (data container).
    Outputs: n/a (data container).
    Complexity: n/a.
    Failure cases: n/a.
    """

    request_count: int
    error_count: int
    p50: float
    p95: float
    p99: float

    @property
    def error_rate(self) -> float:
        """Fraction of requests classified as errors, 0.0 if request_count is 0."""
        return self.error_count / self.request_count if self.request_count else 0.0


@dataclass(frozen=True)
class DetectionResult:
    """One detector's raw output for one signal, before it's turned into a Finding."""

    score: float


@dataclass(frozen=True)
class Finding:
    """One anomaly a detector flagged, ready to persist as a row in the anomalies table.

    Purpose: the output shape detector/fusion.py produces — detector/consumer.py maps
        this directly onto db.models.Anomaly's detector/score/reason columns.
    Inputs: n/a (data container).
    Outputs: n/a (data container).
    Complexity: n/a.
    Failure cases: n/a.
    """

    detector: str
    score: float
    reason: str
