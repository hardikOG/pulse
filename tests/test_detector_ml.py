"""Unit tests for detector/ml.py — isolation_forest_detect."""

from detector.ml import isolation_forest_detect
from detector.types import BucketSample

MIN_HISTORY = 10
CONTAMINATION = 0.05


def _cluster_sample(i: int) -> BucketSample:
    return BucketSample(
        request_count=100 + (i % 5),
        error_count=2 + (i % 2),
        p50=50.0 + (i % 3),
        p95=90.0 + (i % 4),
        p99=110.0 + (i % 3),
    )


HISTORY = [_cluster_sample(i) for i in range(15)]


def test_insufficient_history_returns_none() -> None:
    current = _cluster_sample(0)
    result = isolation_forest_detect(current, HISTORY[:5], CONTAMINATION, MIN_HISTORY)
    assert result is None


def test_normal_point_not_flagged() -> None:
    current = BucketSample(request_count=102, error_count=2, p50=51.0, p95=91.0, p99=111.0)
    result = isolation_forest_detect(current, HISTORY, CONTAMINATION, MIN_HISTORY)
    assert result is None


def test_clear_outlier_flagged() -> None:
    current = BucketSample(request_count=100, error_count=80, p50=500.0, p95=900.0, p99=1200.0)
    result = isolation_forest_detect(current, HISTORY, CONTAMINATION, MIN_HISTORY)
    assert result is not None
    # scikit-learn's convention: lower (more negative) score_samples means more anomalous.
    assert result.score < 0


def test_deterministic_with_fixed_random_state() -> None:
    current = BucketSample(request_count=100, error_count=80, p50=500.0, p95=900.0, p99=1200.0)
    result1 = isolation_forest_detect(current, HISTORY, CONTAMINATION, MIN_HISTORY)
    result2 = isolation_forest_detect(current, HISTORY, CONTAMINATION, MIN_HISTORY)
    assert result1 is not None and result2 is not None
    assert result1.score == result2.score
