"""Unit tests for detector/fusion.py — the fused, flag-on-either orchestration."""

from detector.fusion import detect_anomalies
from detector.types import BucketSample

MIN_HISTORY = 10
ISOLATION_FOREST_MIN_HISTORY = 30
ZSCORE_THRESHOLD = 3.0
EWMA_ALPHA = 0.3
CONTAMINATION = 0.05

VALID_DETECTORS = {"zscore", "ewma", "isolation_forest"}


def _stable_sample(i: int) -> BucketSample:
    return BucketSample(
        request_count=100 + (i % 5),
        error_count=2 + (i % 2),
        p50=50.0 + (i % 3),
        p95=90.0 + (i % 4),
        p99=110.0 + (i % 3),
    )


STABLE_HISTORY_15 = [_stable_sample(i) for i in range(15)]
STABLE_HISTORY_30 = [_stable_sample(i) for i in range(30)]

EXTREME_SPIKE = BucketSample(request_count=100, error_count=90, p50=800.0, p95=2000.0, p99=3000.0)


def _fuse(current: BucketSample, history: list[BucketSample]) -> list:
    return detect_anomalies(
        current,
        history,
        ZSCORE_THRESHOLD,
        EWMA_ALPHA,
        CONTAMINATION,
        MIN_HISTORY,
        ISOLATION_FOREST_MIN_HISTORY,
    )


def test_no_findings_for_stable_current() -> None:
    current = BucketSample(request_count=101, error_count=2, p50=51.0, p95=91.0, p99=111.0)
    assert _fuse(current, STABLE_HISTORY_30) == []


def test_empty_history_yields_no_findings() -> None:
    current = BucketSample(request_count=101, error_count=2, p50=51.0, p95=91.0, p99=111.0)
    assert _fuse(current, []) == []


def test_extreme_spike_flagged_by_multiple_detectors() -> None:
    findings = _fuse(EXTREME_SPIKE, STABLE_HISTORY_30)
    assert len(findings) >= 2
    detector_names = {f.detector for f in findings}
    assert detector_names.issubset(VALID_DETECTORS)
    assert "zscore" in detector_names


def test_all_findings_have_valid_detector_names_and_nonempty_reason() -> None:
    findings = _fuse(EXTREME_SPIKE, STABLE_HISTORY_30)
    assert findings
    for finding in findings:
        assert finding.detector in VALID_DETECTORS
        assert finding.reason
        assert isinstance(finding.score, float)


def test_isolation_forest_needs_more_history_than_statistical_detectors() -> None:
    # Regression test for a real finding from Phase 4's live gate verification:
    # IsolationForest at n=10 (the statistical detectors' minimum) failed to flag an
    # obvious injected outlier that zscore/ewma both caught cleanly — an inherent
    # small-sample limitation of random-split isolation, not a bug. With only 15
    # history points (enough for zscore/ewma at MIN_HISTORY=10, not enough for
    # isolation forest at ISOLATION_FOREST_MIN_HISTORY=30), isolation_forest must not
    # appear among the findings, while the statistical detectors still fire.
    findings = _fuse(EXTREME_SPIKE, STABLE_HISTORY_15)
    detector_names = {f.detector for f in findings}
    assert "isolation_forest" not in detector_names
    assert "zscore" in detector_names


def test_isolation_forest_fires_once_it_has_enough_history() -> None:
    findings = _fuse(EXTREME_SPIKE, STABLE_HISTORY_30)
    detector_names = {f.detector for f in findings}
    assert "isolation_forest" in detector_names
