"""Unit tests for detector/statistical.py — zscore_detect and ewma_detect."""

from detector.statistical import ewma_detect, zscore_detect

STABLE_HISTORY = [100.0, 102.0, 98.0, 101.0, 99.0, 100.0, 103.0, 97.0, 100.0, 101.0]
MIN_HISTORY = 10
THRESHOLD = 3.0


def test_zscore_insufficient_history_returns_none() -> None:
    assert zscore_detect(500.0, STABLE_HISTORY[:5], THRESHOLD, MIN_HISTORY) is None


def test_zscore_no_anomaly_within_threshold() -> None:
    assert zscore_detect(101.5, STABLE_HISTORY, THRESHOLD, MIN_HISTORY) is None


def test_zscore_flags_high_spike() -> None:
    result = zscore_detect(1000.0, STABLE_HISTORY, THRESHOLD, MIN_HISTORY)
    assert result is not None
    assert result.score > THRESHOLD


def test_zscore_flags_low_dip() -> None:
    result = zscore_detect(1.0, STABLE_HISTORY, THRESHOLD, MIN_HISTORY)
    assert result is not None
    assert result.score < -THRESHOLD


def test_zscore_zero_variance_history_returns_none() -> None:
    flat_history = [50.0] * MIN_HISTORY
    assert zscore_detect(999.0, flat_history, THRESHOLD, MIN_HISTORY) is None


def test_ewma_insufficient_history_returns_none() -> None:
    assert ewma_detect(500.0, STABLE_HISTORY[:5], 0.3, THRESHOLD, MIN_HISTORY) is None


def test_ewma_no_anomaly_for_stable_history() -> None:
    assert ewma_detect(101.5, STABLE_HISTORY, 0.3, THRESHOLD, MIN_HISTORY) is None


def test_ewma_flags_spike_against_recent_baseline() -> None:
    result = ewma_detect(1000.0, STABLE_HISTORY, 0.3, THRESHOLD, MIN_HISTORY)
    assert result is not None
    assert result.score > THRESHOLD


def test_ewma_zero_variance_history_returns_none() -> None:
    flat_history = [50.0] * MIN_HISTORY
    assert ewma_detect(999.0, flat_history, 0.3, THRESHOLD, MIN_HISTORY) is None


def test_ewma_more_sensitive_than_zscore_to_recent_shift_after_old_outlier() -> None:
    # An old, one-off outlier followed by a long stable run: z-score's equal-weighted
    # stdev stays inflated by the old outlier for as long as it's in the window, while
    # EWMA's recency weighting has largely "forgotten" it — so the same moderately
    # shifted current value should look more anomalous to EWMA than to z-score.
    history = [100.0] * 5 + [500.0] + [100.0] * 20
    current = 160.0

    zscore_result = zscore_detect(current, history, 0.0, MIN_HISTORY)
    ewma_result = ewma_detect(current, history, 0.3, 0.0, MIN_HISTORY)

    assert zscore_result is not None
    assert ewma_result is not None
    assert abs(ewma_result.score) > abs(zscore_result.score)
