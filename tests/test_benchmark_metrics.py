"""Unit tests for benchmark/metrics.py — pure precision/recall computation."""

from datetime import datetime, timezone

from benchmark.metrics import compute_precision_recall, compute_throughput_variance

T1 = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
T2 = datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc)
T3 = datetime(2026, 1, 1, 0, 2, tzinfo=timezone.utc)


def test_perfect_detection_gives_precision_and_recall_of_one() -> None:
    ground_truth = {(T1, "checkout", "/pay"), (T2, "auth", "/login")}
    detected = {(T1, "checkout", "/pay"), (T2, "auth", "/login")}

    metrics = compute_precision_recall(ground_truth, detected)

    assert metrics.true_positives == 2
    assert metrics.false_positives == 0
    assert metrics.false_negatives == 0
    assert metrics.precision == 1.0
    assert metrics.recall == 1.0


def test_missed_anomaly_reduces_recall_not_precision() -> None:
    ground_truth = {(T1, "checkout", "/pay"), (T2, "auth", "/login")}
    detected = {(T1, "checkout", "/pay")}

    metrics = compute_precision_recall(ground_truth, detected)

    assert metrics.true_positives == 1
    assert metrics.false_negatives == 1
    assert metrics.false_positives == 0
    assert metrics.precision == 1.0
    assert metrics.recall == 0.5


def test_extra_detection_reduces_precision_not_recall() -> None:
    ground_truth = {(T1, "checkout", "/pay")}
    detected = {(T1, "checkout", "/pay"), (T3, "search", "/query")}

    metrics = compute_precision_recall(ground_truth, detected)

    assert metrics.true_positives == 1
    assert metrics.false_positives == 1
    assert metrics.false_negatives == 0
    assert metrics.precision == 0.5
    assert metrics.recall == 1.0


def test_no_detections_and_no_ground_truth_gives_zero_not_nan() -> None:
    metrics = compute_precision_recall(set(), set())
    assert metrics.precision == 0.0
    assert metrics.recall == 0.0


def test_no_detections_with_ground_truth_gives_zero_recall() -> None:
    ground_truth = {(T1, "checkout", "/pay")}
    metrics = compute_precision_recall(ground_truth, set())
    assert metrics.recall == 0.0
    assert metrics.false_negatives == 1


def test_throughput_variance_perfectly_stable_rate() -> None:
    # 10 events in each of 3 seconds
    offsets = [s + i * 0.09 for s in range(3) for i in range(10)]
    variance = compute_throughput_variance(offsets, duration_seconds=3.0)
    assert variance.per_second_counts == (10, 10, 10)
    assert variance.mean == 10.0
    assert variance.stddev == 0.0
    assert variance.minimum == 10
    assert variance.maximum == 10


def test_throughput_variance_detects_uneven_rate() -> None:
    # second 0: 20 events (all offsets in [0, 1)), second 1: 0 events,
    # second 2: 10 events (all offsets in [2, 3))
    offsets = [0.04 * i for i in range(20)] + [2.0 + 0.05 * i for i in range(10)]
    variance = compute_throughput_variance(offsets, duration_seconds=3.0)
    assert variance.per_second_counts == (20, 0, 10)
    assert variance.minimum == 0
    assert variance.maximum == 20
    assert variance.stddev > 0.0


def test_throughput_variance_empty_input() -> None:
    variance = compute_throughput_variance([], duration_seconds=5.0)
    assert variance.mean == 0.0
    assert variance.per_second_counts == (0, 0, 0, 0, 0)
