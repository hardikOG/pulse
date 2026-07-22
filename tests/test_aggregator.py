"""Unit tests for consumer/aggregator.py — pure functions, no Redis/Postgres needed."""

from datetime import datetime, timezone

import pytest

from consumer.aggregator import bucket_for, compute_rollup, is_error


def test_bucket_for_truncates_to_minute_boundary() -> None:
    ts = datetime(2026, 1, 1, 0, 0, 30, tzinfo=timezone.utc)
    assert bucket_for(ts, 60) == datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def test_bucket_for_exact_boundary_is_itself() -> None:
    ts = datetime(2026, 1, 1, 0, 1, 0, tzinfo=timezone.utc)
    assert bucket_for(ts, 60) == datetime(2026, 1, 1, 0, 1, 0, tzinfo=timezone.utc)


def test_bucket_for_different_window_size() -> None:
    ts = datetime(2026, 1, 1, 0, 7, 30, tzinfo=timezone.utc)
    assert bucket_for(ts, 300) == datetime(2026, 1, 1, 0, 5, 0, tzinfo=timezone.utc)


def test_bucket_for_naive_datetime_raises() -> None:
    naive = datetime(2026, 1, 1, 0, 0, 0)
    with pytest.raises(ValueError):
        bucket_for(naive, 60)


def test_compute_rollup_known_dataset() -> None:
    latencies = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0]
    rollup = compute_rollup(latencies, error_count=2)
    assert rollup.request_count == 10
    assert rollup.error_count == 2
    assert rollup.p50 == pytest.approx(55.0)
    assert rollup.p95 == pytest.approx(95.5)
    assert rollup.p99 == pytest.approx(99.1)


def test_compute_rollup_single_event() -> None:
    rollup = compute_rollup([42.0], error_count=0)
    assert rollup.request_count == 1
    assert rollup.p50 == rollup.p95 == rollup.p99 == pytest.approx(42.0)


def test_compute_rollup_empty_raises() -> None:
    with pytest.raises(ValueError):
        compute_rollup([], error_count=0)


@pytest.mark.parametrize(
    ("status_code", "expected"),
    [(200, False), (301, False), (404, False), (499, False), (500, True), (503, True), (599, True)],
)
def test_is_error_boundary(status_code: int, expected: bool) -> None:
    assert is_error(status_code) is expected
