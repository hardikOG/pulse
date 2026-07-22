"""Unit tests for benchmark/report.py — pure latency stats + report rendering."""

import json

from benchmark.metrics import DetectionMetrics, ThroughputVariance
from benchmark.report import (
    BenchmarkReportData,
    OperationalStats,
    PipelineThroughput,
    SystemInfo,
    TimelineEntry,
    compute_latency_stats,
    render_json,
    render_markdown,
)


def test_compute_latency_stats_empty() -> None:
    stats = compute_latency_stats([])
    assert stats.count == 0
    assert stats.avg_ms == 0.0
    assert stats.p95_ms == 0.0


def test_compute_latency_stats_known_dataset() -> None:
    latencies = [float(i) for i in range(1, 101)]  # 1..100
    stats = compute_latency_stats(latencies)
    assert stats.count == 100
    assert stats.p50_ms == 51.0
    assert stats.p95_ms == 96.0
    assert stats.p99_ms == 100.0


def _sample_report_data() -> BenchmarkReportData:
    system = SystemInfo(
        os_name="Linux 6.0",
        python_version="3.11.0",
        cpu_count=4,
        total_memory_mb=8192.0,
        hardware="x86_64",
    )
    detection = DetectionMetrics(
        true_positives=2, false_positives=1, false_negatives=0, precision=0.667, recall=1.0
    )
    latency = compute_latency_stats([1.0, 2.0, 3.0])
    variance = ThroughputVariance(per_second_counts=(10, 10, 10), mean=10.0, stddev=0.0, minimum=10, maximum=10)
    operational = OperationalStats(
        redis_used_memory_mb=12.5,
        redis_ops_per_sec=340.0,
        postgres_size_mb=48.2,
        peak_pending_count=3,
        peak_lag=1,
    )
    pipeline = PipelineThroughput(
        api_ingest_events_per_sec=100.0, consumer_processed_events_per_sec=99.5
    )
    timeline = [
        TimelineEntry(label="warmup started", offset_seconds=0.0),
        TimelineEntry(label="live phase finished", offset_seconds=240.0),
    ]
    return BenchmarkReportData(
        scenario_name="steady",
        run_timestamp="2026-01-01T00:00:00+00:00",
        system=system,
        operational=operational,
        pipeline=pipeline,
        timeline=timeline,
        throughput_duration_seconds=15.0,
        throughput_events_sent=1500,
        throughput_events_failed=0,
        throughput_latency=latency,
        throughput_variance=variance,
        live_duration_seconds=240.0,
        live_events_sent=4800,
        live_events_failed=0,
        live_latency=latency,
        detection=detection,
        warmup_events_sent=1920,
    )


def test_render_markdown_includes_key_numbers() -> None:
    report = render_markdown(_sample_report_data())

    assert "steady" in report
    assert "66.7%" in report  # precision
    assert "100.0%" in report  # recall
    assert "Linux 6.0" in report
    assert "12.5" in report  # redis memory
    assert "warmup started" in report  # timeline
    assert "XADD" in report  # pipeline throughput note


def test_render_json_round_trips_key_fields() -> None:
    data = _sample_report_data()
    parsed = json.loads(render_json(data))

    assert parsed["scenario_name"] == "steady"
    assert parsed["detection"]["precision"] == 0.667
    assert parsed["operational"]["redis_used_memory_mb"] == 12.5
    assert parsed["pipeline"]["api_ingest_events_per_sec"] == 100.0
    assert parsed["timeline"][0]["label"] == "warmup started"
