"""Pure Markdown/JSON report rendering — no I/O. benchmark/run.py gathers every
measured value into a BenchmarkReportData; this module only formats it. Every number
here is measured, never an asserted target — see CLAUDE.md's PERFORMANCE section.
"""

import json
import statistics
from dataclasses import asdict, dataclass

from benchmark.metrics import DetectionMetrics, ThroughputVariance


@dataclass(frozen=True)
class SystemInfo:
    """Environment description for the benchmark report."""

    os_name: str
    python_version: str
    cpu_count: int
    total_memory_mb: float
    hardware: str


@dataclass(frozen=True)
class LatencyStats:
    """Summary latency statistics for one measured phase."""

    count: int
    avg_ms: float
    p50_ms: float
    p95_ms: float
    p99_ms: float


@dataclass(frozen=True)
class OperationalStats:
    """Redis/Postgres operational health snapshot — queue/backpressure visibility,
    not just request-level latency (interviewers ask about this)."""

    redis_used_memory_mb: float
    redis_ops_per_sec: float
    postgres_size_mb: float
    peak_pending_count: int
    peak_lag: int | None


@dataclass(frozen=True)
class PipelineThroughput:
    """Distinguishes API-ingest acceptance rate from what the consumer actually
    durably aggregated. In Pulse's design these are measured at different points on
    purpose — see the `note` field for why a third "Redis stream throughput" number
    isn't reported separately.
    """

    api_ingest_events_per_sec: float
    consumer_processed_events_per_sec: float
    note: str = (
        "API ingest throughput and Redis XADD throughput are the same measurement "
        "here, not two separate numbers: POST /events awaits XADD synchronously "
        "before returning, so accepted-per-second already IS Redis-write-per-second. "
        "Consumer processed throughput is measured independently, from Postgres "
        "(total request_count aggregated within the run window / wall-clock "
        "duration) — the gap between it and API ingest throughput (if any) is the "
        "honest signal for consumer backpressure, not an assumption that they match."
    )


@dataclass(frozen=True)
class TimelineEntry:
    """One labeled point in the run's wall-clock timeline, for readability."""

    label: str
    offset_seconds: float


@dataclass(frozen=True)
class BenchmarkReportData:
    """Everything benchmark/run.py measured, bundled for rendering."""

    scenario_name: str
    run_timestamp: str
    system: SystemInfo
    operational: OperationalStats
    pipeline: PipelineThroughput
    timeline: list[TimelineEntry]
    throughput_duration_seconds: float
    throughput_events_sent: int
    throughput_events_failed: int
    throughput_latency: LatencyStats
    throughput_variance: ThroughputVariance
    live_duration_seconds: float
    live_events_sent: int
    live_events_failed: int
    live_latency: LatencyStats
    detection: DetectionMetrics
    warmup_events_sent: int = 0


def compute_latency_stats(latencies_ms: list[float]) -> LatencyStats:
    """Compute avg/p50/p95/p99 from a raw list of per-request latencies.

    Purpose: single, honest percentile computation shared by every phase's report
        section — nearest-rank on the full sample, not an approximation.
    Inputs: latencies_ms — every request's observed latency in milliseconds.
    Outputs: LatencyStats; all zero if latencies_ms is empty (no requests to report).
    Complexity: O(n log n) — sorts the sample.
    Failure cases: none.
    """
    if not latencies_ms:
        return LatencyStats(count=0, avg_ms=0.0, p50_ms=0.0, p95_ms=0.0, p99_ms=0.0)
    sorted_latencies = sorted(latencies_ms)
    n = len(sorted_latencies)

    def _percentile(p: float) -> float:
        return sorted_latencies[min(int(n * p), n - 1)]

    return LatencyStats(
        count=n,
        avg_ms=statistics.mean(sorted_latencies),
        p50_ms=_percentile(0.50),
        p95_ms=_percentile(0.95),
        p99_ms=_percentile(0.99),
    )


def render_markdown(data: BenchmarkReportData) -> str:
    """Render the full benchmark_report.md content.

    Purpose: the single place the report's Markdown shape is defined.
    Inputs: data — every measured value from benchmark/run.py's orchestration.
    Outputs: a Markdown string ready to write to benchmark_report.md.
    Complexity: O(1) beyond the inputs already being computed.
    Failure cases: none.
    """
    api_rate = data.pipeline.api_ingest_events_per_sec
    consumer_rate = data.pipeline.consumer_processed_events_per_sec
    timeline_lines = "\n".join(
        f"- {entry.offset_seconds:>6.1f}s — {entry.label}" for entry in data.timeline
    )
    lag_str = (
        "n/a (Redis < 7 or group not yet created)"
        if data.operational.peak_lag is None
        else str(data.operational.peak_lag)
    )
    tv = data.throughput_variance
    throughput_variance_str = (
        f"mean: {tv.mean:.1f}, stddev: {tv.stddev:.1f}, min: {tv.minimum}, max: {tv.maximum}"
    )
    tl = data.throughput_latency
    throughput_latency_str = (
        f"avg: {tl.avg_ms:.2f}ms, p50: {tl.p50_ms:.2f}ms, "
        f"**p95: {tl.p95_ms:.2f}ms**, p99: {tl.p99_ms:.2f}ms (n={tl.count})"
    )
    live_latency_str = (
        f"avg: {data.live_latency.avg_ms:.2f}ms, p50: {data.live_latency.p50_ms:.2f}ms, "
        f"p95: {data.live_latency.p95_ms:.2f}ms, p99: {data.live_latency.p99_ms:.2f}ms "
        f"(n={data.live_latency.count})"
    )

    return f"""# Pulse Benchmark Report

Generated: {data.run_timestamp}
Scenario: {data.scenario_name}

All numbers below are measured from this run — see CLAUDE.md's PERFORMANCE section:
targets are never hardcoded or asserted ahead of measurement.

## Timeline

{timeline_lines}

## Environment

- OS: {data.system.os_name}
- Python: {data.system.python_version}
- CPU cores: {data.system.cpu_count}
- Total memory: {data.system.total_memory_mb:.0f} MB
- Hardware: {data.system.hardware}

## Pipeline throughput

- **API ingest (= Redis XADD completion): {api_rate:.1f} events/sec**
- **Consumer processed (durably aggregated into Postgres): {consumer_rate:.1f} events/sec**
- {data.pipeline.note}

## Throughput (unpaced concurrent burst)

- Duration: {data.throughput_duration_seconds:.1f}s
- Events sent: {data.throughput_events_sent} ({data.throughput_events_failed} failed)
- **Events/sec (average): {api_rate:.1f}**
- Per-second throughput — {throughput_variance_str}
- Ingest latency — {throughput_latency_str}

## Labeled scenario run ({data.scenario_name})

- Warmup events (backdated history): {data.warmup_events_sent}
- Duration: {data.live_duration_seconds:.1f}s
- Events sent: {data.live_events_sent} ({data.live_events_failed} failed)
- Ingest latency — {live_latency_str}

## Detector precision/recall (against labeled ground truth)

- True positives: {data.detection.true_positives}
- False positives: {data.detection.false_positives}
- False negatives: {data.detection.false_negatives}
- **Precision: {data.detection.precision:.1%}**
- **Recall: {data.detection.recall:.1%}**

## Operational health

- Redis used memory: {data.operational.redis_used_memory_mb:.1f} MB
- Redis ops/sec (instantaneous, sampled at report time): {data.operational.redis_ops_per_sec:.0f}
- Postgres database size: {data.operational.postgres_size_mb:.1f} MB
- Peak consumer-group pending entries observed during the run: {data.operational.peak_pending_count}
- Peak consumer-group lag observed during the run: {lag_str}
"""


def render_json(data: BenchmarkReportData) -> str:
    """Render the same report data as JSON, alongside the Markdown.

    Purpose: benchmark_report.md is for humans; benchmark_report.json is for
        machines — regenerating README numbers, comparing runs over time, or
        plotting trends later without re-parsing Markdown.
    Inputs: data — the same BenchmarkReportData passed to render_markdown.
    Outputs: a JSON string (pretty-printed) with identical content to the Markdown
        report.
    Complexity: O(1) beyond the inputs already being computed.
    Failure cases: none — every field is a plain dataclass of JSON-safe types.
    """
    return json.dumps(asdict(data), indent=2)
