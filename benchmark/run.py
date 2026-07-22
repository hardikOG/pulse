"""Benchmark harness entrypoint. Orchestrates the I/O: runs simulator/ traffic phases
against a live Pulse deployment, queries Postgres/Redis for what actually happened,
and writes benchmark_report.md + benchmark_report.json.

Must be run from inside the Docker network (e.g. `docker compose --profile benchmark
run --rm benchmark`), not from the host — see docs/private/ARCHITECTURE_LEDGER.md:
Docker Desktop's host<->container port-forwarding on this dev machine adds a ~40ms
artifact to POST-with-body requests that would make the latency numbers meaningless.
"""

import argparse
import asyncio
import platform
import time
from datetime import datetime, timedelta, timezone

import httpx
import psutil
from redis.asyncio import Redis
from sqlalchemy import func, select, text
from sqlalchemy.orm import sessionmaker

from benchmark.metrics import compute_precision_recall, compute_throughput_variance
from benchmark.report import (
    BenchmarkReportData,
    LatencyStats,
    OperationalStats,
    PipelineThroughput,
    SystemInfo,
    TimelineEntry,
    compute_latency_stats,
    render_json,
    render_markdown,
)
from core.config import get_settings
from core.redis_client import make_redis_client
from core.redis_lag import get_lag_info
from db.models import Anomaly, Metric
from db.session import make_engine, make_session_factory
from simulator.generator import (
    BURST_ENDPOINT,
    BURST_SERVICE,
    run_live_phase,
    run_throughput_burst,
    run_warmup,
)
from simulator.scenarios import SCENARIOS, Scenario


def _gather_system_info() -> SystemInfo:
    """Collect CPU/RAM/OS/Python/hardware info for the report's Environment section.

    Purpose: satisfies CLAUDE.md's Benchmark Report Format — real measured
        environment details, not a placeholder.
    Inputs: none.
    Outputs: a SystemInfo snapshot of the machine running the benchmark.
    Complexity: O(1).
    Failure cases: none — psutil/platform calls used here don't raise in normal
        operation.
    """
    return SystemInfo(
        os_name=f"{platform.system()} {platform.release()}",
        python_version=platform.python_version(),
        cpu_count=psutil.cpu_count(logical=True) or 0,
        total_memory_mb=psutil.virtual_memory().total / (1024 * 1024),
        hardware=platform.processor() or platform.machine(),
    )


async def _sample_peak_lag(
    redis_client: Redis, settings, stop_event: asyncio.Event
) -> tuple[int, int | None]:
    """Poll consumer-group lag every 2s until stop_event is set, tracking the peak.

    Purpose: the live phase's queue health over time — a single end-of-run snapshot
        would miss a lag spike that the consumer later caught up from.
    Inputs: redis_client; settings; stop_event — set by the caller once the live
        phase finishes.
    Outputs: (peak_pending_count, peak_lag) — peak_lag is None if Redis never
        reported a lag value (see core.redis_lag.get_lag_info).
    Complexity: O(1) per sample, sampling every 2s for the caller-controlled duration.
    Failure cases: never raises — get_lag_info is already best-effort.
    """
    peak_pending = 0
    peak_lag: int | None = None
    while not stop_event.is_set():
        info = await get_lag_info(redis_client, settings.event_stream, settings.consumer_group)
        peak_pending = max(peak_pending, info.pending_count)
        if info.lag is not None:
            peak_lag = max(peak_lag or 0, info.lag)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=2.0)
        except TimeoutError:
            pass
    return peak_pending, peak_lag


async def _query_redis_stats(redis_client: Redis) -> tuple[float, float]:
    """Fetch Redis used memory and instantaneous ops/sec.

    Purpose: operational visibility CLAUDE.md's benchmark format calls for — real
        Redis health at report time, not a placeholder.
    Inputs: redis_client.
    Outputs: (used_memory_mb, instantaneous_ops_per_sec). (0.0, 0.0) if the INFO
        calls fail for any reason.
    Complexity: O(1).
    Failure cases: never raises — best-effort observability read.
    """
    try:
        memory_info = await redis_client.info("memory")
        stats_info = await redis_client.info("stats")
        used_memory_mb = memory_info.get("used_memory", 0) / (1024 * 1024)
        ops_per_sec = float(stats_info.get("instantaneous_ops_per_sec", 0))
        return used_memory_mb, ops_per_sec
    except Exception:  # noqa: BLE001 - best-effort observability read
        return 0.0, 0.0


def _query_postgres_size_mb(session_factory: sessionmaker) -> float:
    """Fetch the Postgres database's on-disk size.

    Purpose: operational visibility — how much storage this run's rollups/anomalies
        actually consumed.
    Inputs: session_factory.
    Outputs: size in MB, or 0.0 if the query fails.
    Complexity: O(1).
    Failure cases: never raises — best-effort observability read.
    """
    session = session_factory()
    try:
        size_bytes = session.execute(text("SELECT pg_database_size(current_database())")).scalar_one()
        return size_bytes / (1024 * 1024)
    except Exception:  # noqa: BLE001 - best-effort observability read
        return 0.0
    finally:
        session.close()


def _query_consumer_processed_count(
    session_factory: sessionmaker, service: str, endpoint: str, since: datetime
) -> int:
    """Sum request_count durably aggregated for one (service, endpoint) since `since`.

    Purpose: the "consumer processed throughput" half of the pipeline-throughput
        comparison — measured independently from Postgres, not assumed to equal the
        API's accepted count.
    Inputs: session_factory; service/endpoint — exact match (the throughput burst's
        dedicated BURST_SERVICE/BURST_ENDPOINT, isolated from all other traffic);
        since — only buckets at/after this timestamp count.
    Outputs: total request_count summed across matching rows.
    Complexity: O(n) in matching rows.
    Failure cases: propagates on Postgres connectivity failure — same reasoning as
        _query_detected_anomalies.
    """
    session = session_factory()
    try:
        stmt = select(func.coalesce(func.sum(Metric.request_count), 0)).where(
            Metric.service == service, Metric.endpoint == endpoint, Metric.minute_bucket >= since
        )
        return session.execute(stmt).scalar_one()
    finally:
        session.close()


def _query_detected_anomalies(
    session_factory: sessionmaker, since: datetime
) -> set[tuple[datetime, str, str]]:
    """Fetch the distinct (minute_bucket, service, endpoint) buckets flagged since `since`.

    Purpose: builds the "detected" set compute_precision_recall compares against
        ground truth. DISTINCT collapses multiple detectors firing on the same
        bucket into one entry — precision/recall here is about bucket-level
        detection, not per-detector agreement (see benchmark/metrics.py).
    Inputs: session_factory; since — only buckets at/after this timestamp count
        (excludes anomalies from a prior benchmark run or warmup).
    Outputs: a set of (minute_bucket, service, endpoint) tuples.
    Complexity: O(n) in matching anomaly rows.
    Failure cases: none raised — a query failure here is a real benchmark-run error
        and is allowed to propagate (unlike the consumer's best-effort detection).
    """
    session = session_factory()
    try:
        stmt = (
            select(Anomaly.minute_bucket, Anomaly.service, Anomaly.endpoint)
            .where(Anomaly.minute_bucket >= since)
            .distinct()
        )
        return set(session.execute(stmt).all())
    finally:
        session.close()


async def _run(
    scenario: Scenario,
    target_url: str,
    throughput_seconds: float,
    throughput_concurrency: int,
    output_path: str,
) -> None:
    """Run the full benchmark: warmup, throughput burst, labeled live phase, then report.

    Purpose: the orchestration this module exists for — see module docstring for
        each phase's purpose.
    Inputs: scenario; target_url — the /events endpoint to hit; throughput_seconds/
        throughput_concurrency — burst phase tuning; output_path — where to write
        the rendered Markdown report (the JSON report is written alongside it with
        a .json extension).
    Outputs: None — writes output_path (+ .json) as a side effect and prints a
        summary.
    Complexity: dominated by scenario.live_duration_seconds (real wall-clock time).
    Failure cases: propagates on Postgres connectivity failure when querying
        detected anomalies — a benchmark run that can't verify its own results isn't
        a successful run.
    """
    settings = get_settings()
    engine = make_engine(settings)
    session_factory = make_session_factory(engine)
    lag_redis = make_redis_client(settings)
    events_url = f"{target_url}/events"
    timeline: list[TimelineEntry] = []
    run_start = time.perf_counter()

    def _mark(label: str) -> None:
        timeline.append(TimelineEntry(label=label, offset_seconds=time.perf_counter() - run_start))

    async with httpx.AsyncClient(timeout=10.0) as client:
        _mark("warmup started")
        print(f"[1/4] warmup: backfilling {scenario.warmup_minutes} minutes of history...")
        warmed = await run_warmup(client, events_url, scenario)
        print(f"      {warmed} warmup events accepted")
        _mark("warmup finished")

        print(f"[2/4] throughput burst: {throughput_seconds:.0f}s at concurrency={throughput_concurrency}...")
        burst_start = datetime.now(timezone.utc)
        throughput = await run_throughput_burst(client, events_url, throughput_seconds, throughput_concurrency)
        print(f"      {throughput.events_sent} sent, {throughput.events_failed} failed")
        _mark("throughput burst finished")

        print(f"[3/4] labeled live phase: scenario={scenario.name}, {scenario.live_duration_seconds:.0f}s...")
        _mark("live phase started")
        live_start = datetime.now(timezone.utc)
        stop_lag_sampling = asyncio.Event()
        lag_task = asyncio.create_task(_sample_peak_lag(lag_redis, settings, stop_lag_sampling))
        live = await run_live_phase(client, events_url, scenario)
        stop_lag_sampling.set()
        peak_pending, peak_lag = await lag_task
        print(f"      {live.events_sent} sent, {live.events_failed} failed, {len(live.labels)} anomalies injected")
        _mark("live phase finished")

    grace_seconds = 15
    print(f"[4/4] waiting {grace_seconds}s for the consumer to finish processing, then querying results...")
    await asyncio.sleep(grace_seconds)
    _mark("grace period elapsed")

    # metrics.minute_bucket/anomalies.minute_bucket are always truncated to :00
    # seconds; comparing them against a `since` that still has a live wall-clock
    # second component (e.g. "started at :47") would incorrectly exclude the very
    # bucket that was actively forming at that moment. Floor to the minute, then
    # step back one more full minute as a safety margin, rather than a small fixed
    # second offset that doesn't actually solve the truncation mismatch.
    def _bucket_safe_since(moment: datetime) -> datetime:
        return moment.replace(second=0, microsecond=0) - timedelta(minutes=1)

    ground_truth = {(label.minute_bucket, label.service, label.endpoint) for label in live.labels}
    detected = _query_detected_anomalies(session_factory, _bucket_safe_since(live_start))
    detection = compute_precision_recall(ground_truth, detected)

    consumer_processed = _query_consumer_processed_count(
        session_factory, BURST_SERVICE, BURST_ENDPOINT, _bucket_safe_since(burst_start)
    )
    redis_memory_mb, redis_ops = await _query_redis_stats(lag_redis)
    postgres_size_mb = _query_postgres_size_mb(session_factory)
    await lag_redis.aclose()

    report_data = BenchmarkReportData(
        scenario_name=scenario.name,
        run_timestamp=datetime.now(timezone.utc).isoformat(),
        system=_gather_system_info(),
        operational=OperationalStats(
            redis_used_memory_mb=redis_memory_mb,
            redis_ops_per_sec=redis_ops,
            postgres_size_mb=postgres_size_mb,
            peak_pending_count=peak_pending,
            peak_lag=peak_lag,
        ),
        pipeline=PipelineThroughput(
            api_ingest_events_per_sec=(
                throughput.events_sent / throughput.duration_seconds if throughput.duration_seconds else 0.0
            ),
            consumer_processed_events_per_sec=(
                consumer_processed / throughput.duration_seconds if throughput.duration_seconds else 0.0
            ),
        ),
        timeline=timeline,
        throughput_duration_seconds=throughput.duration_seconds,
        throughput_events_sent=throughput.events_sent,
        throughput_events_failed=throughput.events_failed,
        throughput_latency=compute_latency_stats(throughput.latencies_ms),
        throughput_variance=compute_throughput_variance(
            throughput.send_offsets_seconds, throughput.duration_seconds
        ),
        live_duration_seconds=live.duration_seconds,
        live_events_sent=live.events_sent,
        live_events_failed=live.events_failed,
        live_latency=compute_latency_stats(live.latencies_ms),
        detection=detection,
        warmup_events_sent=warmed,
    )

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(render_markdown(report_data))
    json_path = output_path.rsplit(".", 1)[0] + ".json"
    with open(json_path, "w", encoding="utf-8") as f:
        f.write(render_json(report_data))

    print(f"\nReports written to {output_path} and {json_path}")
    print(
        f"precision={detection.precision:.1%} recall={detection.recall:.1%} "
        f"(TP={detection.true_positives} FP={detection.false_positives} FN={detection.false_negatives})"
    )
    print(
        f"API ingest: {report_data.pipeline.api_ingest_events_per_sec:.1f}/s, "
        f"consumer processed: {report_data.pipeline.consumer_processed_events_per_sec:.1f}/s"
    )


def main() -> None:
    """CLI entrypoint: parse args and run the benchmark.

    Purpose: `python -m benchmark.run --scenario steady` from inside the Docker
        network — see module docstring for why it must run there.
    Inputs: command-line arguments (--scenario, --url, --throughput-seconds,
        --throughput-concurrency, --output, --live-seconds).
    Outputs: none — see _run.
    Complexity: n/a.
    Failure cases: argparse exits with an error for an unknown --scenario.
    """
    parser = argparse.ArgumentParser(description="Pulse benchmark harness")
    parser.add_argument("--scenario", choices=sorted(SCENARIOS), default="steady")
    parser.add_argument("--url", default="http://api:8000")
    parser.add_argument("--throughput-seconds", type=float, default=15.0)
    parser.add_argument("--throughput-concurrency", type=int, default=50)
    parser.add_argument("--output", default="benchmark_report.md")
    parser.add_argument(
        "--live-seconds",
        type=float,
        default=None,
        help="override the scenario's live_duration_seconds (for quick smoke runs)",
    )
    args = parser.parse_args()

    scenario = SCENARIOS[args.scenario]
    if args.live_seconds is not None:
        scenario = Scenario(
            name=scenario.name,
            services=scenario.services,
            base_rate_per_second=scenario.base_rate_per_second,
            shape=scenario.shape,
            live_duration_seconds=args.live_seconds,
            warmup_minutes=scenario.warmup_minutes,
            anomalies=scenario.anomalies,
        )

    asyncio.run(
        _run(scenario, args.url, args.throughput_seconds, args.throughput_concurrency, args.output)
    )


if __name__ == "__main__":
    main()
