"""Traffic generator — the I/O side of simulator/. Everything goes through the real
POST /events API (never seeds the database directly), so warmup history, the labeled
live scenario, and the throughput burst all exercise the exact same
validation/aggregation/detection pipeline production traffic does.
"""

import asyncio
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import httpx

from simulator.scenarios import Scenario, rate_at

# Dedicated, isolated (service, endpoint) for the throughput burst — never used by
# warmup or the labeled live phase — so benchmark/run.py can query Postgres for
# exactly the burst's consumer-processed count without it being mixed in with any
# other traffic landing in the same bucket.
BURST_SERVICE = "benchmark"
BURST_ENDPOINT = "/throughput"

# "Normal" traffic's status/latency distribution — deliberately wide (a realistic mix
# of success, redirect-ish 201s, and the occasional not-found) so injected anomalies
# have to be genuinely extreme to stand out, rather than the simulator handing the
# detector an easy, unrealistically narrow baseline.
_NORMAL_STATUS_WEIGHTS = (200, 200, 200, 201, 404)
_NORMAL_LATENCY_RANGE_MS = (15, 120)
_LATENCY_SPIKE_RANGE_MS = (3000, 6000)
_ERROR_BURST_STATUSES = (500, 502, 503)
# Concurrency cap for the warmup backfill's concurrent sends — independent of
# benchmark/run.py's --throughput-concurrency (which tunes the separate, unpaced
# throughput-burst phase); coincidentally the same default value, not linked.
_WARMUP_CONCURRENCY = 50
# Floor under rate_at()'s output so 1.0 / target_rate never approaches an unbounded
# sleep if a scenario's rate curve ever produced (or was misconfigured to produce) a
# near-zero or negative rate.
_MIN_TARGET_RATE_PER_SECOND = 0.1


@dataclass(frozen=True)
class GroundTruthLabel:
    """One (service, endpoint, minute_bucket) the simulator deliberately made anomalous."""

    minute_bucket: datetime
    service: str
    endpoint: str


@dataclass
class LiveResult:
    """Everything the labeled live phase produced: ground truth + timing."""

    labels: list[GroundTruthLabel] = field(default_factory=list)
    latencies_ms: list[float] = field(default_factory=list)
    events_sent: int = 0
    events_failed: int = 0
    duration_seconds: float = 0.0


@dataclass
class ThroughputResult:
    """Raw throughput measurement from an unpaced concurrent burst."""

    events_sent: int
    events_failed: int
    duration_seconds: float
    latencies_ms: list[float]
    send_offsets_seconds: list[float] = field(default_factory=list)
    """Each successful send's completion time, relative to the burst's start —
    lets the benchmark compute per-second throughput variance (mean/stddev/min/max),
    not just an aggregate events/sec, without needing separate instrumentation."""


def _bucket_for(ts: datetime) -> datetime:
    return ts.replace(second=0, microsecond=0)


def _event_payload(
    service: str, endpoint: str, status_code: int, latency_ms: float, ts: datetime
) -> dict:
    """Build the wire-format dict every generated event shares, regardless of scenario."""
    return {
        "service": service,
        "endpoint": endpoint,
        "status_code": status_code,
        "latency_ms": round(latency_ms, 2),
        "ts": ts.isoformat(),
    }


def _normal_event(service: str, endpoint: str, ts: datetime) -> dict:
    return _event_payload(
        service,
        endpoint,
        random.choice(_NORMAL_STATUS_WEIGHTS),
        random.uniform(*_NORMAL_LATENCY_RANGE_MS),
        ts,
    )


def _anomalous_event(service: str, endpoint: str, ts: datetime, kind: str) -> dict:
    if kind == "latency_spike":
        return _event_payload(service, endpoint, 200, random.uniform(*_LATENCY_SPIKE_RANGE_MS), ts)
    if kind == "error_burst":
        return _event_payload(
            service,
            endpoint,
            random.choice(_ERROR_BURST_STATUSES),
            random.uniform(*_NORMAL_LATENCY_RANGE_MS),
            ts,
        )
    raise ValueError(f"unknown anomaly kind: {kind}")


async def _post_event(client: httpx.AsyncClient, url: str, payload: dict) -> float | None:
    """POST one event, returning client-observed latency in ms, or None on failure.

    Purpose: the one place every phase (warmup, live, throughput burst) sends an
        event and times it — keeps the latency-measurement methodology identical
        across all three.
    Inputs: client; url; payload — an event body.
    Outputs: elapsed milliseconds if the API returned 202, else None.
    Complexity: O(1) plus network round trip.
    Failure cases: never raises — httpx.HTTPError is caught and treated as a failure
        (returns None), since a benchmark run must survive individual request errors.
    """
    start = time.perf_counter()
    try:
        response = await client.post(url, json=payload)
    except httpx.HTTPError:
        return None
    elapsed_ms = (time.perf_counter() - start) * 1000
    return elapsed_ms if response.status_code == 202 else None


async def run_warmup(client: httpx.AsyncClient, url: str, scenario: Scenario) -> int:
    """Send warmup_minutes of backdated history for every (service, endpoint).

    Purpose: gives the detector a real baseline (satisfying
        isolation_forest_min_history) before the live phase's labeled anomalies need
        to be caught — through the real API with past timestamps, not a direct DB
        seed, so it's aggregated by the real consumer exactly like production traffic.
    Inputs: client; url — the /events endpoint; scenario — provides warmup_minutes
        and the services/endpoints to backfill.
    Outputs: count of successfully accepted warmup events.
    Complexity: O(warmup_minutes * endpoints * events_per_bucket), sent concurrently
        (bounded), not paced to real time — this is a fast bulk backfill, not a
        simulation of real-time traffic.
    Failure cases: none raised — individual failures are simply not counted.
    """
    now = datetime.now(timezone.utc)
    events_per_bucket = 12
    semaphore = asyncio.Semaphore(_WARMUP_CONCURRENCY)

    async def _send(payload: dict) -> float | None:
        async with semaphore:
            return await _post_event(client, url, payload)

    coros = []
    for minute_offset in range(scenario.warmup_minutes, 0, -1):
        bucket_ts = now - timedelta(minutes=minute_offset)
        for service, endpoints in scenario.services.items():
            for endpoint in endpoints:
                for _ in range(events_per_bucket):
                    jitter = timedelta(seconds=random.uniform(0, 59))
                    payload = _normal_event(service, endpoint, bucket_ts + jitter)
                    coros.append(_send(payload))

    results = await asyncio.gather(*coros)
    return sum(1 for r in results if r is not None)


async def run_live_phase(client: httpx.AsyncClient, url: str, scenario: Scenario) -> LiveResult:
    """Run the labeled, paced live phase: real-time traffic at the scenario's rate
    curve, injecting anomalies on schedule and recording ground truth.

    Purpose: produces both the ingest latency sample and the ground-truth labels the
        benchmark later compares against what the detector actually flagged.
    Inputs: client; url; scenario — provides rate_at(), duration, and the anomaly
        injection schedule.
    Outputs: a LiveResult with every injected (bucket, service, endpoint) labeled
        exactly once (deduplicated — an injection spanning multiple seconds within
        one minute bucket produces one label, not one per event) and every request's
        latency.
    Complexity: runs for scenario.live_duration_seconds of wall-clock time.
    Failure cases: none raised — failed requests are counted, not fatal.
    """
    result = LiveResult()
    seen_label_keys: set[tuple[datetime, str, str]] = set()
    start = time.perf_counter()

    while True:
        elapsed = time.perf_counter() - start
        if elapsed >= scenario.live_duration_seconds:
            break

        target_rate = max(rate_at(scenario, elapsed), _MIN_TARGET_RATE_PER_SECOND)
        now = datetime.now(timezone.utc)
        injection = next(
            (
                a
                for a in scenario.anomalies
                if a.offset_seconds <= elapsed < a.offset_seconds + a.duration_seconds
            ),
            None,
        )

        if injection is not None:
            payload = _anomalous_event(injection.service, injection.endpoint, now, injection.kind)
            key = (_bucket_for(now), injection.service, injection.endpoint)
            if key not in seen_label_keys:
                seen_label_keys.add(key)
                result.labels.append(GroundTruthLabel(*key))
        else:
            service = random.choice(list(scenario.services))
            endpoint = random.choice(scenario.services[service])
            payload = _normal_event(service, endpoint, now)

        latency = await _post_event(client, url, payload)
        if latency is None:
            result.events_failed += 1
        else:
            result.events_sent += 1
            result.latencies_ms.append(latency)

        await asyncio.sleep(1.0 / target_rate)

    result.duration_seconds = time.perf_counter() - start
    return result


async def run_throughput_burst(
    client: httpx.AsyncClient, url: str, duration_seconds: float, concurrency: int
) -> ThroughputResult:
    """Fire events as fast as possible with N concurrent workers, unpaced.

    Purpose: measures raw sustained ingest throughput and latency under load,
        decoupled from the labeled scenario's controlled pacing (which is tuned for
        realistic-looking traffic, not for stressing the system).
    Inputs: client; url; duration_seconds — how long to sustain the burst;
        concurrency — number of concurrent worker loops.
    Outputs: a ThroughputResult with every request's latency, completion offset (for
        per-second throughput variance), and success/failure counts.
    Complexity: O(concurrency) parallel request loops for duration_seconds.
    Failure cases: none raised — failed requests are counted, not fatal.
    """
    latencies: list[float] = []
    send_offsets: list[float] = []
    sent = 0
    failed = 0
    lock = asyncio.Lock()
    start = time.perf_counter()
    stop_at = start + duration_seconds

    async def worker() -> None:
        nonlocal sent, failed
        while time.perf_counter() < stop_at:
            payload = _normal_event(BURST_SERVICE, BURST_ENDPOINT, datetime.now(timezone.utc))
            latency = await _post_event(client, url, payload)
            async with lock:
                if latency is None:
                    failed += 1
                else:
                    sent += 1
                    latencies.append(latency)
                    send_offsets.append(time.perf_counter() - start)

    await asyncio.gather(*(worker() for _ in range(concurrency)))
    elapsed = time.perf_counter() - start
    return ThroughputResult(
        events_sent=sent,
        events_failed=failed,
        duration_seconds=elapsed,
        latencies_ms=latencies,
        send_offsets_seconds=send_offsets,
    )
