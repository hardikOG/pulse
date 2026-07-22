# Pulse Benchmark Report

Generated: 2026-07-22T21:49:03.832290+00:00
Scenario: steady

All numbers below are measured from this run — see CLAUDE.md's PERFORMANCE section:
targets are never hardcoded or asserted ahead of measurement.

## Timeline

-    0.0s — warmup started
-    4.4s — warmup finished
-   19.5s — throughput burst finished
-   19.5s — live phase started
-  259.5s — live phase finished
-  274.6s — grace period elapsed

## Environment

- OS: Linux 6.18.33.2-microsoft-standard-WSL2
- Python: 3.11.15
- CPU cores: 12
- Total memory: 7793 MB
- Hardware: x86_64

## Pipeline throughput

- **API ingest (= Redis XADD completion): 540.3 events/sec**
- **Consumer processed (durably aggregated into Postgres): 540.3 events/sec**
- API ingest throughput and Redis XADD throughput are the same measurement here, not two separate numbers: POST /events awaits XADD synchronously before returning, so accepted-per-second already IS Redis-write-per-second. Consumer processed throughput is measured independently, from Postgres (total request_count aggregated within the run window / wall-clock duration) — the gap between it and API ingest throughput (if any) is the honest signal for consumer backpressure, not an assumption that they match.

## Throughput (unpaced concurrent burst)

- Duration: 15.1s
- Events sent: 8140 (0 failed)
- **Events/sec (average): 540.3**
- Per-second throughput — mean: 542.7, stddev: 31.9, min: 460, max: 591
- Ingest latency — avg: 92.28ms, p50: 62.24ms, **p95: 267.58ms**, p99: 428.89ms (n=8140)

## Labeled scenario run (steady)

- Warmup events (backdated history): 1920
- Duration: 240.0s
- Events sent: 4542 (0 failed)
- Ingest latency — avg: 2.54ms, p50: 2.48ms, p95: 3.04ms, p99: 3.32ms (n=4542)

## Detector precision/recall (against labeled ground truth)

- True positives: 3
- False positives: 21
- False negatives: 0
- **Precision: 12.5%**
- **Recall: 100.0%**

## Operational health

- Redis used memory: 2.3 MB
- Redis ops/sec (instantaneous, sampled at report time): 0
- Postgres database size: 8.8 MB
- Peak consumer-group pending entries observed during the run: 7
- Peak consumer-group lag observed during the run: 6
