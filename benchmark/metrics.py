"""Pure precision/recall and throughput-variance computation — no I/O. Compares
ground-truth labels (from simulator/generator.py) against what the detector actually
persisted to the anomalies table, both expressed as sets of
(minute_bucket, service, endpoint).
"""

import math
from dataclasses import dataclass
from datetime import datetime

BucketKey = tuple[datetime, str, str]


@dataclass(frozen=True)
class DetectionMetrics:
    """Precision/recall of the detector against labeled ground truth for one run."""

    true_positives: int
    false_positives: int
    false_negatives: int
    precision: float
    recall: float


def compute_precision_recall(
    ground_truth: set[BucketKey], detected: set[BucketKey]
) -> DetectionMetrics:
    """Compute precision/recall of detected buckets against labeled ground truth.

    Purpose: the honest measurement CLAUDE.md's benchmark report requires — real
        precision/recall against known injected anomalies, not an asserted number.
    Inputs: ground_truth — (minute_bucket, service, endpoint) keys the simulator
        deliberately made anomalous; detected — the same key shape for every bucket
        with at least one persisted Anomaly row (callers should collapse multiple
        detectors firing on the same bucket into one entry before calling this —
        precision/recall here is about bucket-level detection, not per-detector
        agreement).
    Outputs: DetectionMetrics. precision = TP/(TP+FP), recall = TP/(TP+FN), both 0.0
        (not NaN) when their denominator is zero.
    Complexity: O(n) set operations, n = len(ground_truth) + len(detected).
    Failure cases: none.
    """
    true_positives = len(ground_truth & detected)
    false_positives = len(detected - ground_truth)
    false_negatives = len(ground_truth - detected)
    precision = (
        true_positives / (true_positives + false_positives)
        if (true_positives + false_positives)
        else 0.0
    )
    recall = (
        true_positives / (true_positives + false_negatives)
        if (true_positives + false_negatives)
        else 0.0
    )
    return DetectionMetrics(
        true_positives=true_positives,
        false_positives=false_positives,
        false_negatives=false_negatives,
        precision=precision,
        recall=recall,
    )


@dataclass(frozen=True)
class ThroughputVariance:
    """How stable throughput was across the run, not just its aggregate rate.

    Purpose: a single events/sec average can hide a run that started fast and
        collapsed, or one with periodic stalls — this captures the per-second shape.
    """

    per_second_counts: tuple[int, ...]
    mean: float
    stddev: float
    minimum: int
    maximum: int


def compute_throughput_variance(
    send_offsets_seconds: list[float], duration_seconds: float
) -> ThroughputVariance:
    """Bucket send completion times into 1-second windows and summarize their spread.

    Purpose: distinguishes "sent N events/sec on average" from "sent a stable N
        events/sec every second" — the difference matters for an honest throughput
        claim (see CLAUDE.md's PERFORMANCE section).
    Inputs: send_offsets_seconds — each successful request's completion time,
        relative to the burst's start (from ThroughputResult.send_offsets_seconds);
        duration_seconds — total burst duration, used to size empty trailing/leading
        seconds correctly rather than only counting seconds that had at least one
        event.
    Outputs: ThroughputVariance with the full per-second series plus mean/stddev/
        min/max across it. All-zero counts (not an empty tuple) if there were no
        successful sends.
    Complexity: O(n + d), n = len(send_offsets_seconds), d = whole seconds in
        duration_seconds.
    Failure cases: none.
    """
    num_seconds = max(int(duration_seconds), 1)
    counts = [0] * num_seconds
    for offset in send_offsets_seconds:
        bucket = min(int(offset), num_seconds - 1)
        if bucket >= 0:
            counts[bucket] += 1

    # counts always has at least one element (num_seconds >= 1), so it's never empty
    # here — mean/variance over an all-zero list is still well-defined (0.0).
    mean = sum(counts) / len(counts)
    variance = sum((c - mean) ** 2 for c in counts) / len(counts)
    return ThroughputVariance(
        per_second_counts=tuple(counts),
        mean=mean,
        stddev=math.sqrt(variance),
        minimum=min(counts),
        maximum=max(counts),
    )
