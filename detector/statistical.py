"""Rolling z-score and EWMA anomaly detectors — pure functions, no I/O.

Both compare a current value against a baseline derived from historical values and
flag when the deviation exceeds a threshold (in standard deviations). They differ in
how the baseline is computed: z-score weights every historical point equally; EWMA
weights recent points more heavily, so it responds faster to a level shift and is less
distorted by an old spike sitting in the window. Using both widens the range of
anomaly shapes caught (see docs/private/ARCHITECTURE_LEDGER.md).

history is expected in chronological order (oldest first) — EWMA's recency-weighting
depends on that ordering.
"""

import statistics

from detector.types import DetectionResult


def zscore_detect(
    current: float, history: list[float], threshold: float, min_history: int
) -> DetectionResult | None:
    """Flag current if it's more than `threshold` standard deviations from history's mean.

    Purpose: simple, equal-weighted baseline anomaly check.
    Inputs: current — the value being evaluated; history — prior values, chronological,
        NOT including current; threshold — flag when |z| >= threshold; min_history —
        minimum len(history) required to compute a meaningful baseline.
    Outputs: DetectionResult(score=z) if flagged, else None. score is signed: positive
        means current is above baseline, negative means below.
    Complexity: O(n) in len(history).
    Failure cases: never raises. Returns None if len(history) < min_history (no
        baseline yet) or if the historical sample has zero variance (a z-score is
        undefined against a perfectly constant baseline — treated as "can't evaluate"
        rather than guessing).
    """
    if len(history) < min_history:
        return None
    mean = statistics.mean(history)
    stdev = statistics.stdev(history)
    if stdev == 0:
        return None
    z = (current - mean) / stdev
    if abs(z) >= threshold:
        return DetectionResult(score=z)
    return None


def ewma_detect(
    current: float, history: list[float], alpha: float, threshold: float, min_history: int
) -> DetectionResult | None:
    """Flag current if it's far from an exponentially-weighted moving baseline.

    Purpose: recency-weighted baseline anomaly check — see module docstring for why
        this catches different anomaly shapes than zscore_detect.
    Inputs: current; history — prior values, chronological, NOT including current;
        alpha — EWMA smoothing factor (higher = more weight on recent history);
        threshold — flag when |z| >= threshold; min_history — minimum len(history).
    Outputs: DetectionResult(score=z) if flagged, else None, same sign convention as
        zscore_detect.
    Complexity: O(n) in len(history).
    Failure cases: never raises. Returns None if len(history) < min_history or the
        EWMA variance is zero (constant baseline).
    """
    if len(history) < min_history:
        return None
    ewma_mean = history[0]
    ewma_var = 0.0
    for value in history[1:]:
        diff = value - ewma_mean
        increment = alpha * diff
        ewma_mean += increment
        ewma_var = (1 - alpha) * (ewma_var + diff * increment)
    ewma_std = ewma_var**0.5
    if ewma_std == 0:
        return None
    z = (current - ewma_mean) / ewma_std
    if abs(z) >= threshold:
        return DetectionResult(score=z)
    return None
