"""IsolationForest multivariate anomaly detector — pure function, no I/O.

Fits a fresh IsolationForest on the historical feature vectors every call rather than
persisting/incrementally updating a model: simpler, always trained on the honest
recent window, and cheap at this project's scale (fit once per bucket-close per
endpoint, not per event) — see docs/private/ARCHITECTURE_LEDGER.md.
"""

from sklearn.ensemble import IsolationForest

from detector.types import BucketSample, DetectionResult

# Fixed so results are reproducible in tests — IsolationForest's tree construction is
# randomized.
_RANDOM_STATE = 42


def _feature_vector(sample: BucketSample) -> list[float]:
    """Map a BucketSample onto the fixed feature vector IsolationForest scores.

    Purpose: single place defining which fields feed the multivariate detector, so
        history and current always use the same feature order.
    Inputs: sample — a BucketSample.
    Outputs: [request_count, error_rate, p50, p95, p99].
    Complexity: O(1).
    Failure cases: none.
    """
    return [sample.request_count, sample.error_rate, sample.p50, sample.p95, sample.p99]


def isolation_forest_detect(
    current: BucketSample,
    history: list[BucketSample],
    contamination: float,
    min_history: int,
) -> DetectionResult | None:
    """Flag current if IsolationForest scores it an outlier against historical vectors.

    Purpose: multivariate anomaly check — catches combinations of values that are each
        individually unremarkable but jointly unusual (e.g. normal latency with an
        abnormal request_count/error_rate combination), which the single-signal
        statistical detectors can't see.
    Inputs: current — the bucket being evaluated; history — prior buckets (order does
        not matter for this detector, unlike the statistical ones); contamination —
        expected outlier fraction, passed directly to IsolationForest; min_history —
        minimum len(history) required to fit a meaningful model.
    Outputs: DetectionResult(score=...) if flagged, else None. score is
        score_samples()'s output: LOWER (more negative) means more anomalous — the
        OPPOSITE sign convention from zscore_detect/ewma_detect's |z|, since that's
        scikit-learn's native convention. Callers must label this clearly.
    Complexity: O(n log n) to fit the forest, n = len(history).
    Failure cases: never raises. Returns None if len(history) < min_history.
    """
    if len(history) < min_history:
        return None
    x_history = [_feature_vector(sample) for sample in history]
    x_current = [_feature_vector(current)]
    model = IsolationForest(contamination=contamination, random_state=_RANDOM_STATE)
    model.fit(x_history)
    prediction = model.predict(x_current)[0]
    if prediction == -1:
        score = float(model.score_samples(x_current)[0])
        return DetectionResult(score=score)
    return None
