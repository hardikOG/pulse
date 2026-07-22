"""Named traffic scenarios for the benchmark harness — pure data + pure functions,
no I/O. simulator/generator.py is what actually sends anything.
"""

from dataclasses import dataclass
from typing import Literal

AnomalyKind = Literal["latency_spike", "error_burst"]
RateShape = Literal["constant", "peak_mid"]


@dataclass(frozen=True)
class AnomalyInjection:
    """One deliberately-injected anomaly — both the instruction to generate it and
    the ground-truth label the benchmark later checks the detector against.

    Purpose: describes when (relative to the live phase's start) and where to inject
        an anomalous burst of events.
    Inputs: n/a (data container).
    Outputs: n/a (data container).
    Complexity: n/a.
    Failure cases: n/a.
    """

    offset_seconds: float
    duration_seconds: float
    service: str
    endpoint: str
    kind: AnomalyKind


@dataclass(frozen=True)
class Scenario:
    """A named traffic pattern: which services/endpoints, how fast, how long, and
    which anomalies to inject during the live phase.

    Purpose: data container the traffic generator (simulator/generator.py) drives
        from. warmup_minutes worth of backdated history is sent first (through the
        real API, using past timestamps) so the detector has a real baseline before
        the live phase's labeled anomalies need to be caught.
    Inputs: n/a (data container).
    Outputs: n/a (data container).
    Complexity: n/a.
    Failure cases: n/a.
    """

    name: str
    services: dict[str, tuple[str, ...]]
    base_rate_per_second: float
    shape: RateShape
    live_duration_seconds: float
    warmup_minutes: int
    anomalies: tuple[AnomalyInjection, ...]


def rate_at(scenario: Scenario, elapsed_seconds: float) -> float:
    """Compute the target events/sec at a point in the live phase's elapsed time.

    Purpose: the only place a scenario's rate "shape" actually affects timing — pure
        function of elapsed time, independent of real wall-clock sending, so it's
        directly unit-testable.
    Inputs: scenario; elapsed_seconds — seconds since the live phase started.
    Outputs: target events/sec at that point in the live phase.
    Complexity: O(1).
    Failure cases: raises ValueError for an unrecognized shape (defensive — the
        Literal type should make this unreachable in practice).
    """
    if scenario.shape == "constant":
        return scenario.base_rate_per_second
    if scenario.shape == "peak_mid":
        third = scenario.live_duration_seconds / 3
        if third <= elapsed_seconds < 2 * third:
            return scenario.base_rate_per_second * 3
        return scenario.base_rate_per_second
    raise ValueError(f"unknown rate shape: {scenario.shape}")


_SERVICES: dict[str, tuple[str, ...]] = {
    "checkout": ("/pay", "/cart"),
    "auth": ("/login", "/refresh"),
    "search": ("/query",),
}

# 32 minutes of backdated warmup history comfortably clears
# isolation_forest_min_history (30) — see core/config.py.
_WARMUP_MINUTES = 32

STEADY = Scenario(
    name="steady",
    services=_SERVICES,
    base_rate_per_second=20.0,
    shape="constant",
    live_duration_seconds=240.0,
    warmup_minutes=_WARMUP_MINUTES,
    anomalies=(
        AnomalyInjection(60.0, 30.0, "checkout", "/pay", "latency_spike"),
        AnomalyInjection(150.0, 30.0, "auth", "/login", "error_burst"),
    ),
)

PEAK = Scenario(
    name="peak",
    services=_SERVICES,
    base_rate_per_second=15.0,
    shape="peak_mid",
    live_duration_seconds=240.0,
    warmup_minutes=_WARMUP_MINUTES,
    anomalies=(
        AnomalyInjection(100.0, 30.0, "checkout", "/cart", "latency_spike"),
        AnomalyInjection(190.0, 30.0, "search", "/query", "error_burst"),
    ),
)

WEEKEND = Scenario(
    name="weekend",
    services=_SERVICES,
    base_rate_per_second=5.0,
    shape="constant",
    live_duration_seconds=240.0,
    warmup_minutes=_WARMUP_MINUTES,
    anomalies=(AnomalyInjection(90.0, 30.0, "auth", "/refresh", "latency_spike"),),
)

SCENARIOS: dict[str, Scenario] = {s.name: s for s in (STEADY, PEAK, WEEKEND)}
