"""Unit tests for simulator/scenarios.py — pure rate-curve logic."""

import pytest

from simulator.scenarios import PEAK, SCENARIOS, STEADY, WEEKEND, Scenario, rate_at


def test_all_named_scenarios_registered() -> None:
    assert set(SCENARIOS) == {"steady", "peak", "weekend"}
    assert SCENARIOS["steady"] is STEADY
    assert SCENARIOS["peak"] is PEAK
    assert SCENARIOS["weekend"] is WEEKEND


def test_constant_rate_is_flat_over_time() -> None:
    assert rate_at(STEADY, 0.0) == STEADY.base_rate_per_second
    assert rate_at(STEADY, 100.0) == STEADY.base_rate_per_second
    assert rate_at(STEADY, STEADY.live_duration_seconds - 1) == STEADY.base_rate_per_second


def test_peak_mid_triples_rate_in_middle_third_only() -> None:
    third = PEAK.live_duration_seconds / 3
    assert rate_at(PEAK, 0.0) == PEAK.base_rate_per_second
    assert rate_at(PEAK, third - 1) == PEAK.base_rate_per_second
    assert rate_at(PEAK, third + 1) == PEAK.base_rate_per_second * 3
    assert rate_at(PEAK, 2 * third - 1) == PEAK.base_rate_per_second * 3
    assert rate_at(PEAK, 2 * third + 1) == PEAK.base_rate_per_second


def test_unknown_shape_raises() -> None:
    bogus = Scenario(
        name="bogus",
        services={"a": ("/x",)},
        base_rate_per_second=1.0,
        shape="not-a-real-shape",  # type: ignore[arg-type]
        live_duration_seconds=10.0,
        warmup_minutes=1,
        anomalies=(),
    )
    with pytest.raises(ValueError):
        rate_at(bogus, 0.0)


def test_every_scenario_has_at_least_one_labeled_anomaly() -> None:
    for scenario in SCENARIOS.values():
        assert len(scenario.anomalies) >= 1


def test_anomaly_injections_fit_within_live_duration() -> None:
    for scenario in SCENARIOS.values():
        for injection in scenario.anomalies:
            assert injection.offset_seconds + injection.duration_seconds <= scenario.live_duration_seconds
            assert injection.service in scenario.services
            assert injection.endpoint in scenario.services[injection.service]
