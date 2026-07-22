"""Unit tests for consumer/main.py's _parse_event — poison-message handling."""

from datetime import datetime, timezone

from consumer.main import _parse_event
from core.logging import get_logger

logger = get_logger("test.consumer_parsing")

VALID_FIELDS = {
    "service": "checkout",
    "endpoint": "/api/pay",
    "status_code": "200",
    "latency_ms": "42.5",
    "ts": datetime.now(timezone.utc).isoformat(),
}


def test_parse_event_valid_fields() -> None:
    event = _parse_event(VALID_FIELDS, logger)
    assert event is not None
    assert event.service == "checkout"
    assert event.endpoint == "/api/pay"
    assert event.status_code == 200
    assert event.latency_ms == 42.5


def test_parse_event_missing_field_returns_none() -> None:
    fields = {k: v for k, v in VALID_FIELDS.items() if k != "service"}
    assert _parse_event(fields, logger) is None


def test_parse_event_non_numeric_status_code_returns_none() -> None:
    fields = {**VALID_FIELDS, "status_code": "not-a-number"}
    assert _parse_event(fields, logger) is None


def test_parse_event_malformed_timestamp_returns_none() -> None:
    fields = {**VALID_FIELDS, "ts": "not-a-timestamp"}
    assert _parse_event(fields, logger) is None


def test_parse_event_out_of_range_status_code_returns_none() -> None:
    fields = {**VALID_FIELDS, "status_code": "999"}
    assert _parse_event(fields, logger) is None
