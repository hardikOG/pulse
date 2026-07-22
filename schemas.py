"""Trust boundary for untrusted wire JSON. Nothing past this module should see raw dicts."""

from datetime import datetime, timedelta, timezone

from pydantic import BaseModel, ConfigDict, Field, field_validator

# How far into the future a client-reported timestamp may be before it's rejected.
# Small enough to catch garbage/malicious payloads, generous enough to absorb normal
# clock skew between a client host and Pulse.
_MAX_FUTURE_SKEW = timedelta(minutes=1)


class EventIn(BaseModel):
    """A single API request event as received from a client service.

    Purpose: typed, validated representation of the wire JSON POSTed to /events —
        the boundary between untrusted client input and the rest of Pulse.
    Inputs: n/a (Pydantic model — populated by FastAPI from the request body).
    Outputs: n/a (Pydantic model).
    Complexity: n/a.
    Failure cases: pydantic raises a ValidationError (surfaced by FastAPI as HTTP 422)
        for any missing/mistyped field, an out-of-range status_code, negative latency_ms,
        a blank service/endpoint, or a ts more than _MAX_FUTURE_SKEW in the future.
    """

    model_config = ConfigDict(strict=True)

    service: str = Field(min_length=1)
    endpoint: str = Field(min_length=1)
    status_code: int = Field(ge=100, le=599)
    latency_ms: float = Field(ge=0)
    # strict=False override: JSON has no native datetime type, so even in JSON mode
    # pydantic's strict mode requires an actual datetime object — an ISO string (the
    # only wire representation clients can send) would otherwise be rejected outright.
    ts: datetime = Field(strict=False)

    @field_validator("ts")
    @classmethod
    def ts_not_too_far_in_future(cls, value: datetime) -> datetime:
        """Reject timestamps further in the future than plausible clock skew allows.

        Purpose: guards per-minute bucketing downstream from garbage/malicious
            timestamps that would otherwise create buckets for "future" minutes.
        Inputs: value — the parsed ts datetime.
        Outputs: value, unchanged, if valid.
        Complexity: O(1).
        Failure cases: raises ValueError (surfaced as 422) if value is more than
            _MAX_FUTURE_SKEW ahead of now.
        """
        now = datetime.now(timezone.utc)
        compare_value = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        if compare_value - now > _MAX_FUTURE_SKEW:
            raise ValueError("ts is too far in the future")
        return value
