"""Trust boundary for untrusted wire JSON. Nothing past this module should see raw dicts.

Full validation rules (status_code ranges, timestamp bounds, service/endpoint format) land
in Phase 1 alongside POST /events. This is a structural placeholder so later phases have a
stable import path.
"""

from datetime import datetime

from pydantic import BaseModel, ConfigDict


class EventIn(BaseModel):
    """A single API request event as received from a client service.

    Purpose: typed, validated representation of the wire JSON POSTed to /events —
        the boundary between untrusted client input and the rest of Pulse.
    Inputs: n/a (Pydantic model — populated by FastAPI from the request body).
    Outputs: n/a (Pydantic model).
    Complexity: n/a.
    Failure cases: pydantic raises a ValidationError (surfaced by FastAPI as HTTP 422)
        for any missing or mistyped field.
    """

    model_config = ConfigDict(strict=True)

    service: str
    endpoint: str
    status_code: int
    latency_ms: float
    ts: datetime
