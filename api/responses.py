"""Response shapes for the read API (Phase 3). Deliberately separate from schemas.py,
which is the *input* wire-JSON trust boundary — these are output-only presentation
models with no validation role.

No percentile field appears on any summary/list model (ServiceSummary,
EndpointSummary) — only genuinely additive fields (request_count, error_count,
error_rate). Percentiles are not associative: averaging or summing per-bucket p95
values does not produce a correct combined p95 (same reasoning as Phase 2's
progressive-upsert design; see docs/private/ARCHITECTURE_LEDGER.md). Exact,
never-merged percentiles only appear in MetricPoint, one per-minute-per-endpoint row.
"""

from datetime import datetime

from pydantic import BaseModel, ConfigDict


class PaginationMeta(BaseModel):
    total: int
    limit: int
    offset: int


class ServiceSummary(BaseModel):
    service: str
    request_count: int
    error_count: int
    error_rate: float
    last_seen: datetime | None


class ServiceListResponse(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "items": [
                    {
                        "service": "checkout",
                        "request_count": 5230,
                        "error_count": 12,
                        "error_rate": 0.0023,
                        "last_seen": "2026-01-01T12:34:00Z",
                    }
                ],
                "pagination": {"total": 1, "limit": 20, "offset": 0},
            }
        }
    )

    items: list[ServiceSummary]
    pagination: PaginationMeta


class EndpointSummary(BaseModel):
    service: str
    endpoint: str
    request_count: int
    error_count: int
    error_rate: float
    last_seen: datetime | None


class EndpointListResponse(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "items": [
                    {
                        "service": "checkout",
                        "endpoint": "/api/pay",
                        "request_count": 1840,
                        "error_count": 4,
                        "error_rate": 0.0022,
                        "last_seen": "2026-01-01T12:34:00Z",
                    }
                ],
                "pagination": {"total": 1, "limit": 20, "offset": 0},
            }
        }
    )

    items: list[EndpointSummary]
    pagination: PaginationMeta


class MetricPoint(BaseModel):
    minute_bucket: datetime
    request_count: int
    error_count: int
    p50: float
    p95: float
    p99: float


class EndpointSeries(BaseModel):
    endpoint: str
    request_count: int
    error_count: int
    error_rate: float
    series: list[MetricPoint]


class ServiceDetailResponse(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "service": "checkout",
                "request_count": 1840,
                "error_count": 4,
                "error_rate": 0.0022,
                "endpoints": [
                    {
                        "endpoint": "/api/pay",
                        "request_count": 1840,
                        "error_count": 4,
                        "error_rate": 0.0022,
                        "series": [
                            {
                                "minute_bucket": "2026-01-01T12:34:00Z",
                                "request_count": 62,
                                "error_count": 1,
                                "p50": 41.2,
                                "p95": 118.7,
                                "p99": 203.4,
                            }
                        ],
                    }
                ],
            }
        }
    )

    service: str
    request_count: int
    error_count: int
    error_rate: float
    endpoints: list[EndpointSeries]
