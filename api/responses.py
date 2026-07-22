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

from pydantic import BaseModel


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
    service: str
    request_count: int
    error_count: int
    error_rate: float
    endpoints: list[EndpointSeries]
