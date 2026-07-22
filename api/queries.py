"""Read-API query functions — isolated from route handlers for direct testability.

Only SUM/COUNT/MAX aggregates and GROUP BY/HAVING are used here (no Postgres-specific
constructs), so these run correctly against SQLite in tests as well as Postgres in
production.
"""

from datetime import datetime, timedelta, timezone
from typing import Any, Literal

from sqlalchemy import Select, func, select
from sqlalchemy.orm import Session

from db.models import Metric

StatusClass = Literal["all", "healthy", "error"]

_DEFAULT_WINDOW = timedelta(minutes=60)


def resolve_time_bounds(
    start: datetime | None, end: datetime | None
) -> tuple[datetime, datetime]:
    """Fill in a default recent time window when start/end are not both provided.

    Purpose: single place defining "recent" for every read-API route, so filters
        behave consistently.
    Inputs: start, end — optional caller-provided bounds (tz-aware, or None).
    Outputs: (start, end), both tz-aware: end defaults to now, start defaults to
        end - 60 minutes.
    Complexity: O(1).
    Failure cases: none.
    """
    resolved_end = end if end is not None else datetime.now(timezone.utc)
    resolved_start = start if start is not None else resolved_end - _DEFAULT_WINDOW
    return resolved_start, resolved_end


def _apply_status_class(stmt: Select[Any], status_class: StatusClass) -> Select[Any]:
    """Apply the healthy/error HAVING filter to an already-grouped aggregate query.

    Purpose: shared filter logic between list_services and list_endpoints.
    Inputs: stmt — a SQLAlchemy Select already GROUP BY'd, with error_count summed
        under the label "error_count"; status_class — "all" (no filter), "healthy"
        (zero errors in range), or "error" (at least one error in range).
    Outputs: the Select, with a HAVING clause added if status_class != "all".
    Complexity: O(1).
    Failure cases: none.
    """
    if status_class == "healthy":
        return stmt.having(func.sum(Metric.error_count) == 0)
    if status_class == "error":
        return stmt.having(func.sum(Metric.error_count) > 0)
    return stmt


def list_services(
    session: Session,
    start: datetime | None,
    end: datetime | None,
    status_class: StatusClass,
    limit: int,
    offset: int,
) -> tuple[list[Any], int, datetime, datetime]:
    """Aggregate metrics by service over a time range, paginated and filtered.

    Purpose: backs GET /services.
    Inputs: session; start/end — optional range bounds; status_class — healthy/error/
        all filter; limit/offset — pagination.
    Outputs: (rows, total, resolved_start, resolved_end) — rows are (service,
        request_count, error_count, last_seen) tuples for one page; total is the
        count of distinct services matching the filter (for pagination metadata),
        independent of limit/offset.
    Complexity: two aggregate queries, each O(n) in matching metrics rows.
    Failure cases: none — an empty result set is valid (returns an empty list).
    """
    resolved_start, resolved_end = resolve_time_bounds(start, end)

    base = (
        select(
            Metric.service,
            func.sum(Metric.request_count).label("request_count"),
            func.sum(Metric.error_count).label("error_count"),
            func.max(Metric.minute_bucket).label("last_seen"),
        )
        .where(Metric.minute_bucket >= resolved_start, Metric.minute_bucket < resolved_end)
        .group_by(Metric.service)
    )
    base = _apply_status_class(base, status_class)

    total = session.execute(select(func.count()).select_from(base.subquery())).scalar_one()

    page_stmt = base.order_by(Metric.service).limit(limit).offset(offset)
    rows = session.execute(page_stmt).all()
    return rows, total, resolved_start, resolved_end


def list_endpoints(
    session: Session,
    service: str | None,
    start: datetime | None,
    end: datetime | None,
    status_class: StatusClass,
    limit: int,
    offset: int,
) -> tuple[list[Any], int, datetime, datetime]:
    """Aggregate metrics by (service, endpoint) over a time range, paginated/filtered.

    Purpose: backs GET /endpoints.
    Inputs: session; service — optional exact-match filter; start/end; status_class;
        limit/offset.
    Outputs: (rows, total, resolved_start, resolved_end) — rows are (service,
        endpoint, request_count, error_count, last_seen) tuples for one page.
    Complexity: two aggregate queries, each O(n) in matching metrics rows.
    Failure cases: none.
    """
    resolved_start, resolved_end = resolve_time_bounds(start, end)

    base = (
        select(
            Metric.service,
            Metric.endpoint,
            func.sum(Metric.request_count).label("request_count"),
            func.sum(Metric.error_count).label("error_count"),
            func.max(Metric.minute_bucket).label("last_seen"),
        )
        .where(Metric.minute_bucket >= resolved_start, Metric.minute_bucket < resolved_end)
        .group_by(Metric.service, Metric.endpoint)
    )
    if service is not None:
        base = base.where(Metric.service == service)
    base = _apply_status_class(base, status_class)

    total = session.execute(select(func.count()).select_from(base.subquery())).scalar_one()

    page_stmt = base.order_by(Metric.service, Metric.endpoint).limit(limit).offset(offset)
    rows = session.execute(page_stmt).all()
    return rows, total, resolved_start, resolved_end


def get_service_rows(
    session: Session, service: str, start: datetime | None, end: datetime | None
) -> tuple[list[Metric], datetime, datetime]:
    """Fetch every raw metrics row for one service over a time range.

    Purpose: backs GET /services/{service}. Returns raw rows rather than aggregating
        across endpoints, since merging percentiles across endpoints would be exactly
        the invalid operation this project avoids elsewhere — grouping into a
        per-endpoint series happens in the caller, using each row's exact values
        unmodified.
    Inputs: session; service — exact service name; start/end — optional range bounds.
    Outputs: (rows, resolved_start, resolved_end) — rows ordered by endpoint then
        minute_bucket; an empty list means the service has no data in range (the
        route layer decides whether that's a 404).
    Complexity: O(n) in matching metrics rows.
    Failure cases: none — an empty list is a valid, meaningful result.
    """
    resolved_start, resolved_end = resolve_time_bounds(start, end)
    stmt = (
        select(Metric)
        .where(
            Metric.service == service,
            Metric.minute_bucket >= resolved_start,
            Metric.minute_bucket < resolved_end,
        )
        .order_by(Metric.endpoint, Metric.minute_bucket)
    )
    rows = list(session.execute(stmt).scalars().all())
    return rows, resolved_start, resolved_end
