"""Hermetic tests for the read API (Phase 3) — SQLite in-memory, no Docker needed.

The read queries (SELECT/GROUP BY/HAVING) are portable SQL, unlike the consumer's
Postgres-specific ON CONFLICT upsert, so these can run against SQLite directly rather
than relying solely on the live Docker gate.
"""

from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from api.main import app, get_db
from db.models import Base, Metric

NOW = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
WINDOW_START = (NOW - timedelta(minutes=10)).isoformat()
WINDOW_END = (NOW + timedelta(minutes=1)).isoformat()


def _minute(offset_minutes: int) -> datetime:
    return NOW - timedelta(minutes=offset_minutes)


@pytest.fixture
def db_session():
    # StaticPool + check_same_thread=False: TestClient runs the ASGI app in a
    # different thread than this fixture, and an in-memory SQLite database only
    # exists within a single connection — StaticPool keeps that one connection
    # alive and shared instead of the pool opening a fresh (empty) one per thread.
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    session_local = sessionmaker(bind=engine)
    session = session_local()
    # Explicit ids: SQLite's ROWID-alias autoincrement only kicks in for a column
    # typed exactly INTEGER, not the production schema's BIGINT — a SQLite test-only
    # quirk, not a bug in db/models.py (proven working against real Postgres in
    # Phase 2). Assigning ids directly sidesteps it without touching the schema.
    rows = [
        Metric(
            id=1,
            minute_bucket=_minute(2),
            service="checkout",
            endpoint="/pay",
            request_count=100,
            error_count=5,
            p50=50.0,
            p95=90.0,
            p99=95.0,
        ),
        Metric(
            id=2,
            minute_bucket=_minute(1),
            service="checkout",
            endpoint="/pay",
            request_count=120,
            error_count=0,
            p50=55.0,
            p95=92.0,
            p99=97.0,
        ),
        Metric(
            id=3,
            minute_bucket=_minute(1),
            service="checkout",
            endpoint="/cart",
            request_count=80,
            error_count=10,
            p50=40.0,
            p95=70.0,
            p99=80.0,
        ),
        Metric(
            id=4,
            minute_bucket=_minute(1),
            service="auth",
            endpoint="/login",
            request_count=50,
            error_count=0,
            p50=20.0,
            p95=30.0,
            p99=35.0,
        ),
    ]
    session.add_all(rows)
    session.commit()
    yield session
    session.close()


@pytest.fixture
def client(db_session):
    def override_get_db():
        yield db_session

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()


def test_list_services_aggregates_correctly(client) -> None:
    response = client.get("/services", params={"start": WINDOW_START, "end": WINDOW_END})
    assert response.status_code == 200
    body = response.json()
    assert body["pagination"]["total"] == 2
    by_service = {item["service"]: item for item in body["items"]}
    # checkout spans BOTH /pay (100+120=220) and /cart (80): service-level total is
    # the sum across every endpoint, 300 requests / 15 errors (5+0+10).
    assert by_service["checkout"]["request_count"] == 300
    assert by_service["checkout"]["error_count"] == 15
    assert by_service["checkout"]["error_rate"] == pytest.approx(15 / 300)
    assert by_service["auth"]["request_count"] == 50
    assert by_service["auth"]["error_rate"] == 0.0
    assert "p95" not in body["items"][0]


def test_list_services_pagination(client) -> None:
    page1 = client.get(
        "/services", params={"start": WINDOW_START, "end": WINDOW_END, "limit": 1, "offset": 0}
    ).json()
    page2 = client.get(
        "/services", params={"start": WINDOW_START, "end": WINDOW_END, "limit": 1, "offset": 1}
    ).json()
    assert page1["pagination"]["total"] == 2
    assert len(page1["items"]) == 1
    assert len(page2["items"]) == 1
    assert page1["items"][0]["service"] != page2["items"][0]["service"]


def test_list_services_status_class_filter(client) -> None:
    error_only = client.get(
        "/services",
        params={"start": WINDOW_START, "end": WINDOW_END, "status_class": "error"},
    ).json()
    healthy_only = client.get(
        "/services",
        params={"start": WINDOW_START, "end": WINDOW_END, "status_class": "healthy"},
    ).json()
    assert [item["service"] for item in error_only["items"]] == ["checkout"]
    assert [item["service"] for item in healthy_only["items"]] == ["auth"]


def test_list_endpoints_filters_by_service(client) -> None:
    response = client.get(
        "/endpoints",
        params={"start": WINDOW_START, "end": WINDOW_END, "service": "checkout"},
    )
    body = response.json()
    assert body["pagination"]["total"] == 2
    endpoints = {item["endpoint"] for item in body["items"]}
    assert endpoints == {"/pay", "/cart"}


def test_list_endpoints_without_service_filter_returns_all(client) -> None:
    response = client.get("/endpoints", params={"start": WINDOW_START, "end": WINDOW_END})
    body = response.json()
    assert body["pagination"]["total"] == 3


def test_get_service_metrics_returns_unmerged_per_endpoint_series(client) -> None:
    response = client.get(
        "/services/checkout/metrics", params={"start": WINDOW_START, "end": WINDOW_END}
    )
    assert response.status_code == 200
    body = response.json()
    assert body["request_count"] == 300
    assert body["error_count"] == 15

    by_endpoint = {e["endpoint"]: e for e in body["endpoints"]}
    pay = by_endpoint["/pay"]
    assert pay["request_count"] == 220
    assert len(pay["series"]) == 2
    p95_values = {point["p95"] for point in pay["series"]}
    assert p95_values == {90.0, 92.0}

    cart = by_endpoint["/cart"]
    assert cart["request_count"] == 80
    assert len(cart["series"]) == 1
    assert cart["series"][0]["p95"] == 70.0


def test_get_service_metrics_404_for_unknown_service(client) -> None:
    response = client.get(
        "/services/does-not-exist/metrics", params={"start": WINDOW_START, "end": WINDOW_END}
    )
    assert response.status_code == 404
