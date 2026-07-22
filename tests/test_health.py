"""Phase 0 gate: the API process boots and /health responds correctly."""

from fastapi.testclient import TestClient

from api.main import app


def test_health_returns_200_ok() -> None:
    client = TestClient(app)
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
