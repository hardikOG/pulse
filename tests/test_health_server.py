"""Unit tests for consumer/health_server.py — the Render free-tier port-binding
workaround. See the module docstring for why this exists at all."""

import http.client
import time
from unittest.mock import MagicMock

from consumer.health_server import maybe_start_health_server


def test_does_nothing_when_port_env_var_unset(monkeypatch) -> None:
    monkeypatch.delenv("PORT", raising=False)
    logger = MagicMock()

    maybe_start_health_server(logger)

    logger.info.assert_not_called()
    logger.error.assert_not_called()


def test_serves_200_ok_when_port_env_var_set(monkeypatch) -> None:
    monkeypatch.setenv("PORT", "18080")
    logger = MagicMock()

    maybe_start_health_server(logger)
    time.sleep(0.1)  # let the daemon thread's server start accepting connections

    conn = http.client.HTTPConnection("127.0.0.1", 18080, timeout=5)
    try:
        conn.request("GET", "/")
        response = conn.getresponse()
        assert response.status == 200
        assert response.read() == b'{"status":"ok"}'
    finally:
        conn.close()
    logger.info.assert_called_once()


def test_logs_and_does_not_raise_when_port_is_invalid(monkeypatch) -> None:
    monkeypatch.setenv("PORT", "not-a-port")
    logger = MagicMock()

    maybe_start_health_server(logger)  # must not raise

    logger.error.assert_called_once()
