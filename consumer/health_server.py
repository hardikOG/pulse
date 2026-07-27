"""Minimal HTTP health endpoint for the consumer process — exists solely so a
hosting platform that only offers a free tier to services bound to an HTTP port
(e.g. Render, which gates its Background Worker product behind a paid plan but not
its Web Service product) can run the consumer without paying for a worker instance.
Does nothing locally or under docker-compose, where no PORT environment variable is
set — the consumer's actual behavior (Redis consumer group, aggregation, detection)
is completely unaffected by this module either way.
"""

import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class _HealthHandler(BaseHTTPRequestHandler):
    """Responds 200 to any GET — just enough for a platform's port/health probe."""

    def do_GET(self) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status":"ok"}')

    def log_message(self, format: str, *args: object) -> None:
        pass  # silence BaseHTTPRequestHandler's default per-request stderr logging


def maybe_start_health_server(logger) -> None:
    """Start a minimal HTTP health server bound to $PORT, if that variable is set.

    Purpose: satisfies a hosting platform's port-binding requirement for its free
        service tier, without changing anything about the consumer's real job — see
        the module docstring for why this exists at all.
    Inputs: logger.
    Outputs: None. Starts a daemon thread serving GET requests with a static
        {"status": "ok"} response if PORT is set in the environment; does nothing
        otherwise (docker-compose and a plain `python -m consumer.main` never set it).
    Complexity: O(1).
    Failure cases: never raises — if binding the port fails for any reason (already
        in use, invalid value), logs and continues without the health server, since
        it is not required for the consumer's actual job and must never prevent it
        from starting.
    """
    port_str = os.environ.get("PORT")
    if not port_str:
        return
    try:
        port = int(port_str)
        server = ThreadingHTTPServer(("0.0.0.0", port), _HealthHandler)
    except (ValueError, OSError) as exc:
        logger.error(
            "health server failed to bind, continuing without it",
            extra={"extra_fields": {"port": port_str, "error": str(exc)}},
        )
        return
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info("health server listening", extra={"extra_fields": {"port": port}})
