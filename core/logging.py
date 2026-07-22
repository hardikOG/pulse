"""Structured logging setup shared by the api and consumer processes."""

import logging
import sys
from datetime import datetime, timezone
from typing import Any


class _StructuredFormatter(logging.Formatter):
    """Renders log records as single-line structured text of key=value pairs."""

    def format(self, record: logging.LogRecord) -> str:
        """Render one log record as a structured, greppable line.

        Purpose: produce logs that are human-readable but trivially parseable
            (key=value pairs), without pulling in a JSON logging dependency.
        Inputs: record — the standard library LogRecord being emitted.
        Outputs: a single-line string.
        Complexity: O(k) in the number of extra fields attached to the record.
        Failure cases: none — falls back to str() of the message if formatting args
            are malformed, matching stdlib logging's own behavior.
        """
        fields: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        extra = getattr(record, "extra_fields", None)
        if extra:
            fields.update(extra)
        if record.exc_info:
            fields["exc_info"] = self.formatException(record.exc_info)
        return " ".join(f"{k}={v!r}" for k, v in fields.items())


def get_logger(name: str, level: str = "INFO") -> logging.Logger:
    """Return a configured structured logger for the given component name.

    Purpose: single place that wires up structured, stdout-bound logging so api,
        consumer, and detector all log in the same format.
    Inputs: name — dotted logger name (e.g. "pulse.api", "pulse.consumer");
        level — the minimum level to emit, as a standard logging level name.
    Outputs: a logging.Logger with a single stdout handler attached (idempotent —
        calling this twice for the same name does not duplicate handlers).
    Complexity: O(1).
    Failure cases: raises ValueError from logging.setLevel if level is not a
        recognized level name.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(stream=sys.stdout)
        handler.setFormatter(_StructuredFormatter())
        logger.addHandler(handler)
        logger.propagate = False
    logger.setLevel(level)
    return logger
