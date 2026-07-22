"""SQLAlchemy ORM models for Pulse's two time-series tables.

Schema is fixed per CLAUDE.md as of Phase 0. Table creation happens in Phase 2 — this
module only defines structure so later phases (read API, detector) can import stable
model classes without the schema shifting under them.
"""

from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Float, Index, Integer, String, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Declarative base shared by all Pulse ORM models."""


class Metric(Base):
    """One per-minute, per-(service, endpoint) aggregation bucket.

    Purpose: stores the rollup a consumer produces from raw events — count, error
        count, and latency percentiles for a single minute bucket.
    Inputs: n/a (ORM model).
    Outputs: n/a (ORM model).
    Complexity: n/a.
    Failure cases: a duplicate (minute_bucket, service, endpoint) violates the unique
        constraint — the consumer must upsert, not insert, on conflict.
    """

    __tablename__ = "metrics"
    __table_args__ = (
        UniqueConstraint("minute_bucket", "service", "endpoint", name="uq_metrics_bucket"),
        Index("ix_metrics_service_bucket", "service", "minute_bucket"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    minute_bucket: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    service: Mapped[str] = mapped_column(String, nullable=False)
    endpoint: Mapped[str] = mapped_column(String, nullable=False)
    request_count: Mapped[int] = mapped_column(Integer, nullable=False)
    error_count: Mapped[int] = mapped_column(Integer, nullable=False)
    p50: Mapped[float] = mapped_column(Float, nullable=False)
    p95: Mapped[float] = mapped_column(Float, nullable=False)
    p99: Mapped[float] = mapped_column(Float, nullable=False)


class Anomaly(Base):
    """One flagged anomaly against a specific minute bucket.

    Purpose: records that a detector fired for a given (service, endpoint, minute),
        which detector fired, and its score/reason.
    Inputs: n/a (ORM model).
    Outputs: n/a (ORM model).
    Complexity: n/a.
    Failure cases: none beyond standard FK/NOT NULL constraints (no FK to metrics by
        design — anomalies can reference a bucket even if written in a separate
        transaction from the rollup).
    """

    __tablename__ = "anomalies"
    __table_args__ = (Index("ix_anomalies_service_bucket", "service", "minute_bucket"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    minute_bucket: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    service: Mapped[str] = mapped_column(String, nullable=False)
    endpoint: Mapped[str] = mapped_column(String, nullable=False)
    detector: Mapped[str] = mapped_column(String, nullable=False)
    score: Mapped[float] = mapped_column(Float, nullable=False)
    reason: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
