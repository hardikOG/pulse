"""Environment-driven settings for all Pulse processes (api, consumer, benchmark, simulator)."""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Typed, validated view of Pulse's environment configuration.

    Purpose: single source of truth for every tunable value in Pulse, loaded from
        environment variables (or a local .env file) so nothing is hardcoded.
    Inputs: none directly — populated by pydantic-settings from the process environment.
    Outputs: n/a (this is a data container).
    Complexity: O(1) — read once per process via get_settings().
    Failure cases: pydantic raises a ValidationError at process startup if a required
        variable is missing or malformed; there is no silent fallback for connection URLs.
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Core infra
    database_url: str = "postgresql+psycopg://pulse:pulse@localhost:5432/pulse"
    redis_url: str = "redis://localhost:6379/0"

    # Redis stream (used from Phase 1/2 onward)
    event_stream: str = "pulse:events"
    consumer_group: str = "pulse-consumers"
    consumer_name: str = "consumer-1"
    stream_batch_size: int = 200
    stream_block_timeout_ms: int = 5000
    redis_socket_timeout_seconds: float = 2.0

    # Aggregation (Phase 2 onward)
    aggregation_window_seconds: int = 60
    bucket_eviction_grace_seconds: int = 120
    postgres_max_retries: int = 3
    postgres_retry_backoff_seconds: float = 1.0

    # Anomaly detection (Phase 4 onward)
    zscore_threshold: float = 3.0
    ewma_alpha: float = 0.3
    isolation_forest_contamination: float = 0.05

    # Alerts (Phase 5 onward)
    webhook_url: str | None = None
    webhook_max_retries: int = 3

    # Runtime
    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    """Return the process-wide Settings instance, constructed once and cached.

    Purpose: dependency-injectable settings accessor — avoids a module-level mutable
        singleton while still avoiding re-parsing the environment on every call.
    Inputs: none.
    Outputs: a Settings instance.
    Complexity: O(1) amortized (cached after first call).
    Failure cases: propagates pydantic ValidationError on first call if env is invalid.
    """
    return Settings()
