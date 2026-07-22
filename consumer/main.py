"""Pulse consumer — the slow path. Phase 0: process skeleton only.

Real XREADGROUP consumption, per-minute aggregation, and anomaly detection land in
Phase 2 and Phase 4. This entrypoint exists now so docker-compose can boot a second,
independent process that reflects the fast/slow split from day one.
"""

import time

from core.config import get_settings
from core.logging import get_logger


def run() -> None:
    """Run the consumer process loop.

    Purpose: process entrypoint for the slow path. Phase 0 stub: proves the process
        boots, connects settings/logging, and stays alive under docker-compose without
        crash-looping. Replaced by real stream consumption in Phase 2.
    Inputs: none (reads configuration from the environment via get_settings()).
    Outputs: none — runs until the process is terminated.
    Complexity: O(1) per idle tick.
    Failure cases: none expected in the Phase 0 stub; real failure-recovery behavior
        (Redis/Postgres unavailability) is implemented in Phase 2.
    """
    settings = get_settings()
    logger = get_logger("pulse.consumer", settings.log_level)
    logger.info(
        "consumer starting",
        extra={
            "extra_fields": {
                "event_stream": settings.event_stream,
                "consumer_group": settings.consumer_group,
                "consumer_name": settings.consumer_name,
            }
        },
    )
    while True:
        time.sleep(5)
        logger.info("consumer alive")


if __name__ == "__main__":
    run()
