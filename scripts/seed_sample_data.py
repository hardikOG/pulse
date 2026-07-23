"""Populate a running Pulse deployment with sample data, without running the full
benchmark harness.

Sends backdated warmup history (through the real POST /events API, exactly like
benchmark/run.py's warmup phase — see docs/private/ARCHITECTURE_LEDGER.md for why
that's through the API and not a direct DB seed) so the dashboard has service tiles
and per-endpoint charts to show immediately, then optionally a short live burst so
the WebSocket live-update path and anomaly detection have something to demonstrate.

Usage (against a running `docker compose up` stack):
    python scripts/seed_sample_data.py
    python scripts/seed_sample_data.py --url http://localhost:8000 --scenario peak
    python scripts/seed_sample_data.py --no-live
"""

import argparse
import asyncio

import httpx

from simulator.generator import run_live_phase, run_warmup
from simulator.scenarios import SCENARIOS


async def _seed(url: str, scenario_name: str, live_seconds: float | None) -> None:
    scenario = SCENARIOS[scenario_name]
    events_url = f"{url.rstrip('/')}/events"

    async with httpx.AsyncClient(timeout=10.0) as client:
        print(f"seeding '{scenario.name}' warmup history against {events_url}...")
        warmed = await run_warmup(client, events_url, scenario)
        print(f"  {warmed} warmup events accepted")

        if live_seconds is None:
            return

        # A short live phase so the dashboard's WebSocket updates and anomaly
        # detection have something to show, not just static history.
        short_scenario = scenario.__class__(
            name=scenario.name,
            services=scenario.services,
            base_rate_per_second=scenario.base_rate_per_second,
            shape="constant",
            live_duration_seconds=live_seconds,
            warmup_minutes=scenario.warmup_minutes,
            anomalies=tuple(a for a in scenario.anomalies if a.offset_seconds < live_seconds),
        )
        print(f"  running {live_seconds:.0f}s of live traffic (open the dashboard now)...")
        live = await run_live_phase(client, events_url, short_scenario)
        print(
            f"  {live.events_sent} sent, {live.events_failed} failed, "
            f"{len(live.labels)} anomalies injected"
        )

    print("done. open the dashboard to see the results.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--url", default="http://localhost:8000", help="Pulse API base URL (default: %(default)s)"
    )
    parser.add_argument(
        "--scenario",
        default="steady",
        choices=sorted(SCENARIOS),
        help="traffic scenario to seed from (default: %(default)s)",
    )
    parser.add_argument(
        "--live-seconds",
        type=float,
        default=60.0,
        help="seconds of live traffic to send after warmup (default: %(default)s)",
    )
    parser.add_argument(
        "--no-live", action="store_true", help="skip the live phase, seed warmup history only"
    )
    args = parser.parse_args()

    live_seconds = None if args.no_live else args.live_seconds
    asyncio.run(_seed(args.url, args.scenario, live_seconds))


if __name__ == "__main__":
    main()
