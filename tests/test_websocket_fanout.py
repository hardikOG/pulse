"""Regression test for a race in api.main._pubsub_fanout: a WebSocket client's
send_text (or the /ws route's own disconnect handling, concurrently) can mutate
app.state.websocket_clients while the fanout loop is mid-iteration over it. Iterating
the live set directly raises "Set changed size during iteration" and kills the fanout
task permanently — found via a fresh-eyes audit pass, not a symptom anyone had hit yet.
"""

import asyncio
import json
from contextlib import suppress
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api.main import _pubsub_fanout


class _FakePubSub:
    """A message once, then blocks forever (like the real pubsub.get_message with no
    traffic) until the fanout task is cancelled — mirrors real shutdown behavior."""

    def __init__(self, messages: list[dict]) -> None:
        self._messages = list(messages)
        self.subscribe = AsyncMock()
        self.unsubscribe = AsyncMock()
        self.aclose = AsyncMock()

    async def get_message(self, ignore_subscribe_messages: bool, timeout: float):
        if self._messages:
            return self._messages.pop(0)
        await asyncio.sleep(3600)
        return None


class _FakeRedis:
    def __init__(self, pubsub: _FakePubSub) -> None:
        self._pubsub = pubsub

    def pubsub(self) -> _FakePubSub:
        return self._pubsub


@pytest.mark.asyncio
async def test_fanout_survives_client_set_mutated_mid_iteration() -> None:
    """A send to one client that triggers another client's removal from the set
    (simulating a concurrent /ws disconnect) must not crash the fanout loop."""
    clients: set = set()

    class _MutatingClient:
        """Its send_text discards a sibling client from the shared set, reproducing
        the concurrent-mutation race a real disconnect during fanout would cause."""

        async def send_text(self, data: str) -> None:
            clients.discard(other_client)

    class _NormalClient:
        def __init__(self) -> None:
            self.received: list[str] = []

        async def send_text(self, data: str) -> None:
            self.received.append(data)

    mutating_client = _MutatingClient()
    other_client = _NormalClient()
    clients.add(mutating_client)
    clients.add(other_client)

    app = SimpleNamespace(state=SimpleNamespace(websocket_clients=clients))
    message = {"data": json.dumps({"type": "lag", "pending_count": 0})}
    pubsub = _FakePubSub([message])
    redis_client = _FakeRedis(pubsub)

    task = asyncio.create_task(_pubsub_fanout(app, redis_client))  # type: ignore[arg-type]
    await asyncio.sleep(0.05)  # let the fanout process the one queued message

    # If the loop crashed on the set-mutation race, the task already finished with a
    # RuntimeError — cancel() on a done task is a no-op, and awaiting it below
    # re-raises that real exception instead of a CancelledError, giving a clear
    # failure. If the fix holds, the task is still blocked in get_message and this
    # cancels it cleanly.
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task

    pubsub.unsubscribe.assert_awaited_once()
    pubsub.aclose.assert_awaited_once()
