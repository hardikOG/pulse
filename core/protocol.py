"""Shared wire-protocol constants for the live-update WebSocket/Pub/Sub channel.

Lives in core/ (not consumer/) specifically because both the API (which sends the
snapshot message) and the consumer (which sends metric_point/anomaly/lag messages)
need it, and core/ is the one package both Docker images actually copy in — a
constant defined in consumer/ would import fine in local dev but fail at runtime in
the api container, which never gets a copy of consumer/.
"""

# Bumped whenever a breaking change is made to the live-update message envelope (a
# field renamed/removed, a type's meaning changed) — never for additive changes like
# a new message type or a new optional field. Lets any subscriber detect a protocol
# mismatch instead of silently misinterpreting a differently-shaped message.
LIVE_UPDATES_SCHEMA_VERSION = 1
