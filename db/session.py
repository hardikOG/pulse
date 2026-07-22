"""SQLAlchemy engine/session factory, built from Settings (dependency-injected, not global)."""

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from core.config import Settings


def make_engine(settings: Settings) -> Engine:
    """Build a SQLAlchemy engine for the given settings.

    Purpose: construct a database engine without relying on a module-level global,
        so tests can point at a different database than the running process.
    Inputs: settings — a Settings instance providing database_url.
    Outputs: a configured SQLAlchemy Engine (connection pool not yet opened).
    Complexity: O(1).
    Failure cases: raises sqlalchemy.exc.ArgumentError if database_url is malformed;
        does not itself attempt a connection (lazy), so a down database does not raise here.
    """
    return create_engine(settings.database_url, pool_pre_ping=True)


def make_session_factory(engine: Engine) -> sessionmaker[Session]:
    """Build a session factory bound to the given engine.

    Purpose: standard sessionmaker construction, isolated so callers inject an engine
        instead of reaching for a global.
    Inputs: engine — a SQLAlchemy Engine.
    Outputs: a sessionmaker producing Session objects bound to that engine.
    Complexity: O(1).
    Failure cases: none at construction time.
    """
    return sessionmaker(bind=engine, expire_on_commit=False)


@contextmanager
def session_scope(session_factory: sessionmaker[Session]) -> Iterator[Session]:
    """Provide a transactional session scope: commit on success, rollback on error.

    Purpose: standard unit-of-work context manager for callers that need a session
        for the duration of one operation (e.g. one consumer batch).
    Inputs: session_factory — a sessionmaker as returned by make_session_factory.
    Outputs: yields a Session; commits and closes it on clean exit.
    Complexity: O(1) plus the cost of the wrapped operation.
    Failure cases: on any exception inside the `with` block, rolls back before
        re-raising; the caller sees the original exception unchanged.
    """
    session = session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
