"""SQLite connection helpers and schema management.

Concurrency model
-----------------
Every request opens its own connection. Writes run inside ``BEGIN IMMEDIATE``
transactions, which acquire SQLite's reserved lock up-front; concurrent writers
therefore serialize at the database level (``busy_timeout`` makes them wait
instead of failing immediately). Each request handler performs all of its
state transitions in one such transaction, so any committed state is
equivalent to some serial ordering of the requests.
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator

import aiosqlite

DB_PATH = Path(os.environ.get("DB_PATH", "/data/firmware.db"))

#: Wait this long for a write lock held by another connection.
BUSY_TIMEOUT_MS = 10_000

SCHEMA = """
PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS groups (
    group_id   TEXT PRIMARY KEY,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS devices (
    group_id        TEXT NOT NULL REFERENCES groups(group_id),
    device_id       TEXT NOT NULL,
    current_version TEXT NOT NULL,
    PRIMARY KEY (group_id, device_id)
);

CREATE TABLE IF NOT EXISTS batches (
    batch_id   TEXT PRIMARY KEY,
    group_id   TEXT NOT NULL REFERENCES groups(group_id),
    status     TEXT NOT NULL CHECK (status IN ('pending', 'succeeded', 'failed')),
    created_at TEXT NOT NULL,
    ended_at   TEXT
);

CREATE INDEX IF NOT EXISTS idx_batches_group ON batches(group_id);

-- At most one unfinished (pending) batch per group at any time.
CREATE UNIQUE INDEX IF NOT EXISTS ux_pending_batch_per_group
    ON batches(group_id) WHERE status = 'pending';

CREATE TABLE IF NOT EXISTS batch_targets (
    batch_id       TEXT NOT NULL REFERENCES batches(batch_id),
    group_id       TEXT NOT NULL,
    device_id      TEXT NOT NULL,
    target_version TEXT NOT NULL,
    PRIMARY KEY (batch_id, device_id),
    FOREIGN KEY (group_id, device_id) REFERENCES devices(group_id, device_id)
);

CREATE TABLE IF NOT EXISTS receipts (
    batch_id   TEXT NOT NULL REFERENCES batches(batch_id),
    device_id  TEXT NOT NULL,
    result     TEXT NOT NULL CHECK (result IN ('ready', 'failed')),
    created_at TEXT NOT NULL,
    PRIMARY KEY (batch_id, device_id)
);
"""


async def init_db(db_path: Path | str = DB_PATH) -> None:
    """Create the data directory and initialize schema (idempotent)."""
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    async with aiosqlite.connect(str(path)) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("PRAGMA journal_mode = WAL")
        for statement in SCHEMA.split(";"):
            if statement.strip():
                await db.execute(statement)
        await db.commit()


@asynccontextmanager
async def connect(db_path: Path | str = DB_PATH) -> AsyncIterator[aiosqlite.Connection]:
    """A single connection for one request, with sensible PRAGMAs."""
    db = await aiosqlite.connect(str(db_path))
    db.row_factory = aiosqlite.Row
    try:
        await db.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
        await db.execute("PRAGMA foreign_keys = ON")
        yield db
    finally:
        await db.close()


@asynccontextmanager
async def read_tx(db: aiosqlite.Connection):
    """Deferred read transaction: multi-statement reads see one consistent snapshot."""
    await db.execute("BEGIN")
    try:
        yield
    except Exception:
        await db.rollback()
        raise
    else:
        await db.commit()


@asynccontextmanager
async def write_tx(db: aiosqlite.Connection) -> AsyncIterator[None]:
    """``BEGIN IMMEDIATE`` — writers serialize, deadlock-free with SQLite's lock escalation.

    Commits on clean exit, rolls back on exception.
    """
    await db.execute("BEGIN IMMEDIATE")
    try:
        yield
    except Exception:
        await db.rollback()
        raise
    else:
        await db.commit()
