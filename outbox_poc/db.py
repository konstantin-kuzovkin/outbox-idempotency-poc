"""
Database schema for the outbox proof of concept.

SQLite is used only so the PoC has zero external dependencies and runs
anywhere with plain Python. The schema and the transaction pattern are the
same ones described in the portfolio (case 01 state-machine.md and case 02
outbox.md); porting this to PostgreSQL means swapping the connection and
keeping the SQL almost unchanged (see NOTES.md).
"""
import sqlite3


def connect(path: str = ":memory:") -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


SCHEMA = """
CREATE TABLE IF NOT EXISTS transfer_operation (
    operation_id     TEXT PRIMARY KEY,
    client_id        TEXT NOT NULL,
    idempotency_key  TEXT NOT NULL,
    request_hash     TEXT NOT NULL,
    state            TEXT NOT NULL,
    version          INTEGER NOT NULL DEFAULT 0,
    amount_minor     INTEGER NOT NULL,
    currency         TEXT NOT NULL,
    created_at       TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at       TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (client_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS operation_transition (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    operation_id   TEXT NOT NULL REFERENCES transfer_operation(operation_id),
    from_state     TEXT NOT NULL,
    to_state       TEXT NOT NULL,
    trigger        TEXT NOT NULL,
    created_at     TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS outbox_event (
    event_id       TEXT PRIMARY KEY,
    aggregate_id   TEXT NOT NULL,
    event_type     TEXT NOT NULL,
    payload        TEXT NOT NULL,
    created_at     TEXT NOT NULL DEFAULT (datetime('now')),
    published_at   TEXT
);

CREATE TABLE IF NOT EXISTS processed_event (
    consumer       TEXT NOT NULL,
    event_id       TEXT NOT NULL,
    processed_at   TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (consumer, event_id)
);
"""


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.commit()
