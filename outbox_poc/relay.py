"""
OutboxRelay: the "polling relay" option from outbox.md.

It reads unpublished rows in created_at order, hands each one to a broker,
and marks it published only after the broker accepted it. If the process
dies between "hand to broker" and "mark published", the row is picked up
again on the next poll -- this is what gives at-least-once delivery, which
is exactly why the consumer must be idempotent.
"""
import sqlite3
from typing import Protocol


class Broker(Protocol):
    def publish(self, key: str, event_type: str, payload: str) -> None: ...


class FakeBroker:
    """Stand-in for Kafka: an in-memory, ordered, per-key queue."""

    def __init__(self):
        self.messages: list[tuple[str, str, str]] = []  # (key, event_type, payload)

    def publish(self, key: str, event_type: str, payload: str) -> None:
        self.messages.append((key, event_type, payload))


class OutboxRelay:
    def __init__(self, conn: sqlite3.Connection, broker: Broker):
        self.conn = conn
        self.broker = broker

    def poll_and_publish(self, batch_size: int = 100) -> int:
        rows = self.conn.execute(
            """SELECT event_id, aggregate_id, event_type, payload
               FROM outbox_event WHERE published_at IS NULL
               ORDER BY created_at LIMIT ?""",
            (batch_size,),
        ).fetchall()

        published = 0
        for row in rows:
            self.broker.publish(row["aggregate_id"], row["event_type"], row["payload"])
            self.conn.execute(
                "UPDATE outbox_event SET published_at = datetime('now') WHERE event_id = ?",
                (row["event_id"],),
            )
            self.conn.commit()
            published += 1
        return published
