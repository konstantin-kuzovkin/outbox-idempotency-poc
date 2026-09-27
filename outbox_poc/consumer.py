"""
IdempotentConsumer: the pattern from outbox.md "idempotent consumer" section.

The dedup record (processed_event) and the business effect are written in
one transaction. If a message is redelivered (broker retry, consumer
restart, rebalance), the second delivery is detected and skipped before the
effect runs a second time.
"""
import json
import sqlite3


class IdempotentConsumer:
    def __init__(self, conn: sqlite3.Connection, consumer_name: str):
        self.conn = conn
        self.consumer_name = consumer_name
        self.effects: list[dict] = []  # records of business effects actually applied

    def handle(self, payload: str) -> bool:
        """Returns True if the effect was applied, False if it was a duplicate."""
        event = json.loads(payload)
        event_id = event["eventId"]

        with self.conn:
            cur = self.conn.execute(
                """INSERT OR IGNORE INTO processed_event (consumer, event_id)
                   VALUES (?, ?)""",
                (self.consumer_name, event_id),
            )
            if cur.rowcount == 0:
                return False  # duplicate: already processed, skip the effect

            self._apply_effect(event)

        return True

    def _apply_effect(self, event: dict) -> None:
        # In a real consumer this would be "send a notification", "update a
        # read model", etc. Here we just record it so tests can assert on it.
        self.effects.append(event)
