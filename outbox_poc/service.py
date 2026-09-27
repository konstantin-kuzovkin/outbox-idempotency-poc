"""
TransferService: a small version of the service described in
01-payment-transfer-architecture/state-machine.md.

Two things are demonstrated here, on purpose:

1. Idempotent creation: the same (client_id, idempotency_key) never creates
   a second row, using a unique constraint plus a request_hash check.
2. Outbox write: the final-state transition and the outbox_event row are
   written in the same DB transaction, so a crash cannot separate them
   (see 02-event-driven-architecture/outbox.md).
"""
import hashlib
import json
import sqlite3
import uuid
from dataclasses import dataclass


class IdempotencyKeyReused(Exception):
    pass


class InvalidStateTransition(Exception):
    pass


class ConcurrentUpdate(Exception):
    pass


def _hash_request(body: dict) -> str:
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


@dataclass
class CreateResult:
    operation_id: str
    state: str
    replayed: bool


TERMINAL_STATES = {"COMPLETED", "FAILED"}

ALLOWED_TRANSITIONS = {
    "NEW": {"FUNDS_RESERVED", "FAILED"},
    "FUNDS_RESERVED": {"SUBMITTED"},
    "SUBMITTED": {"COMPLETED", "FAILED", "UNKNOWN"},
    "UNKNOWN": {"COMPLETED", "FAILED", "MANUAL_INVESTIGATION"},
    "MANUAL_INVESTIGATION": {"COMPLETED", "FAILED"},
}


class TransferService:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    # ---- idempotent creation --------------------------------------------
    def create_operation(
        self,
        client_id: str,
        idempotency_key: str,
        amount_minor: int,
        currency: str,
    ) -> CreateResult:
        body = {
            "client_id": client_id,
            "amount_minor": amount_minor,
            "currency": currency,
        }
        request_hash = _hash_request(body)

        existing = self.conn.execute(
            "SELECT * FROM transfer_operation WHERE client_id = ? AND idempotency_key = ?",
            (client_id, idempotency_key),
        ).fetchone()

        if existing:
            if existing["request_hash"] != request_hash:
                raise IdempotencyKeyReused(
                    f"key {idempotency_key!r} was used before with a different request"
                )
            return CreateResult(existing["operation_id"], existing["state"], replayed=True)

        operation_id = str(uuid.uuid4())
        try:
            with self.conn:
                self.conn.execute(
                    """INSERT INTO transfer_operation
                       (operation_id, client_id, idempotency_key, request_hash,
                        state, version, amount_minor, currency)
                       VALUES (?, ?, ?, ?, 'NEW', 0, ?, ?)""",
                    (operation_id, client_id, idempotency_key, request_hash, amount_minor, currency),
                )
                self._record_transition(operation_id, "NONE", "NEW", "create_operation")
        except sqlite3.IntegrityError:
            # Lost the race to a concurrent identical request; return its row (replay).
            existing = self.conn.execute(
                "SELECT * FROM transfer_operation WHERE client_id = ? AND idempotency_key = ?",
                (client_id, idempotency_key),
            ).fetchone()
            return CreateResult(existing["operation_id"], existing["state"], replayed=True)

        return CreateResult(operation_id, "NEW", replayed=False)

    # ---- state transition with optimistic locking + outbox write --------
    def transition(
        self,
        operation_id: str,
        to_state: str,
        trigger: str,
        publish_event_type: str | None = None,
    ) -> None:
        """
        Move an operation to a new state. If publish_event_type is given and
        to_state is terminal, an outbox_event row is written in the SAME
        transaction as the state update: this is the core of the pattern.
        """
        row = self.conn.execute(
            "SELECT state, version FROM transfer_operation WHERE operation_id = ?",
            (operation_id,),
        ).fetchone()
        if row is None:
            raise ValueError(f"unknown operation {operation_id}")

        current_state, version = row["state"], row["version"]
        if current_state in TERMINAL_STATES:
            raise InvalidStateTransition(f"{current_state} is terminal")
        if to_state not in ALLOWED_TRANSITIONS.get(current_state, set()):
            raise InvalidStateTransition(f"{current_state} -> {to_state} is not allowed")

        with self.conn:
            cur = self.conn.execute(
                """UPDATE transfer_operation
                   SET state = ?, version = version + 1, updated_at = datetime('now')
                   WHERE operation_id = ? AND state = ? AND version = ?""",
                (to_state, operation_id, current_state, version),
            )
            if cur.rowcount == 0:
                # Someone else changed the operation between our SELECT and UPDATE.
                raise ConcurrentUpdate(
                    f"operation {operation_id} was modified concurrently; reload and retry"
                )

            self._record_transition(operation_id, current_state, to_state, trigger)

            if publish_event_type and to_state in TERMINAL_STATES:
                payload = json.dumps(
                    {
                        "eventId": str(uuid.uuid4()),
                        "eventType": publish_event_type,
                        "operationId": operation_id,
                        "status": to_state,
                    }
                )
                self.conn.execute(
                    """INSERT INTO outbox_event (event_id, aggregate_id, event_type, payload)
                       VALUES (?, ?, ?, ?)""",
                    (str(uuid.uuid4()), operation_id, publish_event_type, payload),
                )

    def _record_transition(self, operation_id, from_state, to_state, trigger):
        self.conn.execute(
            """INSERT INTO operation_transition (operation_id, from_state, to_state, trigger)
               VALUES (?, ?, ?, ?)""",
            (operation_id, from_state, to_state, trigger),
        )
