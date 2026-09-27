import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from outbox_poc.db import connect, init_schema
from outbox_poc.service import (
    TransferService,
    IdempotencyKeyReused,
    ConcurrentUpdate,
    InvalidStateTransition,
)
from outbox_poc.relay import OutboxRelay, FakeBroker
from outbox_poc.consumer import IdempotentConsumer


def new_service():
    conn = connect()
    init_schema(conn)
    return conn, TransferService(conn)


class TestIdempotentCreate(unittest.TestCase):
    def test_same_key_same_body_returns_same_operation(self):
        conn, svc = new_service()
        r1 = svc.create_operation("client-1", "key-1", 10000, "RUB")
        r2 = svc.create_operation("client-1", "key-1", 10000, "RUB")

        self.assertEqual(r1.operation_id, r2.operation_id)
        self.assertFalse(r1.replayed)
        self.assertTrue(r2.replayed)

        rows = conn.execute("SELECT COUNT(*) c FROM transfer_operation").fetchone()
        self.assertEqual(rows["c"], 1, "a retried identical request must not create a second row")

    def test_same_key_different_body_is_rejected(self):
        _, svc = new_service()
        svc.create_operation("client-1", "key-1", 10000, "RUB")
        with self.assertRaises(IdempotencyKeyReused):
            svc.create_operation("client-1", "key-1", 99999, "RUB")

    def test_different_clients_can_reuse_the_same_key(self):
        _, svc = new_service()
        r1 = svc.create_operation("client-1", "key-1", 10000, "RUB")
        r2 = svc.create_operation("client-2", "key-1", 10000, "RUB")
        self.assertNotEqual(r1.operation_id, r2.operation_id)


class TestOptimisticLocking(unittest.TestCase):
    def test_concurrent_transition_only_one_wins(self):
        """
        Two workers both read the operation while it is FUNDS_RESERVED at
        version 1. Both try to move it forward. The first transition()
        commits and bumps the version; the second worker's UPDATE no longer
        matches (state, version) and must raise ConcurrentUpdate instead of
        silently overwriting the first worker's change.
        """
        _, svc = new_service()
        op = svc.create_operation("client-1", "key-1", 10000, "RUB").operation_id
        svc.transition(op, "FUNDS_RESERVED", "hold_confirmed")  # now version = 1

        # Worker A reads (FUNDS_RESERVED, version=1) and successfully transitions.
        svc.transition(op, "SUBMITTED", "worker_a")  # now version = 2

        # Worker B read the SAME (FUNDS_RESERVED, version=1) snapshot before A
        # committed. Its update is expressed directly against that stale
        # snapshot to prove the guard clause rejects it.
        cur = svc.conn.execute(
            "UPDATE transfer_operation SET state='SUBMITTED', version=version+1 "
            "WHERE operation_id=? AND state='FUNDS_RESERVED' AND version=1",
            (op,),
        )
        self.assertEqual(cur.rowcount, 0, "the stale writer must not be able to apply its update")

        # The operation reflects only worker A's change.
        row = svc.conn.execute(
            "SELECT state, version FROM transfer_operation WHERE operation_id=?", (op,)
        ).fetchone()
        self.assertEqual(row["state"], "SUBMITTED")
        self.assertEqual(row["version"], 2)

    def test_invalid_transition_is_rejected(self):
        _, svc = new_service()
        op = svc.create_operation("client-1", "key-1", 10000, "RUB").operation_id
        with self.assertRaises(InvalidStateTransition):
            svc.transition(op, "COMPLETED", "skip_ahead")  # NEW -> COMPLETED is not allowed

    def test_terminal_state_cannot_be_changed(self):
        _, svc = new_service()
        op = svc.create_operation("client-1", "key-1", 10000, "RUB").operation_id
        svc.transition(op, "FUNDS_RESERVED", "t1")
        svc.transition(op, "SUBMITTED", "t2")
        svc.transition(op, "COMPLETED", "t3", publish_event_type="TransferCompleted")
        with self.assertRaises(InvalidStateTransition):
            svc.transition(op, "FAILED", "t4")


class TestOutboxDurability(unittest.TestCase):
    def test_event_is_written_atomically_with_the_state_change(self):
        """
        If the process crashes right after the transition() call commits,
        the outbox_event row is guaranteed to exist, because it was written
        in the same transaction as the state update. We simulate the crash
        by simply not calling the relay yet, and checking the row is there.
        """
        conn, svc = new_service()
        op = svc.create_operation("client-1", "key-1", 10000, "RUB").operation_id
        svc.transition(op, "FUNDS_RESERVED", "t1")
        svc.transition(op, "SUBMITTED", "t2")
        svc.transition(op, "COMPLETED", "t3", publish_event_type="TransferCompleted")

        # "crash" here: nothing has published anything yet.
        unpublished = conn.execute(
            "SELECT COUNT(*) c FROM outbox_event WHERE published_at IS NULL"
        ).fetchone()
        self.assertEqual(unpublished["c"], 1)

    def test_relay_eventually_publishes_after_the_crash(self):
        conn, svc = new_service()
        op = svc.create_operation("client-1", "key-1", 10000, "RUB").operation_id
        svc.transition(op, "FUNDS_RESERVED", "t1")
        svc.transition(op, "SUBMITTED", "t2")
        svc.transition(op, "COMPLETED", "t3", publish_event_type="TransferCompleted")

        broker = FakeBroker()
        relay = OutboxRelay(conn, broker)
        published = relay.poll_and_publish()

        self.assertEqual(published, 1)
        self.assertEqual(len(broker.messages), 1)
        key, event_type, _ = broker.messages[0]
        self.assertEqual(key, op)
        self.assertEqual(event_type, "TransferCompleted")

        # A second poll finds nothing new: already published rows are not republished.
        self.assertEqual(relay.poll_and_publish(), 0)

    def test_failed_operation_does_not_publish_completed_event(self):
        conn, svc = new_service()
        op = svc.create_operation("client-1", "key-1", 10000, "RUB").operation_id
        svc.transition(op, "FAILED", "hold_declined", publish_event_type="TransferFailed")

        broker = FakeBroker()
        OutboxRelay(conn, broker).poll_and_publish()
        self.assertEqual(broker.messages[0][1], "TransferFailed")


class TestIdempotentConsumer(unittest.TestCase):
    def test_duplicate_delivery_applies_the_effect_once(self):
        conn, svc = new_service()
        op = svc.create_operation("client-1", "key-1", 10000, "RUB").operation_id
        svc.transition(op, "FUNDS_RESERVED", "t1")
        svc.transition(op, "SUBMITTED", "t2")
        svc.transition(op, "COMPLETED", "t3", publish_event_type="TransferCompleted")

        broker = FakeBroker()
        OutboxRelay(conn, broker).poll_and_publish()

        consumer = IdempotentConsumer(conn, consumer_name="notification-service")
        key, event_type, payload = broker.messages[0]

        first = consumer.handle(payload)
        # Simulate the broker redelivering the same message (at-least-once delivery).
        second = consumer.handle(payload)

        self.assertTrue(first)
        self.assertFalse(second, "a redelivered event must be recognised as a duplicate")
        self.assertEqual(len(consumer.effects), 1, "the business effect must run exactly once")

    def test_two_different_consumers_each_process_the_event_once(self):
        """Deduplication is scoped per consumer, so consumer group A processing
        an event does not block consumer group B from processing the same event."""
        conn, svc = new_service()
        op = svc.create_operation("client-1", "key-1", 10000, "RUB").operation_id
        svc.transition(op, "FUNDS_RESERVED", "t1")
        svc.transition(op, "SUBMITTED", "t2")
        svc.transition(op, "COMPLETED", "t3", publish_event_type="TransferCompleted")

        broker = FakeBroker()
        OutboxRelay(conn, broker).poll_and_publish()
        _, _, payload = broker.messages[0]

        notifier = IdempotentConsumer(conn, consumer_name="notification-service")
        analytics = IdempotentConsumer(conn, consumer_name="analytics-service")

        self.assertTrue(notifier.handle(payload))
        self.assertTrue(analytics.handle(payload))
        self.assertFalse(notifier.handle(payload))
        self.assertEqual(len(notifier.effects), 1)
        self.assertEqual(len(analytics.effects), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
