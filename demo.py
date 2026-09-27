"""
Run: python3 demo.py

Walks through one successful transfer end to end and prints what happens at
each step, so you can see the outbox + idempotent consumer pattern working
without reading the tests.
"""
from outbox_poc.db import connect, init_schema
from outbox_poc.service import TransferService
from outbox_poc.relay import OutboxRelay, FakeBroker
from outbox_poc.consumer import IdempotentConsumer


def main():
    conn = connect()
    init_schema(conn)
    svc = TransferService(conn)

    print("1. Client sends POST /transfers with Idempotency-Key=key-1")
    result = svc.create_operation("client-1", "key-1", amount_minor=150050, currency="RUB")
    print(f"   -> operation {result.operation_id} created, state={result.state}")

    print("\n2. Client retries the same request (network hiccup)")
    replay = svc.create_operation("client-1", "key-1", amount_minor=150050, currency="RUB")
    print(f"   -> replayed={replay.replayed}, same operation_id={replay.operation_id == result.operation_id}")

    op = result.operation_id
    print("\n3. ABS confirms the hold")
    svc.transition(op, "FUNDS_RESERVED", "hold_confirmed")
    print("   -> state = FUNDS_RESERVED")

    print("\n4. Intent stored, then sent to the payment network")
    svc.transition(op, "SUBMITTED", "submitted_to_network")
    print("   -> state = SUBMITTED")

    print("\n5. Network confirms success -> capture -> terminal state + outbox event")
    svc.transition(op, "COMPLETED", "network_success", publish_event_type="TransferCompleted")
    print("   -> state = COMPLETED, one outbox_event row written in the same transaction")

    print("\n6. Outbox relay polls and publishes to the broker")
    broker = FakeBroker()
    published = OutboxRelay(conn, broker).poll_and_publish()
    print(f"   -> published {published} event(s): {broker.messages}")

    print("\n7. Two independent consumers process the event; broker redelivers once")
    notifier = IdempotentConsumer(conn, "notification-service")
    analytics = IdempotentConsumer(conn, "analytics-service")
    _, _, payload = broker.messages[0]

    print(f"   notifier first delivery  -> effect applied: {notifier.handle(payload)}")
    print(f"   notifier REdelivery      -> effect applied: {notifier.handle(payload)}  (must be False)")
    print(f"   analytics first delivery -> effect applied: {analytics.handle(payload)}")

    print(f"\nFinal: notifier processed {len(notifier.effects)} effect(s), "
          f"analytics processed {len(analytics.effects)} effect(s).")


if __name__ == "__main__":
    main()
