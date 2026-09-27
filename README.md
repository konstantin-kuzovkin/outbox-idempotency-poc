# Outbox + Idempotent Consumer — Proof of Concept

A small, runnable implementation of the pattern described in the portfolio:
`02-event-driven-architecture/outbox.md`, `ADR-005`, and the idempotency
rules in `01-payment-transfer-architecture/idempotency.md`.

Zero external dependencies: standard library only (`sqlite3`, `unittest`).
SQLite stands in for PostgreSQL; a real broker (Kafka) is stood in for by
`FakeBroker`, an in-memory queue. The SQL and the transaction pattern are
what matters and translate to PostgreSQL almost unchanged — see "Porting to
a real stack" below.

## Run it

```bash
python3 demo.py          # narrated end-to-end walkthrough
python3 -m unittest discover -s tests -v    # 11 tests
```

## What is actually proven here (not just described)

| Claim from the portfolio | Test |
|---|---|
| A retried request with the same Idempotency-Key never creates a second operation | `test_same_key_same_body_returns_same_operation` |
| A reused key with a different body is rejected, not silently accepted | `test_same_key_different_body_is_rejected` |
| Two workers racing on the same operation: the stale one is rejected, not overwritten | `test_concurrent_transition_only_one_wins` |
| An invalid transition (e.g. `NEW -> COMPLETED`) is rejected | `test_invalid_transition_is_rejected` |
| A terminal state cannot be changed again | `test_terminal_state_cannot_be_changed` |
| The outbox event exists immediately after commit, even if nothing has published it yet (simulated crash) | `test_event_is_written_atomically_with_the_state_change` |
| The relay eventually publishes it, and does not republish it again | `test_relay_eventually_publishes_after_the_crash` |
| A `FAILED` operation publishes `TransferFailed`, not `TransferCompleted` | `test_failed_operation_does_not_publish_completed_event` |
| A redelivered event (at-least-once) is applied exactly once | `test_duplicate_delivery_applies_the_effect_once` |
| Deduplication is per consumer group, so two different consumers both get to process the event | `test_two_different_consumers_each_process_the_event_once` |

## What this PoC deliberately does not include

- No real Kafka, no real PostgreSQL, no network. Adding them is infrastructure work, not proof of the pattern.
- No HTTP layer / API. `TransferService` is called directly.
- No retry/backoff/DLQ implementation for the relay or the consumer (described in outbox.md, not re-implemented here).
- No load or concurrency testing with real threads/processes — the "concurrent update" test simulates the race directly at the SQL level rather than spinning up real concurrent workers.

## Porting to a real stack

| Here | Real stack |
|---|---|
| `sqlite3` | `psycopg` / `asyncpg` against PostgreSQL. The `CREATE TABLE` statements in `db.py` are close to valid Postgres already (swap `AUTOINCREMENT` for `GENERATED ALWAYS AS IDENTITY`, `TEXT` timestamps for `timestamptz`). |
| `FakeBroker` | `kafka-python` / `confluent-kafka` producer, keyed by `aggregate_id`. |
| `OutboxRelay.poll_and_publish()` called manually | A scheduled job (cron, Kubernetes CronJob) or a long-running loop with a sleep interval, per `outbox.md`. |
| `IdempotentConsumer.handle()` called manually | A real Kafka consumer loop, committing the offset only after the transaction above commits. |

## Files

```
outbox_poc/
  db.py        schema
  service.py   TransferService: idempotent create, transitions, optimistic locking, outbox write
  relay.py     OutboxRelay + FakeBroker
  consumer.py  IdempotentConsumer
tests/
  test_outbox.py   11 tests, no external test framework needed
demo.py        narrated walkthrough
```
