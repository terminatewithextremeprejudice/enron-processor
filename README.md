# enron-processor

Cloud-native Go microservices that ingest the **Enron Email 2015** dataset
(~500k mails across 151 maildirs) through **Kafka 4.0 (KRaft)**, persist
parsed messages into **PostgreSQL**, and expose metrics over HTTP.

Kafka client: **sarama**. All cross-service payloads are **protobuf**.
Failures land on a **dead-letter topic**, and the whole pipeline is
**idempotent** — redelivery and replay are safe by construction.

---

## Required HTTP endpoints

| Method | Path           | Meaning                                                        |
|--------|----------------|----------------------------------------------------------------|
| POST   | `/start`       | Start ingestion (idempotent — re-calling is safe)              |
| GET    | `/status`      | `{finished: bool, total_messages: int}`                        |
| GET    | `/top-senders` | Top 10 senders by message count                                 |

## Architecture

```
                         +--------+
 POST /start ------------> API    |-------------------------------+
 GET /status <------------| :8080 |                               v
 GET /top-senders <-------+        |                        PostgreSQL
                              +----+------------------- (person, message,
                              |                              message_recipient,
                              v                              envelope_receipt)
+-----------+ RawEnvelope  +---v------------------+   ParsedMessage    +---------+
| producer  |---enron.raw-->| parser               |------------------->| worker  |
| maildir ->| (proto bytes) | RFC-822 -> proto     |   gRPC DbWorker    | Postgres|
+-----------+               +----------+-----------+   WriteMessage     +---------+
      |                          |  ^
      | produce/parse/write fail |  | DLQ replay (manual)
      v                          |  |
        enron.raw.dlq  <---------+--+   (DeadLetter proto: original envelope + error)
```

Data flow:
1. **producer** walks the maildir and emits the *unmodified raw bytes*
   (`RawEnvelope`) onto `enron.raw`. Nothing is parsed here — the producer
   is a safe, dumb ferry.
2. **parser** consumes `enron.raw`, parses RFC-822 mail into a
   `ParsedMessage`, and calls the worker **synchronously over gRPC**.
3. **worker** (the "second worker", no Kafka on this leg by design) upserts
   the person + message + recipient rows into PostgreSQL.
4. Kafka offsets are committed only after the worker acks → **at-least-once**.
5. The worker's writes are **idempotent** (unique `idempotency_key`) →
   effective-once results in the database.
6. Anything unparseable (or whose write failed) is routed to `enron.raw.dlq`
   wrapped in a `DeadLetter` carrying the original envelope + error —
   lossless, inspectable, replayable.

## Idempotency design

Idempotency key = `sha256(dataset-relative mail path)` — deterministic across
producer restarts, re-runs, and DLQ replays. It travels verbatim from
`RawEnvelope` through `ParsedMessage` into the DB.

| Stage      | Mechanism                                                                 |
|------------|---------------------------------------------------------------------------|
| Raw (Kafka) | `envelope_receipt` table: envelope keys seen on the wire, first/last sighting |
| Parsed (DB) | `message.idempotency_key UNIQUE` + `ON CONFLICT DO NOTHING` semantics    |
| Producer   | `acks: all` + explicit retries (sarama)                                   |

## Schema (db/migrations/0001_init.up.sql)

| Table               | Role                                                                            |
|---------------------|---------------------------------------------------------------------------------|
| `person`            | Distinct identities; **identity = email address** (unique), name best-effort   |
| `message`           | Parsed mails; sender is a **one-to-many** FK onto `person`; carries the idem key |
| `message_recipient` | **Many-to-many** bridge message ↔ person, with `to/cc/bcc` provenance           |
| `envelope_receipt`  | Raw-stage idempotency/audit across the wire                                     |

## Local quickstart

```bash
# Prereqs: docker + compose on this machine.

make infra            # Kafka 4.0 (KRaft) + Postgres 16, schema auto-migrated
make fetch-dataset    # download + unpack Enron into ./data (idempotent)
make app              # build the 4 Go services + start everything (phase 2)

# Host-side endpoints once phase 2 is in:
curl -X POST localhost:8080/start
curl localhost:8080/status
curl localhost:8080/top-senders
```

Service endpoints (localhost): API `8080`, Postgres `5432`, Kafka EXTERNAL
listener `9093` (in-network services use `kafka:9092` via the PLAINTEXT listener).

