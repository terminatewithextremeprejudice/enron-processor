# enron-processor

Go microservices that ingest the **Enron Email 2015** dataset
(~500k mails across 151 maildirs) through **Kafka 4.0 (KRaft)**, persist
parsed messages into **PostgreSQL**, and expose metrics over HTTP.

Kafka client: **sarama**. Cross-service payloads are **protobuf**.
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
 POST /start ---------->| API    |-------------------------------+
 GET /status <-----------> :8080 |                               v
 GET /top-senders <------+        |                         PostgreSQL
                         +--------+                  (person, message,
                               ^                              message_recipient,
                               |                              envelope_receipt)
+-----------+ RawEnvelope  +---v------------------+
| producer  |---enron.raw-->| parser               |--- one Postgres tx per
| maildir ->| (proto bytes) | RFC-822 -> ParsedMessage |    message (idempotent)
+-----------+               +----------+-----------+
                                   |  ^
          produce/parse/write fail |  | DLQ replay (manual)
                                   v  |   (DeadLetter proto: original envelope + error)
                                 enron.raw.dlq
```

Data flow:
1. **producer** walks the maildir and emits the *unmodified raw bytes*
   (`RawEnvelope`) onto `enron.raw`. Nothing is parsed here — the producer
   is a safe, dumb ferry.
2. **parser** consumes `enron.raw`, parses RFC-822 mail into a
   `ParsedMessage`, and writes it **directly into PostgreSQL** in a single
   transaction: upsert the sender `person`, insert the `message`
   (`ON CONFLICT (idempotency_key) DO NOTHING`), upsert each recipient
   `person` + `message_recipient` bridge row, and stamp the raw-stage
   `envelope_receipt`. Message bodies are stored as `''` for now.
3. Kafka offsets are committed only after the Postgres commit → **at-least-once**.
4. The parser's writes are **idempotent** (unique `idempotency_key`,
   conflict-does-nothing upserts) → effective-once results in the database.
5. Anything unparseable (or whose write failed) is routed to `enron.raw.dlq`
   wrapped in a `DeadLetter` carrying the original envelope + error —
   lossless, inspectable, replayable.

## Idempotency design

Idempotency key = `sha256(dataset-relative mail path)` — deterministic across
producer restarts, re-runs, and DLQ replays. It travels verbatim from
`RawEnvelope` through `ParsedMessage` into the DB.

| Stage      | Mechanism                                                                 |
|------------|---------------------------------------------------------------------------|
| Raw (Kafka) | `envelope_receipt` table: envelope keys seen on the wire, first/last sighting (stamped by the parser) |
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
make app              # build the 3 Go services + start everything

# Host-side endpoints once the app profile is up:
curl -X POST localhost:8080/start
curl localhost:8080/status
curl localhost:8080/top-senders
```

Service endpoints (localhost): API `8080`, Postgres `5432`, Kafka EXTERNAL
listener `9093` (in-network services use `kafka:9092` via the PLAINTEXT listener).

## Results 

### /status
```
{
  "finished": true,
  "total_messages": 517374
}
```

### /top-senders
```
| Rank | Email                         | Messages |
|------|-------------------------------|----------|
| 1    | kay.mann@enron.com            | 16,735   |
| 2    | vince.kaminski@enron.com      | 14,368   |
| 3    | jeff.dasovich@enron.com       | 11,411   |
| 4    | pete.davis@enron.com          | 9,149    |
| 5    | chris.germany@enron.com       | 8,801    |
| 6    | sara.shackleton@enron.com     | 8,777    |
| 7    | enron.announcements@enron.com | 8,587    |
| 8    | tana.jones@enron.com          | 8,490    |
| 9    | steven.kean@enron.com         | 6,759    |
| 10   | kate.symes@enron.com          | 5,438    |

```
