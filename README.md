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
                        +---------+
 POST /start ---------->| API     |--- trigger walk ----->+-----------+
 GET /status <----------+ :8080   |                      | producer  |
 GET /top-senders <-----+         |       +------------->| maildir ->|--- RawEnvelope
                        +---------+       |             | (proto)    |---+
                                   ^      |             +-----------+   |
                         query DB |      |                     |       |
                                   |      |                   Raw       |
                             +----v----   |             +---------------   v
                            | PostgreSQL  |             | Kafka          |
                            | (message,   |             | enron.raw------->|
                            |  sender     |     +-------+------------------>
                            |  recipient) |     |                   |       |
                            +------------<-----<           |       |
                                                           |       |
                                                   +--------+---+   |
                                                   | DLQ Replay  |<--+
                                                   +--------------+
```

**Data flow:**

1. **API** triggers producer via `POST /start`, queries DB for status/top-senders
2. **Producer** walks maildir, emits `RawEnvelope` protos to `enron.raw` topic
3. **Parser** (consumer group, xN instances) consumes `enron.raw`, parses RFC-822,
   writes to PostgreSQL in an idempotent transaction
4. **Dead-letter topic** (`enron.raw.dlq`) captures unparseable envelopes + errors

## Idempotency design

Idempotency key = `sha256(dataset-relative mail path)` — deterministic across
producer restarts, re-runs, and DLQ replays. It travels verbatim from
`RawEnvelope` through `ParsedMessage` into the DB.

| Stage      | Mechanism                                                                 |
|------------|---------------------------------------------------------------------------|
| Raw (Kafka) | `envelope_receipt` table: envelope keys seen on the wire, first/last sighting (stamped by the parser) |
| Parsed (DB) | `message.idempotency_key UNIQUE` + `ON CONFLICT DO NOTHING` semantics    |
| Producer   | `acks: all` + explicit retries (sarama)                                   |

## Multi-instance parser support

The parser uses **Kafka consumer groups** to support running multiple instances
in parallel. All instances with the same `consumer_group` name share partitions
across the group via Kafka's built-in coordination:

- **Partition assignment**: Kafka's Range strategy distributes consecutive
  partitions to each consumer in the group
- **Rebalance on join/leave**: When a parser instance starts or stops, Kafka
  redistributes partitions among remaining members
- **At-least-once delivery**: Offsets are only committed after successful
  persistence (or DLQ routing), ensuring no message is dropped
- **Idempotent writes**: Database constraints deduplicate messages if the same
  envelope is processed multiple times during rebalances

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
