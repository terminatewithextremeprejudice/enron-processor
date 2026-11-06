// Persistence leg: the parser writes ParsedMessages directly into
// PostgreSQL. There is no intermediate worker service — this package owns
// the only write path into the database, which keeps the at-least-once
// Kafka contract and the effective-once database result coupled in one
// place (the consumer marks the offset only after Persist acks).
package parser

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // pgx driver on the database/sql API

	enronv1 "github.com/terminatewithextremeprejudice/enron-processor/gen/enron/v1"
)

// pgPersister is the production Persister: one Postgres transaction per
// parsed message — person (sender + recipients), message, message_recipient,
// plus the raw-stage envelope_receipt. Idempotent on
// message.idempotency_key (ON CONFLICT DO NOTHING), so Kafka redelivery and
// DLQ replay converge on an exactly-once row set.
type pgPersister struct {
	db    *sql.DB
	log   *slog.Logger
	topic string
}

// newPostgresPersister opens the pool and verifies reachability with a
// bounded ping — sql.Open is lazy, so without the ping a bad DSN would fail
// on the first message instead of at boot.
func newPostgresPersister(dsn, topic string, log *slog.Logger) (*pgPersister, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(30 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	log.Info("postgres persister ready", "topic", topic)
	return &pgPersister{db: db, log: log, topic: topic}, nil
}

func (p *pgPersister) Persist(ctx context.Context, msg *enronv1.ParsedMessage) error {
	if err := p.write(ctx, msg); err != nil {
		return fmt.Errorf("persist %q: %w", msg.GetIdempotencyKey(), err)
	}
	return nil
}

func (p *pgPersister) Close() error { return p.db.Close() }

// write commits one parsed message and everything it implies. A single
// transaction guarantees the person/message/recipient rows appear together —
// a message row can never exist without its recipient bridge.
func (p *pgPersister) write(ctx context.Context, msg *enronv1.ParsedMessage) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }() // no-op after Commit

	senderID, err := upsertPerson(ctx, tx, msg.GetSenderName(), msg.GetSenderEmail())
	if err != nil {
		return err
	}

	var msgID int64
	err = tx.QueryRowContext(ctx, insertMessageSQL,
		msg.GetIdempotencyKey(),
		msg.GetSourcePath(),
		msg.GetMessageId(),
		msg.GetSubject(),
		senderID,
		nullTime(msg.GetSentAtUnix()),
	).Scan(&msgID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// Key already committed by an earlier attempt (Kafka redelivery
			// or DLQ replay). That attempt wrote the recipients in the same
			// transaction, so only the raw-stage receipt still needs updating.
			if err := p.receipt(ctx, tx, msg); err != nil {
				return err
			}
			if err := tx.Commit(); err != nil {
				return fmt.Errorf("commit (duplicate): %w", err)
			}
			p.log.Debug("duplicate idempotency key; row already present",
				"idempotency_key", msg.GetIdempotencyKey())
			return nil
		}
		return fmt.Errorf("insert message: %w", err)
	}

	recipients := dedupeRecipients(msg)
	for j, r := range recipients {
		personID, err := upsertPerson(ctx, tx, r.name, r.email)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, insertRecipientSQL, msgID, personID, r.typ); err != nil {
			return fmt.Errorf("insert recipient %d (%q): %w", j, r.email, err)
		}
	}

	if err := p.receipt(ctx, tx, msg); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	p.log.Debug("persisted",
		"idempotency_key", msg.GetIdempotencyKey(),
		"message_id", msgID,
		"recipients", len(recipients))
	return nil
}

// receipt records the raw-stage observation (envelope_receipt): the parser is
// the component that saw this envelope key on the wire. first_seen stays
// pinned from initial insert by design; last_seen/last_component track the
// most recent sighting across redeliveries.
func (p *pgPersister) receipt(ctx context.Context, tx *sql.Tx, msg *enronv1.ParsedMessage) error {
	if _, err := tx.ExecContext(ctx, upsertReceiptSQL,
		msg.GetIdempotencyKey(), msg.GetSourcePath(), p.topic); err != nil {
		return fmt.Errorf("envelope receipt: %w", err)
	}
	return nil
}

// upsertPerson resolves (or creates) the person row for one email identity.
// Identity is the lower-cased address (unique); the display name is
// best-effort and only back-filled when the stored name is empty.
func upsertPerson(ctx context.Context, tx *sql.Tx, name, email string) (int64, error) {
	email = normEmail(email)
	if email == "" {
		return 0, errors.New("empty email address")
	}
	var id int64
	if err := tx.QueryRowContext(ctx, upsertPersonSQL, strings.TrimSpace(name), email).Scan(&id); err != nil {
		return 0, fmt.Errorf("upsert person %q: %w", email, err)
	}
	return id, nil
}

// normEmail is the identity normalization: the schema's identity convention
// is a lower-cased, trimmed address.
func normEmail(e string) string {
	return strings.ToLower(strings.TrimSpace(e))
}

// recipientRow is one deduped (message, person) bridge candidate.
type recipientRow struct {
	name  string
	email string
	typ   string // to | cc | bcc
}

// dedupeRecipients flattens To/Cc/Bcc into one list with a single row per
// address. Per the schema contract an address appearing in several headers of
// the same mail is stored once, with 'to' winning over 'cc' and 'cc' over
// 'bcc'. First-seen order (to → cc → bcc, header order) is preserved.
func dedupeRecipients(msg *enronv1.ParsedMessage) []recipientRow {
	rank := map[string]int{"to": 0, "cc": 1, "bcc": 2}
	seen := make(map[string]int)
	var out []recipientRow

	add := func(typ string, list []*enronv1.Recipient) {
		for _, r := range list {
			email := normEmail(r.GetEmail())
			if email == "" {
				continue
			}
			if i, ok := seen[email]; ok {
				if rank[typ] < rank[out[i].typ] { // lower rank (to > cc > bcc) wins
					out[i].typ = typ
				}
				continue
			}
			seen[email] = len(out)
			out = append(out, recipientRow{name: strings.TrimSpace(r.GetName()), email: email, typ: typ})
		}
	}
	add("to", msg.GetTo())
	add("cc", msg.GetCc())
	add("bcc", msg.GetBcc())
	return out
}

// nullTime maps the proto's unix-seconds convention (0 = absent) onto a
// nullable timestamptz column.
func nullTime(v int64) sql.NullTime {
	if v <= 0 {
		return sql.NullTime{}
	}
	return sql.NullTime{Time: time.Unix(v, 0).UTC(), Valid: true}
}

// ---------------------------------------------------------------------------
// SQL — statements against the 0001_init schema.
//
// Bodies are persisted as '' for now (see README: status/top-senders are the
// live metrics; body capture is a follow-up).
// ---------------------------------------------------------------------------

const upsertPersonSQL = `
INSERT INTO person (name, email)
VALUES ($1, $2)
ON CONFLICT (email) DO UPDATE
SET name = CASE
         WHEN person.name = '' AND EXCLUDED.name <> '' THEN EXCLUDED.name
         ELSE person.name
       END
RETURNING id`

const insertMessageSQL = `
INSERT INTO message (idempotency_key, source_path, message_id, subject,
                     body_text, body_html, sender_id, sent_at)
VALUES ($1, $2, $3, $4, '', '', $5, $6)
ON CONFLICT (idempotency_key) DO NOTHING
RETURNING id`

const insertRecipientSQL = `
INSERT INTO message_recipient (message_id, person_id, recipient_type)
VALUES ($1, $2, $3)
ON CONFLICT (message_id, person_id) DO NOTHING`

const upsertReceiptSQL = `
INSERT INTO envelope_receipt (envelope_key, source_path, topic, last_component)
VALUES ($1, $2, $3, 'parser')
ON CONFLICT (envelope_key) DO UPDATE
SET last_seen = now(),
    last_component = EXCLUDED.last_component`
