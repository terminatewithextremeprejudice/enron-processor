package parser

import (
	"context"
	"fmt"
	"log/slog"

	enronv1 "github.com/terminatewithextremeprejudice/enron-processor/gen/enron/v1"
)

// Persister is the data-store hand-off seam. The parser's job ends where
// persistence begins: it hands a fully-parsed ParsedMessage to a Persister
// and is done once the Persister acks (or fails).
//
// Persister implementations:
//   - pgPersister (production): writes the message straight into PostgreSQL
//     — person + message + message_recipient + envelope_receipt in one
//     idempotent transaction.
//   - dummyPersister (dev only): accept-and-discard, so the consume → parse →
//     route loop can be exercised without a database.
//
// The interface is deliberately tiny: swapping the persistence target is a
// body-local change behind NewPersister — nothing in the consumer, parse, or
// DLQ code changes.
type Persister interface {
	// Persist durably records one parsed message (idempotent by key).
	Persist(ctx context.Context, msg *enronv1.ParsedMessage) error
	// DeadLetter records an envelope that failed to parse (idempotent by key).
	DeadLetter(ctx context.Context, env *enronv1.RawEnvelope, errStr string) error
	// Close releases the Persister's resources.
	Close() error
}

// NewPersister builds the configured persister. Production is "postgres" —
// the parser writes directly, there is no intermediate worker service. The
// "dummy" target stays for development without a database.
func NewPersister(cfg Config, log *slog.Logger) (Persister, error) {
	switch cfg.Persister {
	case SinkPostgres, "":
		if cfg.DBDSN == "" {
			return nil, fmt.Errorf("persister=postgres requires db_dsn (or set DB_DSN)")
		}
		return newPostgresPersister(cfg.DBDSN, cfg.RawTopic, log)
	case SinkDummy:
		log.Info("persister=dummy (accept-and-discard; no Postgres writes)")
		return &dummyPersister{log: log}, nil
	default:
		return nil, fmt.Errorf("unknown persister %q (want %q or %q)", cfg.Persister, SinkPostgres, SinkDummy)
	}
}

// dummyPersister is the accept-and-discard stand-in for a database-less dev
// run. It is intentionally lossless-with-respect-to-the-state-machine: every
// Persist succeeds, so the consumer's at-least-once contract holds and the
// loop runs end to end without Postgres.
type dummyPersister struct{ log *slog.Logger }

func (d *dummyPersister) Persist(ctx context.Context, msg *enronv1.ParsedMessage) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	d.log.Debug("persisted (dummy sink)", "idempotency_key", msg.GetIdempotencyKey(),
		"sender", msg.GetSenderEmail(), "subject", msg.GetSubject())
	return nil
}

func (d *dummyPersister) DeadLetter(ctx context.Context, env *enronv1.RawEnvelope, errStr string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	d.log.Debug("dead letter (dummy sink)", "idempotency_key", env.GetIdempotencyKey(), "err", errStr)
	return nil
}

func (d *dummyPersister) Close() error { return nil }
