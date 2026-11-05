package parser

import (
	"context"
	"log/slog"
	"fmt"

	enronv1 "github.com/terminatewithextremeprejudice/enron-processor/gen/enron/v1"
)

// Persister is the data-store hand-off seam. The parser's job ends where
// persistence begins: it hands a fully-parsed ParsedMessage to a Persister
// and is done once the Persister acks (or fails).
//
// Persister implementations:
//   - dummyPersister (this revision): accepts and discards — a stand-in so the
//     consume → parse → route loop is complete and testable before worker gRPC.
//   - workerPersister (later): dials enron.v1.DbWorker and calls WriteMessage.
//
// The interface is deliberately tiny so the real leg is a body-local swap
// behind NewPersister — nothing in the consumer, parse, or DLQ code changes.
type Persister interface {
	// Persist durably records one parsed message (idempotent by key).
	Persist(ctx context.Context, msg *enronv1.ParsedMessage) error
	// Close releases the Persister's resources.
	Close() error
}

// NewPersister builds the configured persister. This revision only wires the
// dummy for the real one later (worker Persister requires the worker service
// on cfg.WorkerGRPCAddr — intentionally not built yet; see the worker gRPC
// leg in proto/enron/v1/parse.proto).
func NewPersister(cfg Config, log *slog.Logger) (Persister, error) {
	switch cfg.Persister {
	case SinkDummy, "":
		log.Info("persister=dummy (accepted-and-discarded; worker gRPC leg lands later)")

		return &dummyPersister{log: log}, nil
	case SinkWorker:
		return nil, fmt.Errorf("persister=%q is not implemented yet (worker leg is the next revision)", SinkWorker)
	default:
		return nil, fmt.Errorf("unknown persister %q (want %q or %q)", cfg.Persister, SinkDummy, SinkWorker)
	}
}

// dummyPersister is the accept-and-discard stand-in for the worker leg. It
// is intentionally lossless-with-respect-to-the-state-machine: every Persist
// succeeds, so the consumer's at-least-once contract holds and the loop runs
// end to end without a database.
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

func (d *dummyPersister) Close() error { return nil }

