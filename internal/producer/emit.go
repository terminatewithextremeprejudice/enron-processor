package producer

import (
	"context"
	"fmt"
	"os"
	"strconv"

	enronv1 "github.com/terminatewithextremeprejudice/enron-processor/gen/enron/v1"
)

// EmitFunc receives one RawEnvelope per mail file during the walk. The
// production implementation is a sarama sync producer that serializes the
// envelope and writes it to cfg.RawTopic (acks=all, cfg.Retries); until
// that leg lands the walk drives this seam with the stub below, so the
// fetch → walk → emit pipeline is end to end before the broker exists.
type EmitFunc func(ctx context.Context, env *enronv1.RawEnvelope) error

// emit is the walk's envelope sink.
//
// STUB REVISION: it accepts each mail's raw data, enforces the invariants
// the real producer will inherit, and discards the envelope. Swapping in
// the sarama leg is a body-local change here — the signature is the seam,
// and the walk, the counters, and the Start/Status surface all stay
// unchanged.
func (s *Server) emit(ctx context.Context, env *enronv1.RawEnvelope) error {
	if env == nil {
		return fmt.Errorf("emit: nil envelope")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	// An envelope without a key would corrupt downstream dedupe (the
	// message.idempotency_key UNIQUE constraint IS the pipeline's dedupe),
	// so it must never flow. The walk guarantees one; this is cheap
	// insurance.
	if env.IdempotencyKey == "" {
		return fmt.Errorf("emit: envelope for %q carries no idempotency key", env.SourcePath)
	}
	s.log.Debug("envelope accepted (stub sink, no Kafka write yet)",
		"path", env.SourcePath,
		"bytes", len(env.RawBytes),
		"idempotency_key", env.IdempotencyKey)
	return nil
}

// producerID identifies this process in RawEnvelope.producer_id (DLQ
// forensics): "hostname/pid".
func producerID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return host + "/" + strconv.Itoa(os.Getpid())
}
