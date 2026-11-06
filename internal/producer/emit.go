package producer

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"

	"github.com/IBM/sarama"
	enronv1 "github.com/terminatewithextremeprejudice/enron-processor/gen/enron/v1"
	"google.golang.org/protobuf/proto"
)

// EmitFunc receives one RawEnvelope per mail file during the walk. It is the
// seam the walk (walk.go) drives: every discovered file yields exactly one
// envelope, and the sink's decision (accept / reject) becomes the walk's
// emitted / failed counter.
type EmitFunc func(ctx context.Context, env *enronv1.RawEnvelope) error

// Sink is the destination a RawEnvelope is written to. The production
// implementation is a sarama sync producer (kafkaSink); the dummySink
// accepts and discards envelopes so the fetch → walk → emit state machine can
// be exercised — in production dry runs and the e2e test — with no broker.
type Sink interface {
	// Send writes one envelope and reports an error on failure. Implementations
	// must honor ctx for cancellation and must be safe for concurrent use
	// (the walk is single-pass today, but the final multi-walker leg is not).
	Send(ctx context.Context, env *enronv1.RawEnvelope) error
	// Close releases the sink's resources.
	Close() error
}

// newSink builds the configured sink. The kafka sink dials the broker on
// construction and can therefore fail (no servers / unreachable); the dummy
// sink never does.
func newSink(cfg Config, log *slog.Logger) (Sink, error) {
	switch cfg.Sink {
	case SinkDummy:
		log.Info("emit sink=dummy (envelopes accepted and discarded; no Kafka write)")
		return &dummySink{}, nil
	case SinkKafka:
		k, err := newKafkaSink(cfg, log)
		if err != nil {
			return nil, fmt.Errorf("kafka sink: %w", err)
		}
		return k, nil
	default:
		return nil, fmt.Errorf("unknown emit sink %q (want %q or %q)", cfg.Sink, SinkKafka, SinkDummy)
	}
}

// emit is the walk's envelope sink. It enforces the invariants every sink
// inherits (a non-nil envelope carrying an idempotency key — an envelope
// without one would corrupt downstream dedupe, since the
// message.idempotency_key UNIQUE constraint IS the pipeline's dedupe), then
// hands the envelope to the configured sink. The signature is the seam; the
// walk, the counters, and the Start/Status surface all stay unchanged.
func (s *Server) emit(ctx context.Context, env *enronv1.RawEnvelope) error {
	if env == nil {
		return fmt.Errorf("emit: nil envelope")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if env.IdempotencyKey == "" {
		return fmt.Errorf("emit: envelope for %q carries no idempotency key", env.SourcePath)
	}
	return s.sink.Send(ctx, env)
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

// dummySink accepts every envelope and drops it. It exists so the
// fetch → walk → emit state machine (and the e2e test) can run end to end
// with no Kafka broker required. It is deliberately lossless-with-respect-to-
// the state machine: every Send succeeds, so emitted always equals total.
type dummySink struct{}

func (d *dummySink) Send(ctx context.Context, _ *enronv1.RawEnvelope) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return nil
}

func (d *dummySink) Close() error { return nil }

// kafkaSink is the production sink: a sarama sync producer that serializes
// each envelope to protobuf and writes it to cfg.RawTopic. It runs with
// acks per cfg (all by default) and the configured retry budget, matching the
// README's "acks: all + explicit retries" posture for the raw leg. The
// idempotency key doubles as the partition key, so every copy of one logical
// mail lands on the same partition and its replay order is preserved.
type kafkaSink struct {
	topic string
	sp    sarama.SyncProducer
}

func newKafkaSink(cfg Config, log *slog.Logger) (*kafkaSink, error) {
	if len(cfg.BootstrapServers) == 0 {
		return nil, fmt.Errorf("kafka sink requires at least one bootstrap_server")
	}

	sc := sarama.NewConfig()
	sc.ClientID = "enron-producer"
	sc.Producer.RequiredAcks = acksFor(cfg.Acks)
	sc.Producer.Return.Successes = cfg.ReturnSuccesses
	sc.Producer.Retry.Max = cfg.Retries
	if cfg.MaxMessageBytes > 0 {
		sc.Producer.MaxMessageBytes = cfg.MaxMessageBytes
	}

	sp, err := sarama.NewSyncProducer(cfg.BootstrapServers, sc)
	if err != nil {
		return nil, fmt.Errorf("sarama.NewSyncProducer(%v): %w", cfg.BootstrapServers, err)
	}
	log.Info("kafka sink ready",
		"servers", cfg.BootstrapServers,
		"topic", cfg.RawTopic,
		"acks", cfg.Acks,
		"retries", sc.Producer.Retry.Max)
	return &kafkaSink{topic: cfg.RawTopic, sp: sp}, nil
}

// Send marshals the envelope and writes it to the raw topic synchronously.
func (k *kafkaSink) Send(ctx context.Context, env *enronv1.RawEnvelope) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	body, err := proto.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal envelope %q: %w", env.SourcePath, err)
	}
	msg := &sarama.ProducerMessage{
		Topic: k.topic,
		Key:   sarama.StringEncoder(env.IdempotencyKey),
		Value: sarama.ByteEncoder(body),
	}
	partition, offset, err := k.sp.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("send %q: %w", env.SourcePath, err)
	}
	// Offset/partition are the broker's receipt for this envelope; logged at
	// debug so a replay can be traced without spamming the happy path.
	_ = partition
	_ = offset
	return nil
}

func (k *kafkaSink) Close() error { return k.sp.Close() }

// acksFor maps the config's string form onto sarama's acknowledgment level.
// "all" (the committed default) waits for every ISR replica; "leader" waits
// only for the partition leader; anything unrecognized falls back to the
// safest level rather than the lossiest one.
func acksFor(acks string) sarama.RequiredAcks {
	switch acks {
	case "", "all":
		return sarama.WaitForAll
	case "leader":
		return sarama.WaitForLocal
	default:
		return sarama.WaitForAll
	}
}
