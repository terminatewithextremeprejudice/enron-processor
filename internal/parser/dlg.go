package parser

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/IBM/sarama"
	enronv1 "github.com/terminatewithextremeprejudice/enron-processor/gen/enron/v1"
	"google.golang.org/protobuf/proto"
)

// dlqProducer persists unparseable / failed envelopes onto the DeadLetter
// topic. It is REAL (a sarama sync producer), not a stub: the dead-letter
// path is what makes the pipeline safe to run end to end, and it is the one
// leg that must never silently swallow a failure.
type dlqProducer struct {
	topic string
	sp    sarama.SyncProducer
}

func newDLQProducer(cfg Config, log *slog.Logger) (*dlqProducer, error) {
	if len(cfg.BootstrapServers) == 0 {
		return nil, fmt.Errorf("dlq producer requires bootstrap_servers")
	}
	sc := sarama.NewConfig()
	sc.ClientID = "enron-parser-dlq"
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
	log.Info("dlq producer ready", "servers", cfg.BootstrapServers, "topic", cfg.DLQTopic, "acks", cfg.Acks, "retries", sc.Producer.Retry.Max)
	return &dlqProducer{topic: cfg.DLQTopic, sp: sp}, nil
}

// send writes one DeadLetter onto the DLQ topic (acks per cfg).
func (d *dlqProducer) send(ctx context.Context, dl *enronv1.DeadLetter) error {
	_ = ctx // sarama sync producer manages its own timeouts; we honor ctx at the call sites
	body, err := proto.Marshal(dl)
	if err != nil {
		return fmt.Errorf("marshal dead letter: %w", err)
	}
	msg := &sarama.ProducerMessage{
		Topic: d.topic,
		Key:   sarama.StringEncoder(dl.IdempotencyKey),
		Value: sarama.ByteEncoder(body),
	}
	if _, _, err := d.sp.SendMessage(msg); err != nil {
		return fmt.Errorf("dlq write: %w", err)
	}
	return nil
}

func (d *dlqProducer) Close() error { return d.sp.Close() }

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

func buildDeadLetter(env *enronv1.RawEnvelope, topic string, partition int32, offset int64, group, component string, cause error) *enronv1.DeadLetter {
	return &enronv1.DeadLetter{
		IdempotencyKey:  env.IdempotencyKey,
		FailedTopic:     topic,
		FailedPartition: partition,
		FailedOffset:    offset,
		ConsumerGroup:   group,
		FailedComponent: component,
		Error:           cause.Error(),
		FailedAtMs:      time.Now().UnixMilli(),
		Original:        env,
	}
}
