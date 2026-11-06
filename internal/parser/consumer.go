package parser

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/IBM/sarama"
	enronv1 "github.com/terminatewithextremeprejudice/enron-processor/gen/enron/v1"
	"google.golang.org/protobuf/proto"
)

// Server is the parser service: one Kafka consumer group that reads RawEnvelope
// protos from cfg.RawTopic, parses each, and either persists it (Persister) or
// dead-letters it (dlqProducer). New/Run/Close mirror the other services.
type Server struct {
	cfg       Config
	log       *slog.Logger

	group     sarama.ConsumerGroup
	persister Persister
	dlq       *dlqProducer

	mu sync.Mutex

	closing bool
}

// New wires the parser. It dials Kafka (consumer group + DLQ producer) up
// front — both can fail if the broker is unreachable — so a misconfigured
// bootstrap list fails fast at boot rather than on the first message.
func New(cfg Config, log *slog.Logger, persister Persister) (*Server, error) {
	if len(cfg.BootstrapServers) == 0 {
		return nil, fmt.Errorf("bootstrap_servers is required")
	}

	sc := newConsumerConfig(cfg)

	group, err := sarama.NewConsumerGroup(cfg.BootstrapServers, cfg.ConsumerGroup, sc)
	if err != nil {
		return nil, fmt.Errorf("consumer group: %w", err)
	}

	dlq, err := newDLQProducer(cfg, log)
	if err != nil {
		group.Close()
		return nil, fmt.Errorf("dlq producer: %w", err)
	}

	log.Info("parser ready",
		"servers", cfg.BootstrapServers,
		"topic", cfg.RawTopic,
		"dlq", cfg.DLQTopic,
		"group", cfg.ConsumerGroup,
		"persister", cfg.Persister)
	return &Server{cfg: cfg, log: log, group: group, persister: persister, dlq: dlq},
 nil
}

// newConsumerConfig builds the sarama config for the consume leg.
// Configures consumer group semantics: session timeouts, rebalance behavior,
// and offset management for multiple parser instances to share partitions.
func newConsumerConfig(cfg Config) *sarama.Config {
	sc := sarama.NewConfig()
	sc.Consumer.Return.Errors = false // errors flow through the message path; per-message failures we own

	// Generate a unique client ID for this parser instance using random bytes
	// This ensures distinct group membership across multiple container instances
	// (os.Getpid() returns 1 for all Docker containers, causing membership conflicts)
	b := make([]byte, 8)
	rand.Read(b)
	sc.ClientID = fmt.Sprintf("parser-%s", hex.EncodeToString(b))

	// Consumer group settings for multi-instance deployment
	// Use configured values or sensible defaults
	if cfg.SessionTimeoutMs > 0 {
		sc.Consumer.Group.Session.Timeout = time.Duration(cfg.SessionTimeoutMs) * time.Millisecond
	} else {
		sc.Consumer.Group.Session.Timeout = 45 * time.Second
	}

	if cfg.HeartbeatInterval > 0 {
		sc.Consumer.Group.Heartbeat.Interval = time.Duration(cfg.HeartbeatInterval) * time.Millisecond
	} else {
		sc.Consumer.Group.Heartbeat.Interval = 3 * time.Second
	}

	if cfg.RebalanceTimeoutMs > 0 {
		sc.Consumer.Group.Rebalance.Timeout = time.Duration(cfg.RebalanceTimeoutMs) * time.Millisecond
	} else {
		sc.Consumer.Group.Rebalance.Timeout = 60 * time.Second
	}

	// Auto-commit offsets every 10 seconds - ensures progress is synced to broker
	sc.Consumer.Offsets.AutoCommit.Enable = true
	sc.Consumer.Offsets.AutoCommit.Interval = 10 * time.Second

	// Initial offset position: start from earliest (beginning) for backfill/replay
	sc.Consumer.Offsets.Initial = sarama.OffsetOldest

	return sc
}

// Run enters the consumer group loop until ctx is cancelled.
//
// Note on at-least-once: sarama commits the session's offsets after
// ConsumeClaim has handled each message (see MarkMessage below). A message is
// "handled" iff it was persisted OR dead-lettered. A message we could not
// even dead-letter (the DLQ write itself failed) is not marked → it is
// redelivered, which is exactly the at-least-once contract the README calls
// for.
//
// Consumer group sessions end on rebalances - we must rejoin continuously.
func (s *Server) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			s.log.Info("context cancelled, stopping consumer")
			return ctx.Err()
		default:
		}

		s.log.Info("joining consumer group", "group", s.cfg.ConsumerGroup, "topic", s.cfg.RawTopic)
		if err := s.group.Consume(ctx, []string{s.cfg.RawTopic}, s); err != nil {
			s.log.Error("consumer", "err", err)
			return err
		}
		// Consume returns on session close/rebalance - loop to rejoin
		s.log.Info("consumer session ended, will rejoin on next iteration")
	}
}

// ---------------------------------------------------------------------------
// ConsumerGroupHandler
// ---------------------------------------------------------------------------

// Setup runs once per session, before any ConsumeClaim. Nothing to do here —
// the persister and dlq are built in New.
func (s *Server) Setup(sarama.ConsumerGroupSession) error { return nil }

// Cleanup runs once per session, after all ConsumeClaims and before the
// session's final offset commit. Nothing to do.
func (s *Server) Cleanup(sarama.ConsumerGroupSession) error { return nil }

// ConsumeClaim is the hot loop: for each raw envelope, parse it, then either
// persist (success) or dead-letter (parse / persist failure). Offsets are
// marked (i.e. committed) only after a message is durably handled — the
// at-least-once contract, made effectively-once by the downstream key.
func (s *Server) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		ctx := sess.Context()
		if err := s.handleMessage(ctx, msg, int32(claim.Partition())); err != nil {
			// Only return an error if we neither persisted nor dead-lettered
			// the message. That is the "cannot handle" case: the broker will
			// redeliver (the session is not marked).
			return err
		}
		sess.MarkMessage(msg, "")
	}
	return nil
}

// handleMessage parses one raw envelope and either persists it or dead-letters
// it. It returns an error ONLY when the message could not be durably
// routed anywhere (neither persisted nor DLQ'd) — the at-least-once "retry
// later" path. A message that was persisted or DLQ'd is "handled" regardless
// of which path succeeded; the broker may safely commit its offset.
func (s *Server) handleMessage(ctx context.Context, msg *sarama.ConsumerMessage, partition int32) error {
	env, err := unmarshalEnvelope(msg)
	if err != nil {
		if werr := s.dlq.send(ctx, s.dlqOf(nil, msg, "parser:unmarshal", err)); werr != nil {
			return fmt.Errorf("route unparseable %q to dlq: %w", sourceOf(msg), werr)
		}
		s.log.Warn("envelope undecodable → DLQ", "topic", msg.Topic, "offset", msg.Offset, "err", err)
		return nil
	}

	pm, perr := parseMail(env)
	if perr != nil {
		// Record in dead_letter table for status reporting
		_ = s.persister.DeadLetter(ctx, env, perr.Error())
		
		if werr := s.dlq.send(ctx, s.dlqOf(env, msg, "parser:parse", perr)); werr != nil {
			return fmt.Errorf("route failed-envelope %q to dlq: %w", env.SourcePath, werr)
		}
		s.log.Warn("parse failed → DLQ", "source_path", env.SourcePath, "err", perr)
		return nil
	}

	if perr2 := s.persister.Persist(ctx, pm); perr2 != nil {
		// Record in dead_letter table for status reporting
		_ = s.persister.DeadLetter(ctx, env, perr2.Error())
		
		if werr := s.dlq.send(ctx, s.dlqOf(env, msg, "parser:persist", perr2)); werr != nil {
			return fmt.Errorf("route persist-failed %q to dlq: %w", env.SourcePath, werr)
		}
		s.log.Warn("persist failed → DLQ", "source_path", env.SourcePath, "err", perr2)

		return nil
	}

	return nil
}

// Close releases Kafka resources (consumer group + DLQ producer + persister).
func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing { return nil }
	s.closing = true 
	if err := s.group.Close(); err != nil { s.log.Warn("close consumer group", "err", err) }
	if err := s.dlq.Close(); err != nil { s.log.Warn("close dlq producer", "err", err) }
	if err := s.persister.Close(); err != nil { s.log.Warn("close persister", "err", err) }
	return nil
}

func unmarshalEnvelope(msg *sarama.ConsumerMessage) (*enronv1.RawEnvelope, error) {
	env := &enronv1.RawEnvelope{}
	if err := proto.Unmarshal(msg.Value, env); err != nil {
		return nil, fmt.Errorf("unmarshal RawEnvelope: %w", err)
	}
	return env, nil
}

// sourceOf picks a human-readable identifier for a ConsumerMessage in logs.
func sourceOf(msg *sarama.ConsumerMessage) string {
	if len(msg.Key) > 0 {
		return string(msg.Key)
	}
	return fmt.Sprintf("%s/%d/%d", msg.Topic, msg.Partition, msg.Offset)
}

// dlqOf builds the DeadLetter for one failed message. When env is nil (a
// pre-unmarshal failure) the key falls back to a topic/partition/offset
// identity so the DLQ record is still inspectable and replayable.
func (s *Server) dlqOf(env *enronv1.RawEnvelope, msg *sarama.ConsumerMessage, component string, cause error) *enronv1.DeadLetter {
	key := ""
	if env != nil {
		key = env.IdempotencyKey
	}
	if key == "" {
		key = fmt.Sprintf("undecodable:%s/%d/%d", msg.Topic, msg.Partition, msg.Offset)

	}
	return &enronv1.DeadLetter{
		IdempotencyKey:  key,
		FailedTopic:     msg.Topic,
		FailedPartition: msg.Partition,
		FailedOffset:    msg.Offset,
		ConsumerGroup:   s.cfg.ConsumerGroup,
		FailedComponent: component,
		Error:           cause.Error(),
		FailedAtMs:      time.Now().UnixMilli(),
		Original:        env,
	}
}
