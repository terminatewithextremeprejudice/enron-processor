// Package parser implements the parser service: a Kafka consumer of
// enron.raw that parses each RawEnvelope's RFC-822 mail into a ParsedMessage.
// Success is handed to the (currently stubbed) persistence sink — the
// real sink is the worker's DbWorker.WriteMessage over gRPC, which is the
// next revision; failures are routed to enron.raw.dlq wrapped in a DeadLetter
// carrying the original envelope + error (lossless, inspectable, replayable).
package parser

import (
	"fmt"
	"os"
	"time"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	// defaultRawTopic / defaultDLQTopic mirror the topic contract in the
	// README so a minimal config need not restate them.
	defaultRawTopic = "enron.raw"
	defaultDLQTopic = "enron.raw.dlq"
	// defaultConsumerGroup is the Kafka group id for this leg.
	defaultConsumerGroup = "parser"
)

// Sink selects whether the success path is the (future) worker gRPC leg or the
// current no-op placeholder.
const (
	SinkWorker = "worker" // TODO(later): dial enron.v1.DbWorker.WriteMessage
	SinkDummy  = "dummy"  // accept-and-discard (this revision)
)

// Config is the runtime configuration of the parser service (config/parser.yaml).

type Config struct {
	// ---- Kafka -------------------------------------------------------------
	BootstrapServers []string `yaml:"bootstrap_servers"`
	RawTopic         string   `yaml:"raw_topic"` // consumed: RawEnvelope protos
	DLQTopic         string   `yaml:"dlq_topic"` // produced: DeadLetter protos
	ConsumerGroup    string   `yaml:"consumer_group"`
	MaxPollRecords   int      `yaml:"max_poll_records"` // advisory (sarama has no exact knob; used for pacing)

	// ---- persistence target (worker leg; stubbed this revision) ------------
	Persister            string        `yaml:"persister"` // "dummy" | "worker"
	WorkerGRPCAddr       string        `yaml:"worker_grpc_addr"`
	WorkerRPCTimeout     time.Duration `yaml:"worker_rpc_timeout_ms"` // note: ms in yaml
	Acks                 string        `yaml:"acks"` // DLQ producer acks (all|leader)
	Retries              int           `yaml:"retries"`
	MaxMessageBytes      int           `yaml:"max_message_bytes"`
}

func Load(path string) (Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config %q: %w", path, err)
	}
	var cfg Config
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse config %q: %w", path, err)
	}
	// env overrides
	if v := os.Getenv("KAFKA_BOOTSTRAP_SERVERS"); v != "" { cfg.BootstrapServers = splitHosts(v) }
	if v := os.Getenv("RAW_TOPIC"); v != "" { cfg.RawTopic = v }
	if v := os.Getenv("DLQ_TOPIC"); v != "" { cfg.DLQTopic = v }
	if v := os.Getenv("CONSUMER_GROUP"); v != "" { cfg.ConsumerGroup = v }
	// defaults
	if len(cfg.BootstrapServers) == 0 { return cfg, fmt.Errorf("bootstrap_servers required %d", cfg.BootstrapServers) }
	if cfg.RawTopic == "" { cfg.RawTopic = defaultRawTopic }
	if cfg.DLQTopic == "" { cfg.DLQTopic = defaultDLQTopic }
	if cfg.ConsumerGroup == "" { cfg.ConsumerGroup = defaultConsumerGroup }
	if cfg.Persister == "" { cfg.Persister = SinkDummy }
	return cfg, nil
}

func splitHosts(v string) []string {
	var out []string
	for _, h := range strings.Split(v, ",") {
		if h = strings.TrimSpace(h); h != "" {
			out = append(out, h)
		}
	}
	return out
}
