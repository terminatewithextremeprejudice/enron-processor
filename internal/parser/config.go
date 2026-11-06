// Package parser implements the parser service: a Kafka consumer of
// enron.raw that parses each RawEnvelope's RFC-822 mail into a ParsedMessage
// and writes it directly into PostgreSQL (person, message, message_recipient,
// envelope_receipt) in one idempotent transaction. Failures are routed to
// enron.raw.dlq wrapped in a DeadLetter carrying the original envelope +
// error (lossless, inspectable, replayable).
package parser

import (
	"fmt"
	"os"
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

// Sink selects where parsed messages land.
const (
	SinkPostgres = "postgres" // write directly into PostgreSQL (production)
	SinkDummy    = "dummy"    // accept-and-discard (dev without a database)
)

// Config is the runtime configuration of the parser service (config/parser.yaml).

type Config struct {
	// ---- Kafka -------------------------------------------------------------
	BootstrapServers []string `yaml:"bootstrap_servers"`
	RawTopic         string   `yaml:"raw_topic"` // consumed: RawEnvelope protos
	DLQTopic         string   `yaml:"dlq_topic"` // produced: DeadLetter protos
	ConsumerGroup    string   `yaml:"consumer_group"`
	MaxPollRecords   int      `yaml:"max_poll_records"` // advisory (sarama has no exact knob; used for pacing)

	// ---- persistence target (direct Postgres writes) ------------------------
	Persister      string `yaml:"persister"` // "postgres" | "dummy"
	DBDSN          string `yaml:"db_dsn"`    // Postgres DSN (or DB_DSN env)
	Acks           string `yaml:"acks"`      // DLQ producer acks (all|leader)
	Retries        int    `yaml:"retries"`
	ReturnSuccesses bool   `yaml:"return_successes"`
	MaxMessageBytes int   `yaml:"max_message_bytes"`
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
	if v := os.Getenv("DB_DSN"); v != "" { cfg.DBDSN = v }
	// defaults
	if len(cfg.BootstrapServers) == 0 { return cfg, fmt.Errorf("bootstrap_servers required (none configured) %v", cfg.BootstrapServers) }
	if cfg.RawTopic == "" { cfg.RawTopic = defaultRawTopic }
	if cfg.DLQTopic == "" { cfg.DLQTopic = defaultDLQTopic }
	if cfg.ConsumerGroup == "" { cfg.ConsumerGroup = defaultConsumerGroup }
	if cfg.Persister == "" { cfg.Persister = SinkPostgres }
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
