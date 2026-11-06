// Package producer implements the producer service: on Start it ensures
// the Enron dataset tree is present (downloading + extracting it on first
// runs), walks the maildir, and emits one RawEnvelope per mail file.
//
// Control surface (consumed by the api service):
//
//	gRPC enron.v1.Producer.Start   "ensure a walk is running"  (from api POST /start)
//	gRPC enron.v1.Producer.Status  state + walk progress       (from api GET /status)
//
// STATUS: KAFKA LEG LANDED. The dataset fetch (dataset.go), the maildir
// walk (walk.go), and the state machine (server.go) are real, and the emit
// sink (emit.go) now carries each envelope to cfg.RawTopic over a sarama
// sync producer (config.Sink == "kafka") — or, with config.Sink == "dummy",
// drops it so the pipeline still runs broker-less (dry runs, e2e).
package producer

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

const (
	// defaultListenAddr is the in-network control gRPC bind; the api
	// service dials it (default producer_addr: producer:9000).
	defaultListenAddr = ":9000"
	// defaultDataDir mirrors the repo convention of `make fetch-dataset`
	// (unpack into ./data) for local runs outside Compose.
	defaultDataDir = "data"
)

// Sink selects the envelope destination. These are the accepted values of
// Config.Sink (see newSink in emit.go).
const (
	// SinkKafka writes raw envelopes to cfg.RawTopic over sarama.
	SinkKafka = "kafka"
	// SinkDummy accepts and discards envelopes; the pipeline runs without a
	// broker (dry runs, e2e).
	SinkDummy = "dummy"
)

// defaultRawTopic / defaultDLQTopic mirror the README's topic contract
// ("enron.raw" is the raw envelope channel, "enron.raw.dlq" the dead-letter
// channel) so a minimal config need not restate them.
const (
	defaultRawTopic = "enron.raw"
	defaultDLQTopic = "enron.raw.dlq"
)

// Config is the runtime configuration of the producer service
// (config/producer.yaml).
type Config struct {
	// ListenAddr is the control gRPC bind address (e.g. ":9000").
	ListenAddr string `yaml:"listen_addr"`

	// ---- ingestion --------------------------------------------------------
	// DataDir is where the dataset is fetched/unpacked to (root =
	// DataDir/DatasetName).
	DataDir string `yaml:"data_dir"`
	// DatasetName is the unpacked tree's directory name.
	DatasetName string `yaml:"dataset_name"`

	// ---- Kafka emission (see emit.go: the sarama sink behind Server.sink) -
	// Sink selects the envelope destination: SinkKafka (sarama → RawTopic) or
	// SinkDummy (accept-and-discard, for dry runs and the e2e test).
	Sink string `yaml:"sink"`
	// BootstrapServers is the Kafka contact point list for the raw leg
	// (e.g. ["kafka:9092"] in-network, ["localhost:9093"] host-side).
	BootstrapServers []string `yaml:"bootstrap_servers"`
	RawTopic         string   `yaml:"raw_topic"`
	DLQTopic         string   `yaml:"dlq_topic"`
	// Producers is the number of parallel mail-walker goroutines planned
	// for the final walk; the current walk is single-pass.
	Producers       int    `yaml:"producers"`
	Acks            string `yaml:"acks"`
	Retries         int    `yaml:"retries"`
	ReturnSuccesses bool   `yaml:"return_successes"`
	MaxMessageBytes int    `yaml:"max_message_bytes"`
	MaxEnvelopeBytes int   `yaml:"max_envelope_bytes"` // truncate raw bytes if exceeded
	BatchSize       int    `yaml:"batch_size"`
	FlushMS         int    `yaml:"flush_ms"`
}

// Load reads the YAML config file.
//
// The file is required: a service that silently fell back to defaults
// would hide deploy mistakes. The only values with defaults are the ones
// with safe, documented defaults (listen_addr, data_dir, dataset_name).
func Load(path string) (Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config %q: %w", path, err)
	}
	var cfg Config
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse config %q: %w", path, err)
	}
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = defaultListenAddr
	}
	if cfg.DataDir == "" {
		cfg.DataDir = defaultDataDir
	}
	if cfg.DatasetName == "" {
		cfg.DatasetName = datasetNameCanonical
	}
	// The sink defaults to Kafka (the production leg) so an operator who
	// wants the broker does not have to spell it out. A misspelled value is
	// a deploy mistake and is refused at load rather than swallowed.
	if cfg.Sink == "" {
		cfg.Sink = SinkKafka
	}
	if cfg.Sink != SinkKafka && cfg.Sink != SinkDummy {
		return Config{}, fmt.Errorf("config %q: sink %q is not %q or %q", path, cfg.Sink, SinkKafka, SinkDummy)
	}
	if cfg.RawTopic == "" {
		cfg.RawTopic = defaultRawTopic
	}
	if cfg.DLQTopic == "" {
		cfg.DLQTopic = defaultDLQTopic
	}
	if cfg.Acks == "" {
		cfg.Acks = "all"
	}
	return cfg, nil
}
