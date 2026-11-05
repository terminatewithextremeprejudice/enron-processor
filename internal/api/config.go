package api

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

const (
	defaultListenAddr   = ":8080"
	defaultProducerAddr = "producer:9000"
	defaultTopN         = 10
)

// Config is the runtime configuration of the api service (config/api.yaml).
type Config struct {
	// ListenAddr is the HTTP bind address (e.g. ":8080").
	ListenAddr string `yaml:"listen_addr"`

	// DBDSN is the PostgreSQL DSN used for the read-only status queries.
	DBDSN string `yaml:"db_dsn"`

	// ProducerAddr is host:port of the producer's control gRPC service
	// (enron.v1.Producer Start/Status).
	ProducerAddr string `yaml:"producer_addr"`

	// TopN is the size of the /top-senders result set.
	TopN int `yaml:"top_n"`

	// BootstrapServers is reserved for a future Kafka-based trigger path;
	// this revision triggers exclusively over gRPC.
	BootstrapServers []string `yaml:"bootstrap_servers"`
}

// Load reads the YAML config file and applies environment overrides.
func Load(path string) (Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config %q: %w", path, err)
	}
	var cfg Config
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse config %q: %w", path, err)
	}
	if v := os.Getenv("DB_DSN"); v != "" {
		cfg.DBDSN = v
	}
	if v := os.Getenv("PRODUCER_GRPC_ADDR"); v != "" {
		cfg.ProducerAddr = v
	}

	if cfg.DBDSN == "" {
		return Config{}, fmt.Errorf("%s: db_dsn is required (or set DB_DSN)", path)
	}
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = defaultListenAddr
	}
	if cfg.ProducerAddr == "" {
		cfg.ProducerAddr = defaultProducerAddr
	}
	if cfg.TopN <= 0 {
		cfg.TopN = defaultTopN
	}
	return cfg, nil
}
