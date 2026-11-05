// Command producer owns the raw Enron dataset. On Start it ensures the
// dataset is present (downloading + extracting enron_mail_20150507 from
// the assignment URL on first runs), walks the maildir tree, and emits one
// RawEnvelope per mail file. Each envelope is written to the raw Kafka topic
// over a sarama sync producer (config.Sink == "kafka"); set config.Sink to
// "dummy" to accept-and-discard envelopes for a broker-less dry run.
// The walk itself is real, idempotent, and resumable.
//
// The control plane is the full enron.v1.Producer contract over gRPC,
// driven by the api service:
//
//	api POST /start  -> gRPC Producer.Start   "ensure a walk is running"
//	api GET  /status -> gRPC Producer.Status  state + walk progress
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/terminatewithextremeprejudice/enron-processor/internal/producer"
)

func main() {
	cfgPath := flag.String("config", defaultConfigPath(), "path to the producer config file (YAML)")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := producer.Load(*cfgPath)
	if err != nil {
		log.Error("config", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	srv, err := producer.New(cfg, log)
	if err != nil {
		log.Error("producer init", "err", err)
		os.Exit(1)
	}
	if err := srv.Run(ctx); err != nil {
		log.Error("server", "err", err)
		os.Exit(1)
	}
	srv.Close()
	log.Info("producer stopped")
}

// defaultConfigPath mirrors docker-compose.yml (APP_CONFIG) with a sane
// in-repo fallback for local runs.
func defaultConfigPath() string {
	if p := os.Getenv("APP_CONFIG"); p != "" {
		return p
	}
	return "config/producer.yaml"
}
