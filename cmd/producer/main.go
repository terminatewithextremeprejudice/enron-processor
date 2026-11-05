// Command producer owns the raw Enron dataset. On Start it ensures the
// dataset is present (downloading + extracting enron_mail_20150507 from
// the assignment URL on first runs), walks the maildir tree, and emits one
// RawEnvelope per mail file. The emit sink is currently a stub (see
// internal/producer/emit.go) — the sarama leg onto enron.raw is the next
// revision; the walk itself is real, idempotent, and resumable.
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

	srv := producer.New(cfg, log)
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
