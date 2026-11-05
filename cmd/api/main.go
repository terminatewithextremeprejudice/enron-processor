// Command api is the HTTP control surface of the Enron mail processor:
//
//	POST /start        trigger (re)ingestion via the producer control gRPC
//	GET  /status       finished? + total messages durably processed
//	GET  /top-senders  top N senders by message count
//	GET  /healthz      process liveness
//
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/terminatewithextremeprejudice/enron-processor/internal/api"
)

func main() {
	cfgPath := flag.String("config", defaultConfigPath(), "path to the api config file (YAML)")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := api.Load(*cfgPath)
	if err != nil {
		log.Error("config", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	store, err := api.NewStore(ctx, cfg.DBDSN)
	if err != nil {
		log.Error("postgres", "err", err)
		os.Exit(1)
	}
	log.Info("postgres connected")

	prod, err := api.NewProducer(cfg.ProducerAddr)
	if err != nil {
		log.Error("producer channel", "err", err)
		os.Exit(1)
	}
	go probeProducer(log, prod, cfg)

	server := api.New(cfg, store, prod, log)
	if err := server.Run(ctx); err != nil {
		log.Error("server", "err", err)
		os.Exit(1)
	}
	server.Close()
	log.Info("api stopped")
}

// probeProducer reports producer reachability without gating startup.
func probeProducer(log *slog.Logger, prod api.Producer, cfg api.Config) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := prod.Probe(ctx); err != nil {
		log.Warn("producer not reachable yet; /start and /status will 503 until it is",
			"addr", cfg.ProducerAddr, "err", err)
		return
	}
	log.Info("producer reachable", "addr", cfg.ProducerAddr)
}

// defaultConfigPath mirrors docker-compose.yml (APP_CONFIG) with a sane
// in-repo fallback for local runs.
func defaultConfigPath() string {
	if p := os.Getenv("APP_CONFIG"); p != "" {
		return p
	}
	return "config/api.yaml"
}
