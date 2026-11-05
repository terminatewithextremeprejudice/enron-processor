package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/terminatewithextremeprejudice/enron-processor/internal/parser"
)

func main() {
	cfgPath := flag.String("config", defaultConfigPath(), "path to the parser config (YAML)")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := parser.Load(*cfgPath)
	if err != nil {
		log.Error("config", "err", err)
		os.Exit(1)
	}

	persister, err := parser.NewPersister(cfg, log)
	if err != nil {
		log.Error("persister", "err", err)
		os.Exit(1)
	}

	srv, err := parser.New(cfg, log, persister)
	if err != nil {
		log.Error("parser init", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := srv.Run(ctx); err != nil {
		log.Error("parser", "err", err)
		os.Exit(1)
	}
	srv.Close()
	log.Info("parser stopped")
}

func defaultConfigPath() string {
	if p := os.Getenv("APP_CONFIG"); p != "" { return p }
	return "config/parser.yaml"
}
