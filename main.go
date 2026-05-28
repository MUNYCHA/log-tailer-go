package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"log-tailer-go/config"
	"log-tailer-go/kafka"
	"log-tailer-go/tailer"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	configPath := config.ResolvePath(os.Args[1:])
	slog.Info("Loading config", "path", configPath)

	cfg, err := config.Load(configPath)
	if err != nil {
		slog.Error("Failed to load config", "error", err)
		os.Exit(1)
	}

	if !cfg.LogTailer.Enabled {
		slog.Error("logTailer is not enabled in config, nothing to do")
		os.Exit(1)
	}

	producer, err := kafka.NewProducer(cfg.BootstrapServers)
	if err != nil {
		slog.Error("Failed to create Kafka producer", "error", err)
		os.Exit(1)
	}

	// Drain Kafka delivery errors in background; exits when producer is closed
	errDone := make(chan struct{})
	go func() {
		defer close(errDone)
		kafka.DrainErrors(producer)
	}()

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	serverName := cfg.Identity.Server.Name

	for _, f := range cfg.LogTailer.Files {
		wg.Add(1)
		go func(f config.LogFileConfig) {
			defer wg.Done()
			tailer.New(f.Path, f.Topic, serverName, producer).Run(ctx)
		}(f)
	}

	// Block until SIGTERM or SIGINT
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	sig := <-sigCh

	slog.Info("Received signal, shutting down", "signal", sig)
	cancel()
	wg.Wait()

	if err := producer.Close(); err != nil {
		slog.Error("Error closing Kafka producer", "error", err)
	}
	<-errDone

	slog.Info("Shutdown complete")
}
