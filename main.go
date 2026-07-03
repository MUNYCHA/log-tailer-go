package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"log-tailer-go/config"
	"log-tailer-go/kafka"
	"log-tailer-go/tailer"
)

const (
	restartDelay      = time.Second
	kafkaConnectRetry = 5 * time.Second
	// Must stay below the systemd unit's TimeoutStopSec=20
	flushTimeout = 10 * time.Second
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

	// Retry until Kafka is reachable so a bare (non-systemd) run survives
	// Kafka being down at startup. Ctrl+C / SIGTERM still kills the process
	// here since graceful signal handling is installed later.
	producer, err := kafka.NewProducer(cfg.BootstrapServers)
	for err != nil {
		slog.Error("Failed to create Kafka producer, retrying", "error", err, "retry_in", kafkaConnectRetry)
		time.Sleep(kafkaConnectRetry)
		producer, err = kafka.NewProducer(cfg.BootstrapServers)
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
			for ctx.Err() == nil {
				func() {
					defer func() {
						if r := recover(); r != nil {
							slog.Error("Tailer panicked, restarting", "path", f.Path, "panic", r)
						}
					}()
					tailer.New(f.Path, f.Topic, serverName, producer).Run(ctx)
				}()
				// Pause before restart so a repeatedly-panicking tailer
				// cannot spin at full speed
				select {
				case <-ctx.Done():
				case <-time.After(restartDelay):
				}
			}
		}(f)
	}

	// Block until SIGTERM or SIGINT
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	sig := <-sigCh

	slog.Info("Received signal, shutting down", "signal", sig)
	cancel()
	wg.Wait()

	// Flush with a deadline — against a dead Kafka, Close can spend minutes
	// retrying messages that are doomed anyway
	closeDone := make(chan struct{})
	go func() {
		defer close(closeDone)
		if err := producer.Close(); err != nil {
			slog.Error("Error closing Kafka producer", "error", err)
		}
		<-errDone
	}()
	select {
	case <-closeDone:
	case <-time.After(flushTimeout):
		slog.Warn("Kafka flush timed out, exiting without full delivery", "timeout", flushTimeout)
	}

	slog.Info("Shutdown complete")
}
