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
	"log-tailer-go/redis"
	"log-tailer-go/tailer"
)

const (
	restartDelay      = time.Second
	redisConnectRetry = 5 * time.Second
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

	// Retry until Redis is reachable so a bare (non-systemd) run survives
	// Redis being down at startup. Ctrl+C / SIGTERM still kills the process
	// here since graceful signal handling is installed later.
	publisher, err := redis.New(cfg.Redis.Addr, cfg.Redis.Password, cfg.Redis.DB)
	for err != nil {
		slog.Error("Failed to connect to Redis, retrying", "error", err, "retry_in", redisConnectRetry)
		time.Sleep(redisConnectRetry)
		publisher, err = redis.New(cfg.Redis.Addr, cfg.Redis.Password, cfg.Redis.DB)
	}

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
					tailer.New(f.Path, f.Channel, serverName, publisher).Run(ctx)
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

	// Publishes are synchronous — nothing is in flight after the tailers
	// return, so close is immediate and cannot stall the exit
	if err := publisher.Close(); err != nil {
		slog.Error("Error closing Redis client", "error", err)
	}

	slog.Info("Shutdown complete")
}
