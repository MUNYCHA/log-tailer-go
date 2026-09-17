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
	"log-tailer-go/heartbeat"
	"log-tailer-go/logs"
	"log-tailer-go/redis"
	"log-tailer-go/resources"
	"log-tailer-go/storage"
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

	if !cfg.LogTailer.Enabled && !cfg.Resources.Enabled && !cfg.Storage.Enabled && !cfg.Heartbeat.IsEnabled() {
		slog.Error("No component is enabled in config, nothing to do")
		os.Exit(1)
	}

	var resourcesInterval time.Duration
	if cfg.Resources.Enabled {
		resourcesInterval, err = time.ParseDuration(cfg.Resources.Interval)
		if err != nil {
			slog.Error("Failed to parse resources.interval", "error", err)
			os.Exit(1)
		}
	}

	var storageInterval time.Duration
	if cfg.Storage.Enabled {
		storageInterval, err = time.ParseDuration(cfg.Storage.Interval)
		if err != nil {
			slog.Error("Failed to parse storage.interval", "error", err)
			os.Exit(1)
		}
	}

	var heartbeatInterval time.Duration
	if cfg.Heartbeat.IsEnabled() {
		heartbeatInterval, err = time.ParseDuration(cfg.Heartbeat.Interval)
		if err != nil {
			slog.Error("Failed to parse heartbeat.interval", "error", err)
			os.Exit(1)
		}
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

	if cfg.LogTailer.Enabled {
		for _, f := range cfg.LogTailer.Files {
			runSupervised(ctx, &wg, "tailer:"+f.Path, func(ctx context.Context) {
				logs.New(f.Path, f.Channel, cfg.Identity, publisher).Run(ctx)
			})
		}
	}

	if cfg.Resources.Enabled {
		runSupervised(ctx, &wg, "resources", func(ctx context.Context) {
			resources.New(cfg.Resources.Channel, cfg.Identity, resourcesInterval, publisher).Run(ctx)
		})
	}

	// Supervised separately from resources on purpose: a stat stuck on a dead
	// mount only delays the storage event, never cpu or memory
	if cfg.Storage.Enabled {
		runSupervised(ctx, &wg, "storage", func(ctx context.Context) {
			storage.New(cfg.Storage.Mounts, cfg.Storage.Channel, cfg.Identity, storageInterval, publisher).Run(ctx)
		})
	}

	// Supervised separately from storage on purpose: a collector wedged on a
	// stuck mount must not be able to stop the beat
	if cfg.Heartbeat.IsEnabled() {
		runSupervised(ctx, &wg, "heartbeat", func(ctx context.Context) {
			heartbeat.New(cfg.Identity, cfg.Heartbeat.Channel, heartbeatInterval, publisher).Run(ctx)
		})
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

// runSupervised starts run in its own goroutine, restarting it after a panic
// (with restartDelay pause so a crash loop can't spin hot) until ctx is
// cancelled. wg is released only once the goroutine has fully exited.
func runSupervised(ctx context.Context, wg *sync.WaitGroup, name string, run func(context.Context)) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			func() {
				defer func() {
					if r := recover(); r != nil {
						slog.Error("Component panicked, restarting", "component", name, "panic", r)
					}
				}()
				run(ctx)
			}()
			select {
			case <-ctx.Done():
			case <-time.After(restartDelay):
			}
		}
	}()
}
