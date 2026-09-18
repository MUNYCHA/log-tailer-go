package config

import (
	"context"
	"fmt"
	"time"
)

// StartupDelay is how long every collector waits before its first event, so
// they all publish together on startup instead of one per interval boundary.
// The resources collector spends it taking the baseline reading its rates are
// differenced against, so the wait costs nothing there.
const StartupDelay = time.Second

// WaitForStartup blocks until the startup delay has passed, or until the
// collector's own interval has, whichever is shorter — a sub-second interval
// is not slowed down to wait for it. It reports false when ctx was cancelled
// first, meaning the caller should return without publishing rather than hold
// shutdown open.
func WaitForStartup(ctx context.Context, interval time.Duration) bool {
	wait := StartupDelay
	if interval < wait {
		wait = interval
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

type AppConfig struct {
	Redis     RedisConfig     `json:"redis"`
	Identity  IdentityConfig  `json:"identity"`
	LogTailer LogTailerConfig `json:"logTailer"`
	Resources ResourcesConfig `json:"resources"`
	Storage   StorageConfig   `json:"storage"`
	Heartbeat HeartbeatConfig `json:"heartbeat"`
}

type RedisConfig struct {
	Addr     string `json:"addr"`
	Password string `json:"password"`
	DB       int    `json:"db"`
}

// IdentityConfig names the server every event comes from. It is one opaque
// id on purpose: a consumer joins on it, and anything else about the server —
// its hostname, address or which system it belongs to — is looked up there
// rather than carried on every event, where it would be a copy that goes
// stale.
type IdentityConfig struct {
	ServerID string `json:"serverId"`
}

type LogTailerConfig struct {
	Enabled bool            `json:"enabled"`
	Files   []LogFileConfig `json:"files"`
}

type LogFileConfig struct {
	Path    string `json:"path"`
	Channel string `json:"channel"`
}

// ResourcesConfig gates the resources event: uptime, cpu, memory, swap and
// network.
type ResourcesConfig struct {
	Enabled  bool   `json:"enabled"`
	Channel  string `json:"channel"`
	Interval string `json:"interval"` // e.g. "30s" — parsed with time.ParseDuration
}

// StorageConfig gates the storage event. Mounts lists paths to report one by
// one and may be empty: the server total is found from the mount table, not
// from this list.
type StorageConfig struct {
	Enabled  bool     `json:"enabled"`
	Channel  string   `json:"channel"`
	Interval string   `json:"interval"` // e.g. "5m" — parsed with time.ParseDuration
	Mounts   []string `json:"mounts"`
}

// HeartbeatConfig gates the liveness beat. Enabled is a pointer so an omitted
// key means "on" — existing configs written before the heartbeat existed get
// it without being edited, while enabled: false stays available.
type HeartbeatConfig struct {
	Enabled  *bool  `json:"enabled"`
	Channel  string `json:"channel"`  // omitted means DefaultHeartbeatChannel
	Interval string `json:"interval"` // e.g. "10s" — parsed with time.ParseDuration
}

func (h HeartbeatConfig) IsEnabled() bool {
	return h.Enabled == nil || *h.Enabled
}

// The consumer expires a server's heartbeat key on a TTL of roughly three
// beats. Raising this past that TTL makes healthy servers read as offline, so
// coordinate a change with the API side before shipping it.
const DefaultHeartbeatInterval = "10s"

// The channel the consumer subscribes to. Overriding it per server is
// supported but is a coordinated change: a name the subscriber does not know
// is not an error, it is silence — Redis discards a publish nobody is
// listening for, and the server then reads as offline while beating happily.
const DefaultHeartbeatChannel = "agent-heartbeat"

func (c *AppConfig) applyDefaults() {
	if c.Heartbeat.Interval == "" {
		c.Heartbeat.Interval = DefaultHeartbeatInterval
	}
	if c.Heartbeat.Channel == "" {
		c.Heartbeat.Channel = DefaultHeartbeatChannel
	}
}

// Validate fills in defaults for omitted optional keys, then reports the first
// problem it finds.
func (c *AppConfig) Validate() error {
	c.applyDefaults()

	if c.Redis.Addr == "" {
		return fmt.Errorf("'redis.addr' is required")
	}
	if c.Identity.ServerID == "" {
		return fmt.Errorf("'identity.serverId' is required")
	}
	if c.LogTailer.Enabled {
		if len(c.LogTailer.Files) == 0 {
			return fmt.Errorf("'logTailer.files' must not be empty when enabled")
		}
		for _, f := range c.LogTailer.Files {
			if f.Path == "" {
				return fmt.Errorf("each 'logTailer.files' entry must have a 'path'")
			}
			if f.Channel == "" {
				return fmt.Errorf("each 'logTailer.files' entry must have a 'channel'")
			}
		}
	}
	if c.Resources.Enabled {
		if c.Resources.Channel == "" {
			return fmt.Errorf("'resources.channel' is required")
		}
		if d, err := time.ParseDuration(c.Resources.Interval); err != nil || d <= 0 {
			return fmt.Errorf("'resources.interval' must be a positive duration (e.g. \"30s\")")
		}
	}
	if c.Storage.Enabled {
		if c.Storage.Channel == "" {
			return fmt.Errorf("'storage.channel' is required")
		}
		if d, err := time.ParseDuration(c.Storage.Interval); err != nil || d <= 0 {
			return fmt.Errorf("'storage.interval' must be a positive duration (e.g. \"5m\")")
		}
		for _, m := range c.Storage.Mounts {
			if m == "" {
				return fmt.Errorf("each 'storage.mounts' entry must be non-empty")
			}
		}
	}
	if c.Heartbeat.IsEnabled() {
		if d, err := time.ParseDuration(c.Heartbeat.Interval); err != nil || d <= 0 {
			return fmt.Errorf("'heartbeat.interval' must be a positive duration (e.g. \"10s\")")
		}
	}
	return nil
}
