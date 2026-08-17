package config

import (
	"fmt"
	"time"
)

type AppConfig struct {
	Redis     RedisConfig     `json:"redis"`
	Identity  IdentityConfig  `json:"identity"`
	LogTailer LogTailerConfig `json:"logTailer"`
	Metrics   MetricsConfig   `json:"metrics"`
}

type RedisConfig struct {
	Addr     string `json:"addr"`
	Password string `json:"password"`
	DB       int    `json:"db"`
}

type IdentityConfig struct {
	System SystemIdentity `json:"system"`
	Server ServerIdentity `json:"server"`
}

type SystemIdentity struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type ServerIdentity struct {
	Name string `json:"name"`
	IP   string `json:"ip"`
}

type LogTailerConfig struct {
	Enabled bool            `json:"enabled"`
	Files   []LogFileConfig `json:"files"`
}

type LogFileConfig struct {
	Path    string `json:"path"`
	Channel string `json:"channel"`
}

type MetricsConfig struct {
	Enabled  bool     `json:"enabled"`
	Channel  string   `json:"channel"`
	Interval string   `json:"interval"` // e.g. "1m", "30s" — parsed with time.ParseDuration
	Mounts   []string `json:"mounts"`
}

func (c *AppConfig) Validate() error {
	if c.Redis.Addr == "" {
		return fmt.Errorf("'redis.addr' is required")
	}
	if c.Identity.System.ID == "" {
		return fmt.Errorf("'identity.system.id' is required")
	}
	if c.Identity.System.Name == "" {
		return fmt.Errorf("'identity.system.name' is required")
	}
	if c.Identity.Server.Name == "" {
		return fmt.Errorf("'identity.server.name' is required")
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
	if c.Metrics.Enabled {
		if c.Metrics.Channel == "" {
			return fmt.Errorf("'metrics.channel' is required")
		}
		if d, err := time.ParseDuration(c.Metrics.Interval); err != nil || d <= 0 {
			return fmt.Errorf("'metrics.interval' must be a positive duration (e.g. \"1m\")")
		}
		if len(c.Metrics.Mounts) == 0 {
			return fmt.Errorf("'metrics.mounts' must not be empty when enabled")
		}
		for _, m := range c.Metrics.Mounts {
			if m == "" {
				return fmt.Errorf("each 'metrics.mounts' entry must be non-empty")
			}
		}
	}
	return nil
}
