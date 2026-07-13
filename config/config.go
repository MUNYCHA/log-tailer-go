package config

import "fmt"

type AppConfig struct {
	Redis     RedisConfig     `json:"redis"`
	Identity  IdentityConfig  `json:"identity"`
	LogTailer LogTailerConfig `json:"logTailer"`
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
	return nil
}
