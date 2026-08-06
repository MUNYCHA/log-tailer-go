package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"sigs.k8s.io/yaml"
)

const (
	envKey      = "LOGTAILER_CONFIG"
	defaultPath = "config/config.json"
)

// ResolvePath picks the config path from CLI args, env var, or the built-in default.
// Priority: --config flag > LOGTAILER_CONFIG env var > default path.
func ResolvePath(args []string) string {
	for i, arg := range args {
		if strings.HasPrefix(arg, "--config=") {
			return strings.TrimPrefix(arg, "--config=")
		}
		if arg == "--config" && i+1 < len(args) {
			return args[i+1]
		}
		if !strings.HasPrefix(arg, "-") && strings.TrimSpace(arg) != "" {
			return arg
		}
	}
	if path := os.Getenv(envKey); path != "" {
		return path
	}
	return defaultPath
}

func Load(path string) (*AppConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cannot open config file %q: %w", path, err)
	}

	var cfg AppConfig
	switch ext := strings.ToLower(filepath.Ext(path)); ext {
	case ".yaml", ".yml":
		err = yaml.UnmarshalStrict(data, &cfg)
	default:
		err = json.Unmarshal(data, &cfg)
	}
	if err != nil {
		return nil, fmt.Errorf("cannot parse config file %q: %w", path, err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return &cfg, nil
}
