package config_test

import (
	"testing"

	"log-tailer-go/config"
)

func validBase() config.AppConfig {
	return config.AppConfig{
		Redis: config.RedisConfig{Addr: "127.0.0.1:6379"},
		Identity: config.IdentityConfig{
			System: config.SystemIdentity{ID: "sys", Name: "sys-name"},
			Server: config.ServerIdentity{Name: "server-name"},
		},
	}
}

func TestValidate_MetricsDisabled_NoChecks(t *testing.T) {
	cfg := validBase()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected no error with metrics disabled, got: %v", err)
	}
}

func TestValidate_MetricsEnabled_MissingChannel(t *testing.T) {
	cfg := validBase()
	cfg.Metrics = config.MetricsConfig{
		Enabled:  true,
		Interval: "1m",
		Mounts:   []string{"/"},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for missing metrics.channel, got nil")
	}
}

func TestValidate_MetricsEnabled_BadInterval(t *testing.T) {
	cfg := validBase()
	cfg.Metrics = config.MetricsConfig{
		Enabled:  true,
		Channel:  "metrics-channel",
		Interval: "not-a-duration",
		Mounts:   []string{"/"},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for invalid metrics.interval, got nil")
	}
}

func TestValidate_MetricsEnabled_ZeroInterval(t *testing.T) {
	cfg := validBase()
	cfg.Metrics = config.MetricsConfig{
		Enabled:  true,
		Channel:  "metrics-channel",
		Interval: "0s",
		Mounts:   []string{"/"},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for zero metrics.interval, got nil")
	}
}

func TestValidate_MetricsEnabled_EmptyMounts(t *testing.T) {
	cfg := validBase()
	cfg.Metrics = config.MetricsConfig{
		Enabled:  true,
		Channel:  "metrics-channel",
		Interval: "1m",
		Mounts:   nil,
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for empty metrics.mounts, got nil")
	}
}

func TestValidate_MetricsEnabled_Valid(t *testing.T) {
	cfg := validBase()
	cfg.Metrics = config.MetricsConfig{
		Enabled:  true,
		Channel:  "metrics-channel",
		Interval: "1m",
		Mounts:   []string{"/", "/var/log"},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected valid metrics config to pass, got: %v", err)
	}
}
