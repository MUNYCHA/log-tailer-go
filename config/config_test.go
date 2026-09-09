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

func TestValidate_HeartbeatOmitted_DefaultsToEnabledWithInterval(t *testing.T) {
	cfg := validBase()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected no error with heartbeat omitted, got: %v", err)
	}
	if !cfg.Heartbeat.IsEnabled() {
		t.Fatal("expected an omitted heartbeat block to default to enabled")
	}
	if cfg.Heartbeat.Interval != config.DefaultHeartbeatInterval {
		t.Fatalf("expected the default interval %q, got %q", config.DefaultHeartbeatInterval, cfg.Heartbeat.Interval)
	}
	if cfg.Heartbeat.Channel != config.DefaultHeartbeatChannel {
		t.Fatalf("expected the default channel %q, got %q", config.DefaultHeartbeatChannel, cfg.Heartbeat.Channel)
	}
}

func TestValidate_HeartbeatChannelOverrideIsKept(t *testing.T) {
	cfg := validBase()
	cfg.Heartbeat = config.HeartbeatConfig{Channel: "serverA-heartbeat"}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected a custom heartbeat channel to validate, got: %v", err)
	}
	if cfg.Heartbeat.Channel != "serverA-heartbeat" {
		t.Fatalf("expected the override to survive defaulting, got %q", cfg.Heartbeat.Channel)
	}
}

// An explicitly empty channel is a config that would publish nowhere, so it
// falls back to the default rather than starting a silent agent
func TestValidate_HeartbeatEmptyChannelFallsBackToDefault(t *testing.T) {
	cfg := validBase()
	cfg.Heartbeat = config.HeartbeatConfig{Channel: ""}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if cfg.Heartbeat.Channel != config.DefaultHeartbeatChannel {
		t.Fatalf("expected fallback to %q, got %q", config.DefaultHeartbeatChannel, cfg.Heartbeat.Channel)
	}
}

func TestValidate_HeartbeatExplicitlyDisabled(t *testing.T) {
	cfg := validBase()
	disabled := false
	cfg.Heartbeat = config.HeartbeatConfig{Enabled: &disabled}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected no error with heartbeat disabled, got: %v", err)
	}
	if cfg.Heartbeat.IsEnabled() {
		t.Fatal("expected enabled: false to disable the heartbeat")
	}
}

func TestValidate_HeartbeatBadInterval(t *testing.T) {
	cfg := validBase()
	cfg.Heartbeat = config.HeartbeatConfig{Interval: "not-a-duration"}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for invalid heartbeat.interval, got nil")
	}
}

func TestValidate_HeartbeatZeroInterval(t *testing.T) {
	cfg := validBase()
	cfg.Heartbeat = config.HeartbeatConfig{Interval: "0s"}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for zero heartbeat.interval, got nil")
	}
}

// A disabled heartbeat is not validated, so a stale interval cannot block boot
func TestValidate_HeartbeatDisabled_BadIntervalIgnored(t *testing.T) {
	cfg := validBase()
	disabled := false
	cfg.Heartbeat = config.HeartbeatConfig{Enabled: &disabled, Interval: "not-a-duration"}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected no error validating a disabled heartbeat, got: %v", err)
	}
}
