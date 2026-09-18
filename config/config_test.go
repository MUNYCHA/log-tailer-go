package config_test

import (
	"testing"

	"log-tailer-go/config"
)

func validBase() config.AppConfig {
	return config.AppConfig{
		Redis: config.RedisConfig{Addr: "127.0.0.1:6379"},
		Identity: config.IdentityConfig{
			ServerID: "server-name",
		},
	}
}

func TestValidate_ResourcesAndStorageDisabled_NoChecks(t *testing.T) {
	cfg := validBase()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected no error with resources and storage disabled, got: %v", err)
	}
}

// A disabled block is not validated, so stale values cannot block boot
func TestValidate_ResourcesAndStorageDisabled_BadValuesIgnored(t *testing.T) {
	cfg := validBase()
	cfg.Resources = config.ResourcesConfig{Interval: "not-a-duration"}
	cfg.Storage = config.StorageConfig{Interval: "not-a-duration", Mounts: []string{""}}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected no error validating disabled blocks, got: %v", err)
	}
}

func TestValidate_ResourcesEnabled_MissingChannel(t *testing.T) {
	cfg := validBase()
	cfg.Resources = config.ResourcesConfig{Enabled: true, Interval: "30s"}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for missing resources.channel, got nil")
	}
}

func TestValidate_ResourcesEnabled_BadInterval(t *testing.T) {
	cfg := validBase()
	cfg.Resources = config.ResourcesConfig{Enabled: true, Channel: "resources-channel", Interval: "not-a-duration"}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for invalid resources.interval, got nil")
	}
}

func TestValidate_ResourcesEnabled_ZeroInterval(t *testing.T) {
	cfg := validBase()
	cfg.Resources = config.ResourcesConfig{Enabled: true, Channel: "resources-channel", Interval: "0s"}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for zero resources.interval, got nil")
	}
}

func TestValidate_ResourcesEnabled_Valid(t *testing.T) {
	cfg := validBase()
	cfg.Resources = config.ResourcesConfig{Enabled: true, Channel: "resources-channel", Interval: "30s"}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected valid resources config to pass, got: %v", err)
	}
}

func TestValidate_StorageEnabled_MissingChannel(t *testing.T) {
	cfg := validBase()
	cfg.Storage = config.StorageConfig{Enabled: true, Interval: "5m", Mounts: []string{"/"}}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for missing storage.channel, got nil")
	}
}

func TestValidate_StorageEnabled_BadInterval(t *testing.T) {
	cfg := validBase()
	cfg.Storage = config.StorageConfig{Enabled: true, Channel: "storage-channel", Interval: "not-a-duration"}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for invalid storage.interval, got nil")
	}
}

func TestValidate_StorageEnabled_ZeroInterval(t *testing.T) {
	cfg := validBase()
	cfg.Storage = config.StorageConfig{Enabled: true, Channel: "storage-channel", Interval: "0s"}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for zero storage.interval, got nil")
	}
}

func TestValidate_StorageEnabled_EmptyMountEntry(t *testing.T) {
	cfg := validBase()
	cfg.Storage = config.StorageConfig{Enabled: true, Channel: "storage-channel", Interval: "5m", Mounts: []string{"/", ""}}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for an empty storage.mounts entry, got nil")
	}
}

// The server total comes from the mount table, so no configured mounts is valid
func TestValidate_StorageEnabled_NoMountsIsValid(t *testing.T) {
	cfg := validBase()
	cfg.Storage = config.StorageConfig{Enabled: true, Channel: "storage-channel", Interval: "5m"}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected storage without mounts to pass, got: %v", err)
	}
}

func TestValidate_StorageEnabled_Valid(t *testing.T) {
	cfg := validBase()
	cfg.Storage = config.StorageConfig{Enabled: true, Channel: "storage-channel", Interval: "5m", Mounts: []string{"/", "/var/log"}}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected valid storage config to pass, got: %v", err)
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
