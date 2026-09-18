package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"log-tailer-go/config"
	"log-tailer-go/model"
	"log-tailer-go/storage/mounts"
)

// fakePublisher records published payloads. No Redis involved.
type fakePublisher struct {
	mu    sync.Mutex
	raw   [][]byte
	chans []string
}

func (p *fakePublisher) PublishBatch(_ context.Context, channel string, payloads [][]byte) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, payload := range payloads {
		p.raw = append(p.raw, payload)
		p.chans = append(p.chans, channel)
	}
	return len(payloads)
}

func (p *fakePublisher) events(t *testing.T) []model.StorageEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	events := make([]model.StorageEvent, len(p.raw))
	for i, payload := range p.raw {
		if err := json.Unmarshal(payload, &events[i]); err != nil {
			t.Fatalf("published payload is not a storage event: %v", err)
		}
	}
	return events
}

func TestCollector_PublishesOneMixedGoodAndBadMount(t *testing.T) {
	pub := &fakePublisher{}
	identity := config.IdentityConfig{
		ServerID: "server-1",
	}
	c := New([]string{"/", "/this/path/does/not/exist/hopefully"}, "storage-channel", identity, 10*time.Millisecond, pub)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	c.Run(ctx)

	events := pub.events(t)
	if len(events) == 0 {
		t.Fatal("expected at least one storage event to be published")
	}
	if ch := pub.chans[0]; ch != "storage-channel" {
		t.Fatalf("expected channel 'storage-channel', got %q", ch)
	}

	ev := events[0]
	if ev.ServerID != "server-1" {
		t.Fatalf("expected identity to be copied from config, got %+v", ev)
	}
	if ev.Timestamp == "" {
		t.Fatal("expected a timestamp")
	}
	if len(ev.Mounts) != 2 {
		t.Fatalf("expected 2 mounts in event, got %d", len(ev.Mounts))
	}
	root := ev.Mounts[0]
	if root.Error != "" || root.DiskUsage == nil || root.TotalBytes == 0 {
		t.Fatalf("expected / to have a reading and no error, got %+v", root)
	}
	if root.Device == "" || root.FSType == "" {
		t.Fatalf("expected / to carry device and fsType from the mount table, got %+v", root)
	}
	if ev.Mounts[1].Error == "" {
		t.Fatal("expected the bad mount path to have an error set")
	}
	if ev.Mounts[1].DiskUsage != nil {
		t.Fatalf("expected no sizes on error, got %+v", ev.Mounts[1].DiskUsage)
	}
}

// A failed mount must publish only path and error, never zero sizes
func TestMountUsage_ErrorOmitsSizesInJSON(t *testing.T) {
	c := New(nil, "storage-channel", config.IdentityConfig{}, time.Minute, &fakePublisher{})
	payload, err := json.Marshal(c.mountUsage(nil, "/this/path/does/not/exist/hopefully"))
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	if err := json.Unmarshal(payload, &raw); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"device", "fsType", "totalBytes", "usedBytes", "freeBytes", "reservedBytes", "usedPercent"} {
		if _, ok := raw[key]; ok {
			t.Fatalf("expected %q omitted on error, got %s", key, payload)
		}
	}
	if raw["path"] != "/this/path/does/not/exist/hopefully" || raw["error"] == "" {
		t.Fatalf("expected path and error, got %s", payload)
	}
}

// A network path is reported without calling statfs, so a dead NAS can't
// block the event. The path doesn't exist, so a statfs would have failed with
// a different error.
func TestMountUsage_NetworkFilesystemNotStatted(t *testing.T) {
	table := []mounts.Entry{
		{Device: "/dev/sda1", Path: "/", FSType: "ext4"},
		{Device: "nas:/export", Path: "/no/such/nas", FSType: "nfs4"},
	}
	c := New(nil, "storage-channel", config.IdentityConfig{}, time.Minute, &fakePublisher{})
	got := c.mountUsage(table, "/no/such/nas/share")
	want := model.MountUsage{Path: "/no/such/nas/share", FSType: "nfs4", Error: "network filesystem not supported"}
	if got != want {
		t.Fatalf("expected %+v, got %+v", want, got)
	}
}

// mounts is optional in config, so no mounts still publishes, with [] not null
func TestCollector_NoMountsPublishesEmptyList(t *testing.T) {
	pub := &fakePublisher{}
	c := New(nil, "storage-channel", config.IdentityConfig{}, 10*time.Millisecond, pub)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	c.Run(ctx)

	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.raw) == 0 {
		t.Fatal("expected a storage event even with no mounts configured")
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(pub.raw[0], &raw); err != nil {
		t.Fatal(err)
	}
	if got := string(raw["mounts"]); got != "[]" {
		t.Fatalf("expected mounts to be [], got %s", got)
	}
}

func TestCollector_PublishesServerTotal(t *testing.T) {
	pub := &fakePublisher{}
	c := New(nil, "storage-channel", config.IdentityConfig{}, 10*time.Millisecond, pub)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	c.Run(ctx)

	events := pub.events(t)
	if len(events) == 0 {
		t.Fatal("expected at least one storage event")
	}
	server := events[0].Server
	if server == nil {
		t.Fatal("expected a server total on a host with a local root filesystem")
	}
	if server.TotalBytes == 0 {
		t.Fatal("expected a non-zero server totalBytes")
	}
	if server.UsedBytes+server.FreeBytes+server.ReservedBytes != server.TotalBytes {
		t.Fatalf("expected used + free + reserved = total, got %+v", *server)
	}
	if server.UsedPercent < 0 || server.UsedPercent > 100 {
		t.Fatalf("expected usedPercent in [0,100], got %f", server.UsedPercent)
	}
}

func TestServerStorage_UnreadableFilesystemIsPartial(t *testing.T) {
	c := New(nil, "storage-channel", config.IdentityConfig{}, time.Minute, &fakePublisher{})
	table := []mounts.Entry{
		{Device: "/dev/sda1", Path: "/", FSType: "ext4"},
		{Device: "/dev/sdb1", Path: "/this/path/does/not/exist/hopefully", FSType: "xfs"},
	}
	got := c.serverStorage(table)
	if got == nil {
		t.Fatal("expected a total from the readable filesystem")
	}
	if !got.Partial {
		t.Fatal("expected partial true when a local filesystem can't be read")
	}
	if !c.failing["/this/path/does/not/exist/hopefully"] {
		t.Fatal("expected the failure to be remembered so it is logged once")
	}
	if len(got.MissingPaths) != 1 || got.MissingPaths[0] != "/this/path/does/not/exist/hopefully" {
		t.Fatalf("expected the unreadable path named in missingPaths, got %v", got.MissingPaths)
	}
}

func TestServerStorage_MissingPathsSortedAndOmittedWhenComplete(t *testing.T) {
	c := New(nil, "storage-channel", config.IdentityConfig{}, time.Minute, &fakePublisher{})

	table := []mounts.Entry{
		{Device: "/dev/sda1", Path: "/", FSType: "ext4"},
		{Device: "/dev/sdc1", Path: "/zz/missing", FSType: "ext4"},
		{Device: "/dev/sdb1", Path: "/aa/missing", FSType: "xfs"},
	}
	got := c.serverStorage(table)
	if got == nil {
		t.Fatal("expected a total from the readable filesystem")
	}
	want := []string{"/aa/missing", "/zz/missing"}
	if len(got.MissingPaths) != len(want) {
		t.Fatalf("expected %v, got %v", want, got.MissingPaths)
	}
	for i, path := range want {
		if got.MissingPaths[i] != path {
			t.Fatalf("expected missingPaths sorted as %v, got %v", want, got.MissingPaths)
		}
	}

	// A complete total carries no missingPaths key at all, so a consumer can
	// test for the field's presence rather than for an empty list.
	c = New(nil, "storage-channel", config.IdentityConfig{}, time.Minute, &fakePublisher{})
	complete := c.serverStorage([]mounts.Entry{{Device: "/dev/sda1", Path: "/", FSType: "ext4"}})
	if complete == nil || complete.Partial {
		t.Fatalf("expected a complete total, got %+v", complete)
	}
	payload, err := json.Marshal(complete)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(payload, []byte("missingPaths")) {
		t.Fatalf("expected missingPaths omitted from a complete total, got %s", payload)
	}
}

func TestServerStorage_NothingReadableOmitsTotal(t *testing.T) {
	c := New(nil, "storage-channel", config.IdentityConfig{}, time.Minute, &fakePublisher{})
	if got := c.serverStorage(nil); got != nil {
		t.Fatalf("expected no total without a mount table, got %+v", got)
	}
	table := []mounts.Entry{{Device: "/dev/sdb1", Path: "/this/path/does/not/exist/hopefully", FSType: "ext4"}}
	if got := c.serverStorage(table); got != nil {
		t.Fatalf("expected no total when no filesystem can be read, got %+v", got)
	}
	payload, err := json.Marshal(model.StorageEvent{Mounts: []model.MountUsage{}})
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	if err := json.Unmarshal(payload, &raw); err != nil {
		t.Fatal(err)
	}
	if _, ok := raw["server"]; ok {
		t.Fatalf("expected server omitted when nil, got %s", payload)
	}
}

// A failing path is logged when it starts failing and when it recovers, not on
// every tick
func TestMountUsage_FailureLoggedOncePerState(t *testing.T) {
	var logs bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	defer slog.SetDefault(prev)

	dir := t.TempDir()
	path := filepath.Join(dir, "comes-and-goes")
	c := New(nil, "storage-channel", config.IdentityConfig{}, time.Minute, &fakePublisher{})

	for i := 0; i < 3; i++ {
		if got := c.mountUsage(nil, path); got.Error == "" {
			t.Fatalf("tick %d: expected an error for a missing path", i)
		}
	}
	if n := strings.Count(logs.String(), "Failed to stat mount"); n != 1 {
		t.Fatalf("expected 1 failure log over 3 failing ticks, got %d:\n%s", n, logs.String())
	}

	if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if got := c.mountUsage(nil, path); got.Error != "" {
			t.Fatalf("tick %d: expected a reading after the path appeared, got %q", i, got.Error)
		}
	}
	if n := strings.Count(logs.String(), "Mount readable again"); n != 1 {
		t.Fatalf("expected 1 recovery log, got %d:\n%s", n, logs.String())
	}

	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	c.mountUsage(nil, path)
	if n := strings.Count(logs.String(), "Failed to stat mount"); n != 2 {
		t.Fatalf("expected a new failure log after failing again, got %d", n)
	}
}

func TestCollector_PublishesImmediatelyOnStart(t *testing.T) {
	pub := &fakePublisher{}
	// An interval far longer than the test: anything published can only be
	// the collect that runs before the ticker's first tick.
	c := New([]string{"/"}, "storage-channel", config.IdentityConfig{ServerID: "server-1"}, time.Hour, pub)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	c.Run(ctx)

	events := pub.events(t)
	if len(events) != 1 {
		t.Fatalf("expected exactly one event before the first tick, got %d", len(events))
	}
	// Nothing in storage is differenced between ticks, so it is complete
	if len(events[0].Mounts) != 1 || events[0].Mounts[0].DiskUsage == nil {
		t.Fatalf("expected the first event to be complete, got %+v", events[0].Mounts)
	}
}
