package docker

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"log-tailer-go/config"
	"log-tailer-go/model"
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

func (p *fakePublisher) events(t *testing.T) []model.DockerEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	events := make([]model.DockerEvent, len(p.raw))
	for i, payload := range p.raw {
		if err := json.Unmarshal(payload, &events[i]); err != nil {
			t.Fatalf("published payload is not a docker event: %v", err)
		}
	}
	return events
}

const sampleList = `[
  {"Id":"2e303154764f2e04bbaacca18140c33112465ffdf1814c8aed8e12f90c986d62","Names":["/valkey-valkey-1"],"Image":"valkey/valkey:8-alpine","State":"running"},
  {"Id":"6a415bcfdc94aa0e1f5b2c3d4e5f60718293a4b5c6d7e8f90123456789abcdef","Names":["/postgres-postgres-1"],"Image":"postgres:16","State":"exited"}
]`

// fakeDocker serves canned replies on a unix socket and records every
// request that reaches it.
type fakeDocker struct {
	mu   sync.Mutex
	seen []string
	info string
}

func (f *fakeDocker) requests() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.seen...)
}

func startFakeDocker(t *testing.T, info string) (*fakeDocker, string) {
	t.Helper()
	socket := filepath.Join(t.TempDir(), "docker.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeDocker{info: info}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.seen = append(f.seen, r.Method+" "+r.URL.RequestURI())
		f.mu.Unlock()
		switch r.URL.Path {
		case "/info":
			w.Write([]byte(f.info))
		case "/containers/json":
			w.Write([]byte(sampleList))
		default:
			http.NotFound(w, r)
		}
	}))
	srv.Listener = ln
	srv.Start()
	t.Cleanup(srv.Close)
	return f, socket
}

func run(t *testing.T, socket string, pub *fakePublisher, d time.Duration) {
	t.Helper()
	identity := config.IdentityConfig{ServerID: "server-1"}
	c := New(socket, "docker-channel", identity, 10*time.Millisecond, pub)
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	c.Run(ctx)
}

func TestCollector_PublishesEveryContainerSorted(t *testing.T) {
	_, socket := startFakeDocker(t, `{"CgroupVersion":"2"}`)
	pub := &fakePublisher{}
	run(t, socket, pub, 50*time.Millisecond)

	events := pub.events(t)
	if len(events) == 0 {
		t.Fatal("expected at least one docker event to be published")
	}
	if ch := pub.chans[0]; ch != "docker-channel" {
		t.Fatalf("expected channel 'docker-channel', got %q", ch)
	}

	ev := events[0]
	if ev.ServerID != "server-1" || ev.Timestamp == "" {
		t.Fatalf("expected serverId and a timestamp, got %+v", ev)
	}
	if ev.Error != "" {
		t.Fatalf("expected no error, got %q", ev.Error)
	}
	if ev.CgroupVersion != 2 {
		t.Fatalf("expected cgroupVersion 2, got %d", ev.CgroupVersion)
	}
	want := []model.DockerContainer{
		{ID: "6a415bcfdc94aa0e1f5b2c3d4e5f60718293a4b5c6d7e8f90123456789abcdef", Name: "postgres-postgres-1", Image: "postgres:16", State: "exited"},
		{ID: "2e303154764f2e04bbaacca18140c33112465ffdf1814c8aed8e12f90c986d62", Name: "valkey-valkey-1", Image: "valkey/valkey:8-alpine", State: "running"},
	}
	if len(ev.Containers) != len(want) {
		t.Fatalf("expected %d containers, got %+v", len(want), ev.Containers)
	}
	for i := range want {
		if ev.Containers[i] != want[i] {
			t.Fatalf("container %d: expected %+v, got %+v", i, want[i], ev.Containers[i])
		}
	}
}

// Docker before 20.10 has no CgroupVersion field and only ran on v1
func TestCollector_OldDockerReportsCgroupV1(t *testing.T) {
	_, socket := startFakeDocker(t, `{"ServerVersion":"19.03.15"}`)
	pub := &fakePublisher{}
	run(t, socket, pub, 30*time.Millisecond)

	events := pub.events(t)
	if len(events) == 0 || events[0].CgroupVersion != 1 {
		t.Fatalf("expected cgroupVersion 1, got %+v", events)
	}
}

// Across many ticks the collector only ever lists containers and asks /info
// once; nothing else reaches Docker
func TestCollector_SendsOnlyTheExpectedRequests(t *testing.T) {
	f, socket := startFakeDocker(t, `{"CgroupVersion":"2"}`)
	pub := &fakePublisher{}
	run(t, socket, pub, 60*time.Millisecond)

	got := f.requests()
	if len(pub.events(t)) < 2 {
		t.Fatalf("expected several ticks, got %d events", len(pub.events(t)))
	}
	infos := 0
	for _, r := range got {
		switch r {
		case "GET /info":
			infos++
		case "GET /containers/json?all=true":
		default:
			t.Fatalf("unexpected request reached docker: %s", r)
		}
	}
	if infos != 1 {
		t.Fatalf("expected /info to be asked exactly once, got %d in %v", infos, got)
	}
}

// Docker down still publishes, with the error and an empty list rather than
// silence, so the consumer can tell "can't read" from "agent dead"
func TestCollector_DockerUnreachablePublishesError(t *testing.T) {
	pub := &fakePublisher{}
	run(t, filepath.Join(t.TempDir(), "no-such.sock"), pub, 30*time.Millisecond)

	events := pub.events(t)
	if len(events) == 0 {
		t.Fatal("expected an event even with docker unreachable")
	}
	ev := events[0]
	if ev.Error == "" {
		t.Fatal("expected error set when docker is unreachable")
	}
	if ev.CgroupVersion != 0 {
		t.Fatalf("expected cgroupVersion omitted while unknown, got %d", ev.CgroupVersion)
	}

	pub.mu.Lock()
	payload := string(pub.raw[0])
	pub.mu.Unlock()
	var raw map[string]any
	if err := json.Unmarshal([]byte(payload), &raw); err != nil {
		t.Fatal(err)
	}
	if list, ok := raw["containers"].([]any); !ok || len(list) != 0 {
		t.Fatalf("expected containers: [] on error, got %s", payload)
	}
	if _, ok := raw["cgroupVersion"]; ok {
		t.Fatalf("expected cgroupVersion omitted while unknown, got %s", payload)
	}
}
