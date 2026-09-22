package docker

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"log-tailer-go/config"
	"log-tailer-go/docker/stats"
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

const (
	valkeyID   = "2e303154764f2e04bbaacca18140c33112465ffdf1814c8aed8e12f90c986d62"
	postgresID = "6a415bcfdc94aa0e1f5b2c3d4e5f60718293a4b5c6d7e8f90123456789abcdef"
	brokenID   = "b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0"
	goneID     = "90909090909090909090909090909090909090909090909090909090909090ff"
)

const sampleList = `[
  {"Id":"` + valkeyID + `","Names":["/valkey-valkey-1"],"Image":"valkey/valkey:8-alpine","State":"running"},
  {"Id":"` + postgresID + `","Names":["/postgres-postgres-1"],"Image":"postgres:16","State":"exited"}
]`

// statsReply is a cgroup v2 stats reply whose CPU counters advance by one
// core's worth (on 4 CPUs) per call, so every differenced tick reads 100%.
func statsReply(call int) string {
	return fmt.Sprintf(`{
  "read": "2026-09-21T08:13:00Z",
  "cpu_stats": {"cpu_usage": {"total_usage": %d}, "system_cpu_usage": %d, "online_cpus": 4},
  "memory_stats": {"usage": 3000, "stats": {"inactive_file": 1000}, "limit": 8000},
  "networks": {"eth0": {"rx_bytes": 1700, "tx_bytes": 126}},
  "pids_stats": {"current": 6},
  "blkio_stats": {"io_service_bytes_recursive": [{"op": "read", "value": 20100}, {"op": "write", "value": 0}]}
}`, call*25_000, call*100_000)
}

// inspectReply is postgres's details; its exit code goes up by one per call,
// the way a container that restarts and dies between rounds would look
func inspectReply(call int) string {
	return fmt.Sprintf(`{"State":{"Status":"exited","Running":false,"ExitCode":%d,
  "FinishedAt":"2026-09-21T08:0%d:00.123Z"},"Config":{"Env":["POSTGRES_PASSWORD=secret"]}}`, 136+call, call%10)
}

// What Docker sends for a container that stopped after being listed
const stoppedReply = `{"read":"0001-01-01T00:00:00Z","memory_stats":{},"pids_stats":{}}`

// fakeDocker serves canned replies on a unix socket and records every
// request that reaches it.
type fakeDocker struct {
	mu       sync.Mutex
	seen     []string
	info     string
	list     string
	calls    int
	inspects int
	hang     chan struct{} // stats requests block on this when set
}

func (f *fakeDocker) requests() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.seen...)
}

func startFakeDocker(t *testing.T, info string) (*fakeDocker, string) {
	return startFakeDockerWithList(t, info, sampleList)
}

func startFakeDockerWithList(t *testing.T, info, list string) (*fakeDocker, string) {
	t.Helper()
	socket := filepath.Join(t.TempDir(), "docker.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeDocker{info: info, list: list}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.seen = append(f.seen, r.Method+" "+r.URL.RequestURI())
		f.mu.Unlock()
		switch r.URL.Path {
		case "/info":
			w.Write([]byte(f.info))
		case "/containers/json":
			w.Write([]byte(f.list))
		case "/containers/" + postgresID + "/json":
			f.mu.Lock()
			f.inspects++
			call := f.inspects
			f.mu.Unlock()
			w.Write([]byte(inspectReply(call)))
		case "/containers/" + valkeyID + "/stats":
			if f.hang != nil {
				select {
				case <-f.hang:
				case <-r.Context().Done():
				}
				return
			}
			f.mu.Lock()
			f.calls++
			call := f.calls
			f.mu.Unlock()
			w.Write([]byte(statsReply(call)))
		case "/containers/" + goneID + "/stats":
			w.Write([]byte(stoppedReply))
		case "/containers/" + brokenID + "/stats":
			http.Error(w, `{"message":"cgroup read failed"}`, http.StatusInternalServerError)
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
	if len(ev.Containers) != 2 {
		t.Fatalf("expected 2 containers, got %+v", ev.Containers)
	}

	// Stopped: metadata plus how it last exited, no stats
	pg := ev.Containers[0]
	if pg.ID != postgresID || pg.Name != "postgres-postgres-1" || pg.Image != "postgres:16" || pg.State != "exited" || pg.Error != "" {
		t.Fatalf("expected postgres metadata and no error, got %+v", pg)
	}
	if pg.ExitCode == nil || pg.FinishedAt == "" {
		t.Fatalf("expected exitCode and finishedAt on a stopped container, got %+v", pg)
	}
	if pg.CPUPercent != nil || pg.Memory != nil || pg.Network != nil || pg.BlockIO != nil || pg.PIDs != nil {
		t.Fatalf("expected no stats on a stopped container, got %+v", pg)
	}

	// Running: metadata plus every docker stats figure
	vk := ev.Containers[1]
	if vk.ID != valkeyID || vk.Name != "valkey-valkey-1" || vk.Image != "valkey/valkey:8-alpine" || vk.State != "running" || vk.Error != "" {
		t.Fatalf("expected valkey metadata and no error, got %+v", vk)
	}
	if vk.ExitCode != nil || vk.FinishedAt != "" {
		t.Fatalf("expected no exit details on a running container, got %+v", vk)
	}
	// The startup baseline means CPU is there on the very first event
	if vk.CPUPercent == nil || *vk.CPUPercent != 100 {
		t.Fatalf("expected cpuPercent 100 on the first event, got %v", vk.CPUPercent)
	}
	if vk.Memory == nil || *vk.Memory != (model.DockerMemory{UsedBytes: 2000, LimitBytes: 8000, UsedPercent: 25}) {
		t.Fatalf("expected memory 2000/8000 (25%%), got %+v", vk.Memory)
	}
	if vk.Network == nil || *vk.Network != (model.DockerNetwork{RxBytes: 1700, TxBytes: 126}) {
		t.Fatalf("expected network 1700/126, got %+v", vk.Network)
	}
	if vk.BlockIO == nil || *vk.BlockIO != (model.DockerBlockIO{ReadBytes: 20100, WriteBytes: 0}) {
		t.Fatalf("expected blockIO 20100/0, got %+v", vk.BlockIO)
	}
	if vk.PIDs == nil || *vk.PIDs != 6 {
		t.Fatalf("expected 6 pids, got %v", vk.PIDs)
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

// Across many ticks the collector only lists containers, reads stats for the
// running one and details for the stopped one, and asks /info once; nothing
// else reaches Docker, and a stopped container's stats are never requested
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
		case "GET /containers/" + valkeyID + "/stats?stream=false&one-shot=true":
		case "GET /containers/" + postgresID + "/json":
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

// A container whose stats fail still publishes its metadata, with the error;
// one that stopped after being listed publishes metadata only, no error
func TestCollector_StatsFailureKeepsMetadata(t *testing.T) {
	list := `[
  {"Id":"` + brokenID + `","Names":["/broken"],"Image":"img:1","State":"running"},
  {"Id":"` + goneID + `","Names":["/gone"],"Image":"img:2","State":"running"},
  {"Id":"` + valkeyID + `","Names":["/valkey-valkey-1"],"Image":"valkey/valkey:8-alpine","State":"running"}
]`
	_, socket := startFakeDockerWithList(t, `{"CgroupVersion":"2"}`, list)
	pub := &fakePublisher{}
	run(t, socket, pub, 30*time.Millisecond)

	events := pub.events(t)
	if len(events) == 0 {
		t.Fatal("expected a docker event")
	}
	cs := events[0].Containers
	if len(cs) != 3 {
		t.Fatalf("expected 3 containers, got %+v", cs)
	}

	broken := cs[0]
	if broken.Name != "broken" || !strings.Contains(broken.Error, "cgroup read failed") {
		t.Fatalf("expected broken with docker's error message, got %+v", broken)
	}
	if broken.CPUPercent != nil || broken.Memory != nil || broken.BlockIO != nil || broken.PIDs != nil {
		t.Fatalf("expected no stats on a failed container, got %+v", broken)
	}

	gone := cs[1]
	want := model.DockerContainer{ID: goneID, Name: "gone", Image: "img:2", State: "running"}
	if gone != want {
		t.Fatalf("expected metadata only and no error for a container that stopped, got %+v", gone)
	}

	if cs[2].Memory == nil || cs[2].Error != "" {
		t.Fatalf("expected the healthy container unaffected, got %+v", cs[2])
	}
}

// Paused containers still have stats; every other non-running state is
// metadata only and never asked for stats
func TestHasStats(t *testing.T) {
	for state, want := range map[string]bool{
		"running": true, "paused": true,
		"exited": false, "created": false, "restarting": false, "removing": false, "dead": false,
	} {
		if got := hasStats(state); got != want {
			t.Errorf("hasStats(%q) = %v, want %v", state, got, want)
		}
	}
}

// A stopped container's details are read fresh every round, never cached:
// one that restarted and died again between rounds shows its new exit code
func TestCollector_StoppedDetailsReadEveryRound(t *testing.T) {
	_, socket := startFakeDocker(t, `{"CgroupVersion":"2"}`)
	pub := &fakePublisher{}
	run(t, socket, pub, 60*time.Millisecond)

	events := pub.events(t)
	if len(events) < 2 {
		t.Fatalf("expected several events, got %d", len(events))
	}
	first, second := events[0].Containers[0], events[1].Containers[0]
	if first.ExitCode == nil || second.ExitCode == nil {
		t.Fatalf("expected exit codes on both events, got %+v / %+v", first, second)
	}
	if *first.ExitCode == *second.ExitCode {
		t.Fatalf("expected a fresh exit code each round, got %d twice", *first.ExitCode)
	}
	if first.FinishedAt != "2026-09-21T08:02:00Z" {
		t.Fatalf("expected finishedAt in RFC 3339 UTC, got %q", first.FinishedAt)
	}
}

// A daemon that lists containers but hangs on stats costs one round budget,
// not a request timeout per batch: the event still goes out, with the stuck
// container marked timed out and the stopped one's details intact
func TestCollector_RoundBudgetBoundsAHungDaemon(t *testing.T) {
	f, socket := startFakeDocker(t, `{"CgroupVersion":"2"}`)
	f.hang = make(chan struct{})
	defer close(f.hang)

	pub := &fakePublisher{}
	c := New(socket, "docker-channel", config.IdentityConfig{ServerID: "server-1"}, time.Hour, pub)
	c.roundBudget = 100 * time.Millisecond

	start := time.Now()
	c.collectAndPublish(context.Background())
	if took := time.Since(start); took > time.Second {
		t.Fatalf("expected the round cut off near its 100ms budget, took %v", took)
	}

	events := pub.events(t)
	if len(events) != 1 {
		t.Fatalf("expected one event, got %d", len(events))
	}
	cs := events[0].Containers
	if cs[1].Name != "valkey-valkey-1" || cs[1].Error != "timed out" || cs[1].Memory != nil {
		t.Fatalf("expected valkey published as timed out without stats, got %+v", cs[1])
	}
	if cs[0].Name != "postgres-postgres-1" || cs[0].ExitCode == nil || cs[0].Error != "" {
		t.Fatalf("expected postgres details unaffected by the hung stats, got %+v", cs[0])
	}
}

func TestRoundBudget(t *testing.T) {
	cases := map[time.Duration]time.Duration{
		10 * time.Millisecond: time.Second,      // floor: Docker gets a chance to answer
		5 * time.Second:       5 * time.Second,  // the interval
		30 * time.Second:      10 * time.Second, // capped
	}
	for interval, want := range cases {
		if got := roundBudget(interval); got != want {
			t.Errorf("roundBudget(%v) = %v, want %v", interval, got, want)
		}
	}
}

func newTestCollector(socket string, pub *fakePublisher) *Collector {
	return New(socket, "docker-channel", config.IdentityConfig{ServerID: "server-1"}, 10*time.Second, pub)
}

// A container started since the last tick has no sample to difference; the
// pre-read gives it one, so its first event has cpuPercent instead of
// waiting a round. The pre-read itself publishes nothing.
func TestCollector_PreReadGivesNewContainerCPU(t *testing.T) {
	_, socket := startFakeDocker(t, `{"CgroupVersion":"2"}`)

	without := &fakePublisher{}
	newTestCollector(socket, without).collectAndPublish(context.Background())
	if cpu := without.events(t)[0].Containers[1].CPUPercent; cpu != nil {
		t.Fatalf("expected no cpuPercent without a pre-read, got %v", *cpu)
	}

	pub := &fakePublisher{}
	c := newTestCollector(socket, pub)
	c.preReadCPU(context.Background())
	if n := len(pub.events(t)); n != 0 {
		t.Fatalf("expected the pre-read to publish nothing, got %d events", n)
	}
	c.collectAndPublish(context.Background())

	valkey := pub.events(t)[0].Containers[1]
	if valkey.CPUPercent == nil || *valkey.CPUPercent != 100 {
		t.Fatalf("expected cpuPercent 100 on the first event after a pre-read, got %+v", valkey)
	}
}

// A container restarted between ticks keeps its old sample, whose counters
// are ahead of the new run's; the pre-read replaces it so the tick has a
// usable window
func TestCollector_PreReadReplacesSampleFromBeforeARestart(t *testing.T) {
	_, socket := startFakeDocker(t, `{"CgroupVersion":"2"}`)
	pub := &fakePublisher{}
	c := newTestCollector(socket, pub)
	c.prevStats[valkeyID] = stats.Sample{Running: true, CPUTotal: 1 << 40, SystemTotal: 1, HasSystem: true, CPUs: 4}

	c.preReadCPU(context.Background())
	c.collectAndPublish(context.Background())

	valkey := pub.events(t)[0].Containers[1]
	if valkey.CPUPercent == nil || *valkey.CPUPercent != 100 {
		t.Fatalf("expected cpuPercent 100 after a restart the pre-read caught, got %+v", valkey)
	}
}

// A container whose stored sample still works keeps it: the pre-read's
// reading is dropped, so CPU % stays averaged over the whole interval
func TestCollector_PreReadKeepsAWorkingSample(t *testing.T) {
	f, socket := startFakeDocker(t, `{"CgroupVersion":"2"}`)
	c := newTestCollector(socket, &fakePublisher{})
	c.collectAndPublish(context.Background())
	before := c.prevStats[valkeyID]

	c.preReadCPU(context.Background())

	if f.calls != 2 {
		t.Fatalf("expected the pre-read to read stats, got %d stats calls", f.calls)
	}
	if got := c.prevStats[valkeyID]; got != before {
		t.Fatalf("expected the stored sample kept, got %+v, want %+v", got, before)
	}
	if _, ok := c.prevStats[postgresID]; ok {
		t.Fatal("expected no sample for a stopped container")
	}
}

// Run fires the pre-read a second before each tick: at a 3s interval the
// stats are read at start (prime), 1s (first event), 2s (pre-read) and 3s
// (tick)
func TestCollector_RunPreReadsBeforeEachTick(t *testing.T) {
	if testing.Short() {
		t.Skip("waits a full 3s interval")
	}
	f, socket := startFakeDocker(t, `{"CgroupVersion":"2"}`)
	pub := &fakePublisher{}
	c := New(socket, "docker-channel", config.IdentityConfig{ServerID: "server-1"}, preReadMinInterval, pub)
	ctx, cancel := context.WithTimeout(context.Background(), preReadMinInterval+500*time.Millisecond)
	defer cancel()
	c.Run(ctx)

	if n := len(pub.events(t)); n != 2 {
		t.Fatalf("expected 2 events (startup and one tick), got %d", n)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.calls != 4 {
		t.Fatalf("expected 4 stats reads (prime, first event, pre-read, tick), got %d", f.calls)
	}
}

func TestUsesPreRead(t *testing.T) {
	for interval, want := range map[time.Duration]bool{
		200 * time.Millisecond: false,
		2 * time.Second:        false,
		3 * time.Second:        true,
		10 * time.Second:       true,
	} {
		if got := usesPreRead(interval); got != want {
			t.Errorf("usesPreRead(%v) = %v, want %v", interval, got, want)
		}
	}
}
