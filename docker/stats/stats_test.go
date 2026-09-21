package stats

import (
	"math"
	"testing"
)

// Trimmed from a real Docker 29.6 reply on a cgroup v2 host (redis-redis-1)
const sampleV2 = `{
  "read": "2026-09-21T08:13:00.927973584Z",
  "cpu_stats": {
    "cpu_usage": {"total_usage": 28561240000},
    "system_cpu_usage": 475748040000000,
    "online_cpus": 24
  },
  "memory_stats": {
    "usage": 26898432,
    "stats": {"anon": 6303744, "file": 19206144, "inactive_file": 16785408},
    "limit": 16466882560
  },
  "networks": {"eth0": {"rx_bytes": 2998666, "tx_bytes": 340176}},
  "pids_stats": {"current": 22, "limit": 18446744073709551615},
  "blkio_stats": {
    "io_service_bytes_recursive": [
      {"major": 8, "minor": 64, "op": "read", "value": 34746368},
      {"major": 8, "minor": 64, "op": "write", "value": 0}
    ]
  }
}`

// The cgroup v1 shape, per Docker's API docs: per-CPU usage list, a memory
// cache named total_inactive_file next to the non-total one, capitalised
// block I/O ops with a Total row that must not be double counted, and two
// networks
const sampleV1 = `{
  "read": "2026-09-21T08:13:00Z",
  "cpu_stats": {
    "cpu_usage": {"total_usage": 1000000000, "percpu_usage": [500000000, 500000000]},
    "system_cpu_usage": 90000000000
  },
  "memory_stats": {
    "usage": 10000000,
    "stats": {"inactive_file": 1000, "total_inactive_file": 4000000, "cache": 6000000},
    "limit": 20000000
  },
  "networks": {
    "eth0": {"rx_bytes": 100, "tx_bytes": 10},
    "eth1": {"rx_bytes": 200, "tx_bytes": 20}
  },
  "pids_stats": {"current": 3},
  "blkio_stats": {
    "io_service_bytes_recursive": [
      {"major": 8, "minor": 0, "op": "Read", "value": 4096},
      {"major": 8, "minor": 0, "op": "Write", "value": 8192},
      {"major": 8, "minor": 0, "op": "Sync", "value": 12288},
      {"major": 8, "minor": 0, "op": "Async", "value": 0},
      {"major": 8, "minor": 0, "op": "Total", "value": 12288}
    ]
  }
}`

// What Docker sends for a container that stopped between the list and the
// stats request: a zero read time and empty groups, not an error
const sampleStopped = `{
  "read": "0001-01-01T00:00:00Z",
  "cpu_stats": {"cpu_usage": {"total_usage": 0}},
  "memory_stats": {},
  "pids_stats": {},
  "blkio_stats": {"io_service_bytes_recursive": null}
}`

// Host networking: Docker sends no networks key at all
const sampleHostNetwork = `{
  "read": "2026-09-21T08:13:00Z",
  "cpu_stats": {"cpu_usage": {"total_usage": 5}, "system_cpu_usage": 10, "online_cpus": 4},
  "memory_stats": {"usage": 100, "stats": {}, "limit": 1000},
  "pids_stats": {"current": 1},
  "blkio_stats": {"io_service_bytes_recursive": null}
}`

func parse(t *testing.T, data string) Sample {
	t.Helper()
	s, err := Parse([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestParse_CgroupV2(t *testing.T) {
	s := parse(t, sampleV2)
	want := Sample{
		Running:     true,
		CPUTotal:    28561240000,
		SystemTotal: 475748040000000,
		HasSystem:   true,
		CPUs:        24,
		HasMemory:   true,
		MemUsage:    26898432,
		MemLimit:    16466882560,
		MemCache:    16785408,
		HasNetwork:  true,
		RxBytes:     2998666,
		TxBytes:     340176,
		ReadBytes:   34746368,
		WriteBytes:  0,
		HasPIDs:     true,
		PIDs:        22,
	}
	if s != want {
		t.Fatalf("expected\n%+v\ngot\n%+v", want, s)
	}
}

func TestParse_CgroupV1(t *testing.T) {
	s := parse(t, sampleV1)
	if s.CPUs != 2 {
		t.Fatalf("expected CPUs from the per-CPU list when online_cpus is absent, got %d", s.CPUs)
	}
	if s.MemCache != 4000000 {
		t.Fatalf("expected total_inactive_file preferred over inactive_file, got %d", s.MemCache)
	}
	if s.RxBytes != 300 || s.TxBytes != 30 {
		t.Fatalf("expected networks summed to 300/30, got %d/%d", s.RxBytes, s.TxBytes)
	}
	if s.ReadBytes != 4096 || s.WriteBytes != 8192 {
		t.Fatalf("expected Read/Write only, not Sync or Total, got %d/%d", s.ReadBytes, s.WriteBytes)
	}
	if !s.HasPIDs || s.PIDs != 3 {
		t.Fatalf("expected 3 pids, got %+v", s)
	}
}

func TestParse_StoppedContainer(t *testing.T) {
	s := parse(t, sampleStopped)
	if s != (Sample{}) {
		t.Fatalf("expected an empty not-running sample, got %+v", s)
	}
}

func TestParse_HostNetworkHasNoNetwork(t *testing.T) {
	s := parse(t, sampleHostNetwork)
	if s.HasNetwork {
		t.Fatalf("expected no network group without a networks key, got %+v", s)
	}
	if !s.Running || !s.HasMemory || !s.HasPIDs {
		t.Fatalf("expected the rest of the sample intact, got %+v", s)
	}
}

// Very old Docker without pids_stats.current
func TestParse_NoPIDs(t *testing.T) {
	s := parse(t, `{"read":"2026-09-21T08:13:00Z","pids_stats":{}}`)
	if s.HasPIDs || s.HasMemory || s.HasSystem {
		t.Fatalf("expected pids, memory and system CPU absent, got %+v", s)
	}
}

func TestParse_Invalid(t *testing.T) {
	if _, err := Parse([]byte(`not json`)); err == nil {
		t.Fatal("expected an error for a reply that is not json")
	}
}

func near(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

func TestCPUPercent(t *testing.T) {
	prev := Sample{CPUTotal: 1_000, SystemTotal: 100_000, HasSystem: true, CPUs: 4}

	// The container used 1/4 of all host CPU time on a 4-CPU host: one core
	now := Sample{CPUTotal: 26_000, SystemTotal: 200_000, HasSystem: true, CPUs: 4}
	got, ok := CPUPercent(prev, now)
	if !ok || !near(got, 100) {
		t.Fatalf("expected 100%% (one full core), got %v, %v", got, ok)
	}

	// Two full cores reads above 100, like docker stats
	now.CPUTotal = 51_000
	if got, ok := CPUPercent(prev, now); !ok || !near(got, 200) {
		t.Fatalf("expected 200%%, got %v, %v", got, ok)
	}

	// Idle is a real 0, not omitted
	now.CPUTotal = prev.CPUTotal
	if got, ok := CPUPercent(prev, now); !ok || got != 0 {
		t.Fatalf("expected 0%% for an idle container, got %v, %v", got, ok)
	}
}

func TestCPUPercent_UnusableWindows(t *testing.T) {
	prev := Sample{CPUTotal: 5_000, SystemTotal: 100_000, HasSystem: true, CPUs: 4}
	cases := map[string]struct{ prev, now Sample }{
		"no previous sample":   {Sample{}, prev},
		"no host cpu time":     {prev, Sample{CPUTotal: 6_000, CPUs: 4}},
		"host clock unchanged": {prev, Sample{CPUTotal: 6_000, SystemTotal: 100_000, HasSystem: true, CPUs: 4}},
		"container restarted":  {prev, Sample{CPUTotal: 10, SystemTotal: 200_000, HasSystem: true, CPUs: 4}},
		"no cpu count":         {prev, Sample{CPUTotal: 6_000, SystemTotal: 200_000, HasSystem: true}},
	}
	for name, tc := range cases {
		if got, ok := CPUPercent(tc.prev, tc.now); ok {
			t.Errorf("%s: expected omitted, got %v", name, got)
		}
	}
}

func TestMemoryUsage(t *testing.T) {
	// Matches docker stats for redis-redis-1: 26898432 - 16785408 = 10113024
	// bytes, the 9.645MiB it shows
	got, ok := MemoryUsage(parse(t, sampleV2))
	if !ok {
		t.Fatal("expected memory present")
	}
	if got.UsedBytes != 10113024 || got.LimitBytes != 16466882560 {
		t.Fatalf("expected 10113024 / 16466882560, got %+v", got)
	}
	if !near(got.UsedPercent, 10113024.0/16466882560.0*100) {
		t.Fatalf("expected used/limit percent, got %v", got.UsedPercent)
	}

	got, _ = MemoryUsage(parse(t, sampleV1))
	if got.UsedBytes != 6000000 || !near(got.UsedPercent, 30) {
		t.Fatalf("expected v1 used 6000000 (30%%), got %+v", got)
	}
}

func TestMemoryUsage_CacheAboveUsageFallsBackToUsage(t *testing.T) {
	got, ok := MemoryUsage(Sample{HasMemory: true, MemUsage: 100, MemCache: 500, MemLimit: 1000})
	if !ok || got.UsedBytes != 100 {
		t.Fatalf("expected raw usage 100 rather than an underflow, got %+v", got)
	}
}

func TestMemoryUsage_Absent(t *testing.T) {
	if _, ok := MemoryUsage(Sample{Running: true}); ok {
		t.Fatal("expected memory omitted when Docker sent none")
	}
}
