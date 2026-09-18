package memory

import "testing"

const sampleMeminfo = `MemTotal:       16080932 kB
MemFree:         1000000 kB
MemAvailable:   13815104 kB
Buffers:           20000 kB
Cached:           900000 kB
SwapTotal:       4194304 kB
SwapFree:        4094304 kB
Shmem:             60000 kB
SReclaimable:     160000 kB
`

// A 3.2–3.13 kernel: every line except MemAvailable
const oldKernelMeminfo = `MemTotal:       1000000 kB
MemFree:         400000 kB
Buffers:          20000 kB
Cached:          300000 kB
SwapTotal:       100000 kB
SwapFree:         75000 kB
Shmem:            50000 kB
SReclaimable:     30000 kB
`

func TestParse(t *testing.T) {
	got, err := Parse([]byte(sampleMeminfo))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := Sample{
		MemTotalKB: 16080932, MemAvailableKB: 13815104,
		MemFreeKB: 1000000, BuffersKB: 20000, CachedKB: 900000, ShmemKB: 60000, SReclaimableKB: 160000,
		SwapTotalKB: 4194304, SwapFreeKB: 4094304,
		HasMemTotal: true, HasMemAvailable: true, HasEstimate: true, HasSwap: true,
	}
	if got != want {
		t.Fatalf("expected %+v, got %+v", want, got)
	}
}

func TestParse_MissingLinesClearFlagsNotError(t *testing.T) {
	got, err := Parse([]byte("MemTotal: 100 kB\nSwapTotal: 0 kB\n"))
	if err != nil {
		t.Fatalf("expected missing lines to be tolerated, got %v", err)
	}
	if !got.HasMemTotal || got.HasMemAvailable || got.HasEstimate || got.HasSwap {
		t.Fatalf("expected only HasMemTotal set, got %+v", got)
	}
}

func TestParse_BadNumberFailsSample(t *testing.T) {
	if _, err := Parse([]byte("MemTotal: abc kB\n")); err == nil {
		t.Fatal("expected an error for a non-numeric value, got nil")
	}
}

func TestMemory_UsesMemAvailableWhenPresent(t *testing.T) {
	s, err := Parse([]byte(sampleMeminfo))
	if err != nil {
		t.Fatal(err)
	}
	got, ok := Memory(s)
	if !ok {
		t.Fatal("expected memory to be available")
	}
	if got.Estimated {
		t.Fatal("expected estimated false when MemAvailable is present")
	}
	if got.TotalBytes != 16080932*1024 {
		t.Fatalf("expected total in bytes, got %d", got.TotalBytes)
	}
	if got.AvailableBytes != 13815104*1024 {
		t.Fatalf("expected MemAvailable in bytes, got %d", got.AvailableBytes)
	}
	if got.UsedBytes != (16080932-13815104)*1024 {
		t.Fatalf("expected used = total - available, got %d", got.UsedBytes)
	}
	if got.UsedBytes+got.AvailableBytes != got.TotalBytes {
		t.Fatal("expected used + available = total")
	}
	want := float64(16080932-13815104) / 16080932 * 100
	if got.UsedPercent != want {
		t.Fatalf("expected usedPercent %f, got %f", want, got.UsedPercent)
	}
}

func TestMemory_EstimatesWithoutMemAvailable(t *testing.T) {
	s, err := Parse([]byte(oldKernelMeminfo))
	if err != nil {
		t.Fatal(err)
	}
	got, ok := Memory(s)
	if !ok {
		t.Fatal("expected an estimate on an old kernel")
	}
	if !got.Estimated {
		t.Fatal("expected estimated true without MemAvailable")
	}
	// 400000 + 20000 + 300000 + 30000 - 50000 = 700000 kB available
	if got.AvailableBytes != 700000*1024 {
		t.Fatalf("expected estimated available 700000 kB, got %d bytes", got.AvailableBytes)
	}
	if got.UsedBytes != 300000*1024 {
		t.Fatalf("expected used 300000 kB, got %d bytes", got.UsedBytes)
	}
	if got.UsedPercent != 30 {
		t.Fatalf("expected 30%%, got %f", got.UsedPercent)
	}
}

func TestMemory_EstimateClampedToTotal(t *testing.T) {
	got, ok := Memory(Sample{
		MemTotalKB: 1000, MemFreeKB: 900, BuffersKB: 100, CachedKB: 100, SReclaimableKB: 100,
		HasMemTotal: true, HasEstimate: true,
	})
	if !ok {
		t.Fatal("expected memory to be available")
	}
	if got.AvailableBytes != got.TotalBytes || got.UsedBytes != 0 || got.UsedPercent != 0 {
		t.Fatalf("expected available clamped to total and used 0, got %+v", got)
	}
}

func TestMemory_ShmemAboveReclaimableDoesNotWrap(t *testing.T) {
	got, ok := Memory(Sample{
		MemTotalKB: 1000, MemFreeKB: 10, ShmemKB: 500,
		HasMemTotal: true, HasEstimate: true,
	})
	if !ok {
		t.Fatal("expected memory to be available")
	}
	if got.AvailableBytes != 0 || got.UsedBytes != 1000*1024 {
		t.Fatalf("expected available 0 and used = total, got %+v", got)
	}
}

func TestMemory_MemAvailableAboveTotalDoesNotWrap(t *testing.T) {
	got, ok := Memory(Sample{MemTotalKB: 100, MemAvailableKB: 200, HasMemTotal: true, HasMemAvailable: true})
	if !ok {
		t.Fatal("expected memory to be available")
	}
	if got.UsedBytes != 0 {
		t.Fatalf("expected used clamped to 0, got %d", got.UsedBytes)
	}
}

func TestMemory_UnavailableCases(t *testing.T) {
	cases := map[string]Sample{
		"no MemTotal":            {MemAvailableKB: 50, HasMemAvailable: true},
		"zero MemTotal":          {HasMemTotal: true, MemAvailableKB: 50, HasMemAvailable: true},
		"no available, no lines": {MemTotalKB: 100, HasMemTotal: true},
		"estimate lines partial": {MemTotalKB: 100, MemFreeKB: 50, HasMemTotal: true},
	}
	for name, s := range cases {
		if _, ok := Memory(s); ok {
			t.Fatalf("%s: expected memory to be omitted", name)
		}
	}
}

func TestSwap(t *testing.T) {
	got, ok := Swap(Sample{SwapTotalKB: 4194304, SwapFreeKB: 4094304, HasSwap: true})
	if !ok {
		t.Fatal("expected swap to be available")
	}
	if got.TotalBytes != 4194304*1024 {
		t.Fatalf("expected total in bytes, got %d", got.TotalBytes)
	}
	if got.UsedBytes != (4194304-4094304)*1024 {
		t.Fatalf("expected used = total - free, got %d", got.UsedBytes)
	}
	want := float64(4194304-4094304) / 4194304 * 100
	if got.UsedPercent != want {
		t.Fatalf("expected usedPercent %f, got %f", want, got.UsedPercent)
	}
}

func TestSwap_NoSwapIsAllZeros(t *testing.T) {
	got, ok := Swap(Sample{HasSwap: true})
	if !ok {
		t.Fatal("expected a server without swap to still report the group")
	}
	if got != (SwapUsage{}) {
		t.Fatalf("expected all zeros, got %+v", got)
	}
}

func TestSwap_FreeExceedsTotal(t *testing.T) {
	got, ok := Swap(Sample{SwapTotalKB: 10, SwapFreeKB: 20, HasSwap: true})
	if !ok {
		t.Fatal("expected swap to be available")
	}
	if got.UsedBytes != 0 || got.UsedPercent != 0 {
		t.Fatalf("expected clamped used of 0, got %+v", got)
	}
}

func TestSwap_ReportedWhenMemoryIsNot(t *testing.T) {
	s, err := Parse([]byte("MemTotal: 1000 kB\nSwapTotal: 100 kB\nSwapFree: 75 kB\n"))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := Memory(s); ok {
		t.Fatal("expected memory omitted without MemAvailable or estimate lines")
	}
	got, ok := Swap(s)
	if !ok {
		t.Fatal("expected swap reported on its own")
	}
	if got.UsedPercent != 25 {
		t.Fatalf("expected 25%%, got %f", got.UsedPercent)
	}
}
