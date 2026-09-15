package memory

import "testing"

const sampleMeminfo = `MemTotal:       16080932 kB
MemFree:         1000000 kB
MemAvailable:   13815104 kB
SwapTotal:       4194304 kB
SwapFree:        4094304 kB
`

func TestParse(t *testing.T) {
	got, err := Parse([]byte(sampleMeminfo))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// MemAvailable, not MemFree
	want := Sample{MemTotalKB: 16080932, MemAvailableKB: 13815104, SwapTotalKB: 4194304, SwapFreeKB: 4094304}
	if got != want {
		t.Fatalf("expected %+v, got %+v", want, got)
	}
}

func TestParse_MissingMemAvailable(t *testing.T) {
	_, err := Parse([]byte("MemTotal: 100 kB\nSwapTotal: 0 kB\nSwapFree: 0 kB\n"))
	if err == nil {
		t.Fatal("expected the sample to fail when MemAvailable is absent, got nil")
	}
}

func TestToBytes(t *testing.T) {
	got := ToBytes(Sample{MemTotalKB: 16080932, MemAvailableKB: 13815104, SwapTotalKB: 4194304, SwapFreeKB: 4094304})
	if got.TotalBytes != 16080932*1024 {
		t.Fatalf("expected memTotal in bytes, got %d", got.TotalBytes)
	}
	if got.AvailableBytes != 13815104*1024 {
		t.Fatalf("expected memAvailable in bytes, got %d", got.AvailableBytes)
	}
	if got.SwapTotalBytes != 4194304*1024 {
		t.Fatalf("expected swapTotal in bytes, got %d", got.SwapTotalBytes)
	}
	if got.SwapUsedBytes != (4194304-4094304)*1024 {
		t.Fatalf("expected swapUsed = total - free, got %d", got.SwapUsedBytes)
	}
}

func TestToBytes_SwapFreeExceedsTotal(t *testing.T) {
	got := ToBytes(Sample{MemTotalKB: 100, MemAvailableKB: 50, SwapTotalKB: 10, SwapFreeKB: 20})
	if got.SwapUsedBytes != 0 {
		t.Fatalf("expected clamped swapUsed of 0, got %d", got.SwapUsedBytes)
	}
}
