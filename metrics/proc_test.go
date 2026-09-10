package metrics

import "testing"

func TestParseLoadavg(t *testing.T) {
	got, err := parseLoadavg([]byte("0.02 0.04 0.05 1/791 12163\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.one != 0.02 || got.five != 0.04 || got.fifteen != 0.05 {
		t.Fatalf("expected 0.02/0.04/0.05, got %v", got)
	}
}

func TestParseLoadavg_TooFewFields(t *testing.T) {
	if _, err := parseLoadavg([]byte("0.02 0.04\n")); err == nil {
		t.Fatal("expected error for a truncated loadavg line, got nil")
	}
}

func TestParseLoadavg_NotANumber(t *testing.T) {
	if _, err := parseLoadavg([]byte("x y z 1/791 12163\n")); err == nil {
		t.Fatal("expected error for non-numeric loadavg, got nil")
	}
}

const sampleMeminfo = `MemTotal:       16080932 kB
MemFree:         1000000 kB
MemAvailable:   13815104 kB
SwapTotal:       4194304 kB
SwapFree:        4094304 kB
`

func TestParseMeminfo(t *testing.T) {
	got, err := parseMeminfo([]byte(sampleMeminfo))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.totalBytes != 16080932*1024 {
		t.Fatalf("expected memTotal in bytes, got %d", got.totalBytes)
	}
	// MemAvailable, not MemFree
	if got.availableBytes != 13815104*1024 {
		t.Fatalf("expected memAvailable in bytes, got %d", got.availableBytes)
	}
	if got.swapTotalBytes != 4194304*1024 {
		t.Fatalf("expected swapTotal in bytes, got %d", got.swapTotalBytes)
	}
	if got.swapUsedBytes != (4194304-4094304)*1024 {
		t.Fatalf("expected swapUsed = total - free, got %d", got.swapUsedBytes)
	}
}

func TestParseMeminfo_MissingMemAvailable(t *testing.T) {
	_, err := parseMeminfo([]byte("MemTotal: 100 kB\nSwapTotal: 0 kB\nSwapFree: 0 kB\n"))
	if err == nil {
		t.Fatal("expected the group to fail when MemAvailable is absent, got nil")
	}
}

func TestParseMeminfo_SwapFreeExceedsTotal(t *testing.T) {
	got, err := parseMeminfo([]byte("MemTotal: 100 kB\nMemAvailable: 50 kB\nSwapTotal: 10 kB\nSwapFree: 20 kB\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.swapUsedBytes != 0 {
		t.Fatalf("expected clamped swapUsed of 0, got %d", got.swapUsedBytes)
	}
}

func TestParseStat_SumsAggregateLineAndCountsIowaitAsIdle(t *testing.T) {
	got, err := parseStat([]byte("cpu  10 20 30 40 50 0 0 0 0 0\ncpu0 1 2 3 4 5 0 0 0 0 0\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.total != 150 {
		t.Fatalf("expected total 150, got %d", got.total)
	}
	if got.idle != 90 {
		t.Fatalf("expected idle 90 (idle 40 + iowait 50), got %d", got.idle)
	}
}

func TestParseStat_NoCPULine(t *testing.T) {
	if _, err := parseStat([]byte("intr 12345\nctxt 678\n")); err == nil {
		t.Fatal("expected error when no aggregate cpu line is present, got nil")
	}
}

func TestCPUPercent(t *testing.T) {
	// 100 jiffies elapsed, 25 of them idle -> 75% busy
	pct, ok := cpuPercent(cpuSample{total: 1000, idle: 800}, cpuSample{total: 1100, idle: 825})
	if !ok {
		t.Fatal("expected a usable window")
	}
	if pct != 75 {
		t.Fatalf("expected 75, got %f", pct)
	}
}

func TestCPUPercent_FullyIdle(t *testing.T) {
	pct, ok := cpuPercent(cpuSample{total: 1000, idle: 800}, cpuSample{total: 1100, idle: 900})
	if !ok || pct != 0 {
		t.Fatalf("expected a usable 0%% window, got %f ok=%v", pct, ok)
	}
}

func TestCPUPercent_NoElapsedTime(t *testing.T) {
	if _, ok := cpuPercent(cpuSample{total: 1000, idle: 800}, cpuSample{total: 1000, idle: 800}); ok {
		t.Fatal("expected an unusable window when no jiffies elapsed")
	}
}

func TestCPUPercent_CountersWentBackwards(t *testing.T) {
	// A reboot between ticks resets /proc/stat; better to omit than to invent
	if _, ok := cpuPercent(cpuSample{total: 5000, idle: 4000}, cpuSample{total: 100, idle: 80}); ok {
		t.Fatal("expected an unusable window after a counter reset")
	}
}

func TestCPUPercent_IdleGrewFasterThanTotal(t *testing.T) {
	if _, ok := cpuPercent(cpuSample{total: 1000, idle: 800}, cpuSample{total: 1010, idle: 900}); ok {
		t.Fatal("expected an unusable window when idle outpaces total")
	}
}
