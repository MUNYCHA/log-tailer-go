package metrics

import (
	"testing"
	"time"
)

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

const sampleNetDev = `Inter-|   Receive                                                |  Transmit
 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
    lo: 35172512   11167    0    0    0     0          0         0 35172512   11167    0    0    0     0       0          0
  eth0:198034192  136510    0    0    0     0          0       190  1915978   13630    0    0    0     0       0          0
`

func TestParseNetDev(t *testing.T) {
	got, err := parseNetDev([]byte(sampleNetDev))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 interfaces, got %d", len(got))
	}
	// Counter abutting the colon must still parse
	if eth := got["eth0"]; eth.rxBytes != 198034192 || eth.txBytes != 1915978 {
		t.Fatalf("expected eth0 rx 198034192 / tx 1915978, got %+v", eth)
	}
	if lo := got["lo"]; lo.rxBytes != 35172512 || lo.txBytes != 35172512 {
		t.Fatalf("expected lo rx/tx 35172512, got %+v", lo)
	}
}

func TestParseNetDev_TruncatedRow(t *testing.T) {
	if _, err := parseNetDev([]byte("  eth0: 100 1 0 0\n")); err == nil {
		t.Fatal("expected error for a truncated interface row, got nil")
	}
}

func TestParseNetDev_NoInterfaces(t *testing.T) {
	if _, err := parseNetDev([]byte("Inter-|   Receive\n face |bytes\n")); err == nil {
		t.Fatal("expected error when no interface rows are present, got nil")
	}
}

func TestNetRates(t *testing.T) {
	prev := map[string]netCounters{"eth0": {rxBytes: 1000, txBytes: 500}, "eth1": {rxBytes: 0, txBytes: 0}}
	now := map[string]netCounters{"eth0": {rxBytes: 3000, txBytes: 1500}, "eth1": {rxBytes: 2000, txBytes: 1000}}

	// (2000+2000)/2s rx, (1000+1000)/2s tx
	rx, tx, ok := netRates(prev, now, 2*time.Second)
	if !ok {
		t.Fatal("expected a usable window")
	}
	if rx != 2000 || tx != 1000 {
		t.Fatalf("expected rx 2000 / tx 1000 bytes/s, got %f / %f", rx, tx)
	}
}

func TestNetRates_IgnoresInterfaceWithNoBaseline(t *testing.T) {
	prev := map[string]netCounters{"eth0": {rxBytes: 1000, txBytes: 1000}}
	now := map[string]netCounters{"eth0": {rxBytes: 2000, txBytes: 2000}, "eth1": {rxBytes: 9e9, txBytes: 9e9}}

	rx, tx, ok := netRates(prev, now, time.Second)
	if !ok || rx != 1000 || tx != 1000 {
		t.Fatalf("expected eth1's lifetime counters left out, got rx %f tx %f ok=%v", rx, tx, ok)
	}
}

func TestNetRates_CounterWentBackwards(t *testing.T) {
	prev := map[string]netCounters{"eth0": {rxBytes: 5000, txBytes: 5000}}
	now := map[string]netCounters{"eth0": {rxBytes: 100, txBytes: 6000}}
	if _, _, ok := netRates(prev, now, time.Second); ok {
		t.Fatal("expected an unusable window after a counter reset")
	}
}

func TestNetRates_NoSharedInterfaceOrElapsedTime(t *testing.T) {
	prev := map[string]netCounters{"eth0": {rxBytes: 1, txBytes: 1}}
	if _, _, ok := netRates(prev, map[string]netCounters{"eth1": {rxBytes: 2, txBytes: 2}}, time.Second); ok {
		t.Fatal("expected an unusable window with no interface in both samples")
	}
	if _, _, ok := netRates(prev, prev, 0); ok {
		t.Fatal("expected an unusable window with no elapsed time")
	}
}
