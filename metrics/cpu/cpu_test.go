package cpu

import "testing"

func TestParse_SumsAggregateLineAndCountsIowaitAsIdle(t *testing.T) {
	got, err := Parse([]byte("cpu  10 20 30 40 50 0 0 0 0 0\ncpu0 1 2 3 4 5 0 0 0 0 0\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Total != 150 {
		t.Fatalf("expected total 150, got %d", got.Total)
	}
	if got.Idle != 90 {
		t.Fatalf("expected idle 90 (idle 40 + iowait 50), got %d", got.Idle)
	}
}

func TestParse_NoCPULine(t *testing.T) {
	if _, err := Parse([]byte("intr 12345\nctxt 678\n")); err == nil {
		t.Fatal("expected error when no aggregate cpu line is present, got nil")
	}
}

func TestPercent(t *testing.T) {
	// 100 jiffies elapsed, 25 of them idle -> 75% busy
	pct, ok := Percent(Sample{Total: 1000, Idle: 800}, Sample{Total: 1100, Idle: 825})
	if !ok {
		t.Fatal("expected a usable window")
	}
	if pct != 75 {
		t.Fatalf("expected 75, got %f", pct)
	}
}

func TestPercent_FullyIdle(t *testing.T) {
	pct, ok := Percent(Sample{Total: 1000, Idle: 800}, Sample{Total: 1100, Idle: 900})
	if !ok || pct != 0 {
		t.Fatalf("expected a usable 0%% window, got %f ok=%v", pct, ok)
	}
}

func TestPercent_NoElapsedTime(t *testing.T) {
	if _, ok := Percent(Sample{Total: 1000, Idle: 800}, Sample{Total: 1000, Idle: 800}); ok {
		t.Fatal("expected an unusable window when no jiffies elapsed")
	}
}

func TestPercent_CountersWentBackwards(t *testing.T) {
	// A reboot between ticks resets /proc/stat; better to omit than to invent
	if _, ok := Percent(Sample{Total: 5000, Idle: 4000}, Sample{Total: 100, Idle: 80}); ok {
		t.Fatal("expected an unusable window after a counter reset")
	}
}

func TestPercent_IdleGrewFasterThanTotal(t *testing.T) {
	if _, ok := Percent(Sample{Total: 1000, Idle: 800}, Sample{Total: 1010, Idle: 900}); ok {
		t.Fatal("expected an unusable window when idle outpaces total")
	}
}
