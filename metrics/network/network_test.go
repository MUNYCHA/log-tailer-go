package network

import (
	"testing"
	"time"
)

const sampleNetDev = `Inter-|   Receive                                                |  Transmit
 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
    lo: 35172512   11167    0    0    0     0          0         0 35172512   11167    0    0    0     0       0          0
  eth0:198034192  136510    0    0    0     0          0       190  1915978   13630    0    0    0     0       0          0
`

func TestParse(t *testing.T) {
	got, err := Parse([]byte(sampleNetDev))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 interfaces, got %d", len(got))
	}
	// Counter abutting the colon must still parse
	if eth := got["eth0"]; eth.RxBytes != 198034192 || eth.TxBytes != 1915978 {
		t.Fatalf("expected eth0 rx 198034192 / tx 1915978, got %+v", eth)
	}
	if lo := got["lo"]; lo.RxBytes != 35172512 || lo.TxBytes != 35172512 {
		t.Fatalf("expected lo rx/tx 35172512, got %+v", lo)
	}
}

func TestParse_TruncatedRow(t *testing.T) {
	if _, err := Parse([]byte("  eth0: 100 1 0 0\n")); err == nil {
		t.Fatal("expected error for a truncated interface row, got nil")
	}
}

func TestParse_NoInterfaces(t *testing.T) {
	if _, err := Parse([]byte("Inter-|   Receive\n face |bytes\n")); err == nil {
		t.Fatal("expected error when no interface rows are present, got nil")
	}
}

func TestKeepPhysical_DropsLoopback(t *testing.T) {
	s := Sample{"lo": {RxBytes: 1, TxBytes: 1}}
	KeepPhysical(s)
	if _, kept := s["lo"]; kept {
		t.Fatal("expected loopback to be dropped, it has no device link")
	}
}

func TestRates(t *testing.T) {
	prev := Sample{"eth0": {RxBytes: 1000, TxBytes: 500}, "eth1": {RxBytes: 0, TxBytes: 0}}
	now := Sample{"eth0": {RxBytes: 3000, TxBytes: 1500}, "eth1": {RxBytes: 2000, TxBytes: 1000}}

	// (2000+2000)/2s rx, (1000+1000)/2s tx
	rx, tx, ok := Rates(prev, now, 2*time.Second)
	if !ok {
		t.Fatal("expected a usable window")
	}
	if rx != 2000 || tx != 1000 {
		t.Fatalf("expected rx 2000 / tx 1000 bytes/s, got %f / %f", rx, tx)
	}
}

func TestRates_IgnoresInterfaceWithNoBaseline(t *testing.T) {
	prev := Sample{"eth0": {RxBytes: 1000, TxBytes: 1000}}
	now := Sample{"eth0": {RxBytes: 2000, TxBytes: 2000}, "eth1": {RxBytes: 9e9, TxBytes: 9e9}}

	rx, tx, ok := Rates(prev, now, time.Second)
	if !ok || rx != 1000 || tx != 1000 {
		t.Fatalf("expected eth1's lifetime counters left out, got rx %f tx %f ok=%v", rx, tx, ok)
	}
}

func TestRates_CounterWentBackwards(t *testing.T) {
	prev := Sample{"eth0": {RxBytes: 5000, TxBytes: 5000}}
	now := Sample{"eth0": {RxBytes: 100, TxBytes: 6000}}
	if _, _, ok := Rates(prev, now, time.Second); ok {
		t.Fatal("expected an unusable window after a counter reset")
	}
}

func TestRates_NoSharedInterfaceOrElapsedTime(t *testing.T) {
	prev := Sample{"eth0": {RxBytes: 1, TxBytes: 1}}
	if _, _, ok := Rates(prev, Sample{"eth1": {RxBytes: 2, TxBytes: 2}}, time.Second); ok {
		t.Fatal("expected an unusable window with no interface in both samples")
	}
	if _, _, ok := Rates(prev, prev, 0); ok {
		t.Fatal("expected an unusable window with no elapsed time")
	}
}
