package disk

import (
	"syscall"
	"testing"
)

func TestReadAndToBytes_RootFilesystem(t *testing.T) {
	stat, err := Read("/")
	if err != nil {
		t.Fatalf("unexpected error statting /: %v", err)
	}
	usage := ToBytes(Parse(stat))
	if usage.TotalBytes == 0 {
		t.Fatal("expected TotalBytes > 0 for /")
	}
	if usage.UsedPercent < 0 || usage.UsedPercent > 100 {
		t.Fatalf("expected UsedPercent in [0,100], got %f", usage.UsedPercent)
	}
	if usage.UsedBytes+usage.FreeBytes+usage.ReservedBytes != usage.TotalBytes {
		t.Fatalf("expected used + free + reserved = total, got %+v", usage)
	}
}

func TestRead_BadPath(t *testing.T) {
	if _, err := Read("/this/path/does/not/exist/hopefully"); err == nil {
		t.Fatal("expected error for a nonexistent mount path")
	}
}

// Guards the choice of f_frsize: the block counts are in f_frsize units, and
// f_bsize is only an I/O hint that can differ from it
func TestParse_UsesFrsizeNotBsize(t *testing.T) {
	got := Parse(syscall.Statfs_t{Blocks: 1000, Bfree: 400, Bavail: 300, Bsize: 1048576, Frsize: 4096})
	if got.BlockSize != 4096 {
		t.Fatalf("expected block size from Frsize (4096), got %d", got.BlockSize)
	}
	want := Sample{Blocks: 1000, FreeBlocks: 400, AvailableBlocks: 300, BlockSize: 4096}
	if got != want {
		t.Fatalf("expected %+v, got %+v", want, got)
	}
}

func TestToBytes(t *testing.T) {
	// 1000 blocks, 400 free of which 300 are usable by non-root, 4 KiB each
	got := ToBytes(Sample{Blocks: 1000, FreeBlocks: 400, AvailableBlocks: 300, BlockSize: 4096})
	if got.TotalBytes != 1000*4096 {
		t.Fatalf("expected total %d, got %d", 1000*4096, got.TotalBytes)
	}
	if got.UsedBytes != 600*4096 {
		t.Fatalf("expected used = total - all free blocks (%d), got %d", 600*4096, got.UsedBytes)
	}
	if got.FreeBytes != 300*4096 {
		t.Fatalf("expected free = non-root available blocks (%d), got %d", 300*4096, got.FreeBytes)
	}
	if got.ReservedBytes != 100*4096 {
		t.Fatalf("expected reserved = free - available blocks (%d), got %d", 100*4096, got.ReservedBytes)
	}
	if got.UsedBytes+got.FreeBytes+got.ReservedBytes != got.TotalBytes {
		t.Fatal("expected used + free + reserved = total")
	}
	// df formula: 600 / (600 + 300) = 66.67%, not 600 / 1000 = 60%
	want := float64(600) / float64(900) * 100
	if got.UsedPercent != want {
		t.Fatalf("expected df usedPercent %f, got %f", want, got.UsedPercent)
	}
}

func TestToBytes_FullForAppsIs100Percent(t *testing.T) {
	// Only root-reserved blocks left: apps can't write, so df reports 100%
	got := ToBytes(Sample{Blocks: 1000, FreeBlocks: 50, AvailableBlocks: 0, BlockSize: 4096})
	if got.UsedPercent != 100 {
		t.Fatalf("expected 100%% when nothing is available to apps, got %f", got.UsedPercent)
	}
	if got.ReservedBytes != 50*4096 {
		t.Fatalf("expected reserved 50 blocks, got %d", got.ReservedBytes)
	}
}

func TestToBytes_EmptyFilesystem(t *testing.T) {
	if got := ToBytes(Sample{}); got.UsedPercent != 0 {
		t.Fatalf("expected 0%% for a zero-size filesystem, got %f", got.UsedPercent)
	}
}

func TestToBytes_AvailableAboveFreeDoesNotWrap(t *testing.T) {
	got := ToBytes(Sample{Blocks: 1000, FreeBlocks: 100, AvailableBlocks: 120, BlockSize: 4096})
	if got.ReservedBytes != 0 {
		t.Fatalf("expected reserved clamped to 0, got %d", got.ReservedBytes)
	}
}

func TestToBytes_FreeAboveTotalDoesNotWrap(t *testing.T) {
	got := ToBytes(Sample{Blocks: 100, FreeBlocks: 120, AvailableBlocks: 120, BlockSize: 4096})
	if got.UsedBytes != 0 {
		t.Fatalf("expected used clamped to 0, got %d", got.UsedBytes)
	}
	if got.UsedPercent != 0 {
		t.Fatalf("expected 0%%, got %f", got.UsedPercent)
	}
}

func TestTotal_PercentFromSumsNotAverage(t *testing.T) {
	// 100 GB disk 90% used, 1000 GB disk 10% used (no reserved space)
	const gb = 1000 * 1000 * 1000
	got := Total([]Usage{
		{TotalBytes: 100 * gb, UsedBytes: 90 * gb, FreeBytes: 10 * gb, UsedPercent: 90},
		{TotalBytes: 1000 * gb, UsedBytes: 100 * gb, FreeBytes: 900 * gb, UsedPercent: 10},
	})
	if got.TotalBytes != 1100*gb || got.UsedBytes != 190*gb || got.FreeBytes != 910*gb {
		t.Fatalf("expected summed bytes, got %+v", got)
	}
	want := float64(190) / float64(1100) * 100
	if got.UsedPercent != want {
		t.Fatalf("expected %f from sums (not the 50%% average), got %f", want, got.UsedPercent)
	}
}

func TestTotal_KeepsTheSumsAddingUp(t *testing.T) {
	a := ToBytes(Sample{Blocks: 1000, FreeBlocks: 400, AvailableBlocks: 300, BlockSize: 4096})
	b := ToBytes(Sample{Blocks: 5000, FreeBlocks: 1000, AvailableBlocks: 750, BlockSize: 1024})
	got := Total([]Usage{a, b})
	if got.UsedBytes+got.FreeBytes+got.ReservedBytes != got.TotalBytes {
		t.Fatalf("expected used + free + reserved = total, got %+v", got)
	}
	if got.ReservedBytes != a.ReservedBytes+b.ReservedBytes {
		t.Fatalf("expected reserved summed, got %d", got.ReservedBytes)
	}
}

func TestTotal_Empty(t *testing.T) {
	if got := Total(nil); got != (Usage{}) {
		t.Fatalf("expected zero usage, got %+v", got)
	}
}
