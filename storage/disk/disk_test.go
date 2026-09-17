package disk

import "testing"

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
}

func TestRead_BadPath(t *testing.T) {
	if _, err := Read("/this/path/does/not/exist/hopefully"); err == nil {
		t.Fatal("expected error for a nonexistent mount path")
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
	if got.UsedPercent != 60 {
		t.Fatalf("expected 60%%, got %f", got.UsedPercent)
	}
}

func TestToBytes_EmptyFilesystem(t *testing.T) {
	if got := ToBytes(Sample{}); got.UsedPercent != 0 {
		t.Fatalf("expected 0%% for a zero-size filesystem, got %f", got.UsedPercent)
	}
}
