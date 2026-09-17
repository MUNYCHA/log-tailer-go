package disk

// Usage is a filesystem's size as published, in bytes.
type Usage struct {
	TotalBytes    uint64
	UsedBytes     uint64
	FreeBytes     uint64
	ReservedBytes uint64
	UsedPercent   float64
}

// ToBytes converts block counts to bytes, matching df -B1:
//
//	total    = blocks × size
//	used     = (blocks - free blocks) × size
//	free     = available blocks × size     (what a non-root user can write)
//	reserved = (free blocks - available blocks) × size   (root only)
//
// so used + free + reserved = total. UsedPercent is df's Use%,
// used / (used + free) × 100, so 100% means a normal app can no longer write
// even though root-reserved space remains.
//
// Every subtraction is guarded: a filesystem reporting more free than total,
// or more available than free, gets 0 instead of an underflowed uint64.
func ToBytes(s Sample) Usage {
	var usedBlocks uint64
	if s.Blocks > s.FreeBlocks {
		usedBlocks = s.Blocks - s.FreeBlocks
	}
	var reservedBlocks uint64
	if s.FreeBlocks > s.AvailableBlocks {
		reservedBlocks = s.FreeBlocks - s.AvailableBlocks
	}

	used := usedBlocks * s.BlockSize
	free := s.AvailableBlocks * s.BlockSize

	var usedPercent float64
	if used+free > 0 {
		usedPercent = float64(used) / float64(used+free) * 100
	}

	return Usage{
		TotalBytes:    s.Blocks * s.BlockSize,
		UsedBytes:     used,
		FreeBytes:     free,
		ReservedBytes: reservedBlocks * s.BlockSize,
		UsedPercent:   usedPercent,
	}
}
