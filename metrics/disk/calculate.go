package disk

// Usage is a mount's size as published, in bytes.
type Usage struct {
	TotalBytes  uint64
	UsedBytes   uint64
	FreeBytes   uint64
	UsedPercent float64
}

// ToBytes converts block counts to bytes. Free is what an unprivileged user
// can still write, while used counts everything not free (root-reserved
// blocks included), so used + free can be less than total.
func ToBytes(s Sample) Usage {
	total := s.Blocks * s.BlockSize
	free := s.AvailableBlocks * s.BlockSize
	used := total - s.FreeBlocks*s.BlockSize

	var usedPercent float64
	if total > 0 {
		usedPercent = float64(used) / float64(total) * 100
	}

	return Usage{
		TotalBytes:  total,
		UsedBytes:   used,
		FreeBytes:   free,
		UsedPercent: usedPercent,
	}
}
