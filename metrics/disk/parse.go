package disk

import "syscall"

// Sample is the block counts we use from a statfs result.
type Sample struct {
	Blocks          uint64 // total data blocks
	FreeBlocks      uint64 // free blocks, including those reserved for root
	AvailableBlocks uint64 // free blocks usable by unprivileged users
	BlockSize       uint64 // bytes per block
}

// Parse pulls the block counts out of a statfs result.
func Parse(stat syscall.Statfs_t) Sample {
	return Sample{
		Blocks:          stat.Blocks,
		FreeBlocks:      stat.Bfree,
		AvailableBlocks: stat.Bavail,
		BlockSize:       uint64(stat.Bsize),
	}
}
