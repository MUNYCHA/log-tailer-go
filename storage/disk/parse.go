package disk

import "syscall"

// Sample is the block counts we use from a statfs result.
type Sample struct {
	Blocks          uint64 // total data blocks
	FreeBlocks      uint64 // free blocks, including those reserved for root
	AvailableBlocks uint64 // free blocks usable by unprivileged users
	BlockSize       uint64 // bytes per block, from f_frsize: the unit the three counts are in
}

// Parse pulls the block counts out of a statfs result. The block size is
// f_frsize, not f_bsize: f_bsize is only the preferred I/O size, and on a
// filesystem where the two differ it would scale every size wrongly. df uses
// f_frsize too.
func Parse(stat syscall.Statfs_t) Sample {
	return Sample{
		Blocks:          stat.Blocks,
		FreeBlocks:      stat.Bfree,
		AvailableBlocks: stat.Bavail,
		BlockSize:       uint64(stat.Frsize),
	}
}
