// +build blocksize8

package radix

const (
	// 8MB
	blockSizeShift uint64 = 23
	// 65536 blocks permits having 512 GB of memory blocks
	maxBlockCount = 65536
)
