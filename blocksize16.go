// +build blocksize16

package radix

const (
	// 16MB
	blockSizeShift uint64 = 24
	// 65536 blocks permits having 1 TB of memory blocks
	maxBlockCount = 65536
)
