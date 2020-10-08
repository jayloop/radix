// +build !blocksize8
// +build !blocksize16

package radix

const (
	// 2 MiB
	blockSizeShift uint64 = 21
	// 128 GiB
	maxBlockCount = 65536
)
