package radix

import (
	"sync/atomic"
)

// Stats updates the given map with tree info and operation stats
func (idx *Tree) Stats(stats map[string]interface{}) {
	liveObjects := atomic.LoadInt64(&idx.liveObjects)
	blocksAllocated := int(atomic.LoadUint64(&idx.nextFreeBlock) + 1)
	bytesAllocated := blocksAllocated * int(blockSize)

	stats["objects_approx"] = liveObjects
	stats["block_size"] = blockSize
	stats["blocks_allocated"] = blocksAllocated
	stats["bytes_allocated"] = bytesAllocated
	if liveObjects > 0 {
		stats["bytes_per_key"] = float64(bytesAllocated) / float64(liveObjects)
	}
	stats["locks_per_block"] = locksPerBlock
	stats["allocators"] = len(idx.allocators)
	stats["root_node"] = atomic.LoadUint64(&idx.root)

	stats["current_epoch"] = atomic.LoadUint64(idx.manager.globalEpoch)
}
