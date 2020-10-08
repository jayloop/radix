package radix

import (
	"sync/atomic"
)

// global operation stats
// TODO: move these to the tree?
var (
	nodesAllocated uint64
	nodesReleased  uint64
	nodesReused    uint64

	opNodePrefixMiss    uint64
	opNodeLeafInsert    uint64
	opNode256leafInsert uint64
	opNodeLeafUpdate    uint64
	updateRestarts      uint64

	opDeleteLeaf256   uint64
	opDeleteLeaf      uint64
	opReplaceNodeLeaf uint64
	opReplaceNodeNode uint64
	deleteRestarts    uint64
)

// clearGlobalStats resets the global stat counters
// TODO: export this?
func clearGlobalStats() {
	atomic.StoreUint64(&nodesAllocated, 0)
	atomic.StoreUint64(&nodesReleased, 0)
	atomic.StoreUint64(&nodesReused, 0)
	atomic.StoreUint64(&opNodePrefixMiss, 0)
	atomic.StoreUint64(&opNodeLeafInsert, 0)
	atomic.StoreUint64(&opNode256leafInsert, 0)
	atomic.StoreUint64(&opNodeLeafUpdate, 0)
	atomic.StoreUint64(&updateRestarts, 0)
	atomic.StoreUint64(&opDeleteLeaf256, 0)
	atomic.StoreUint64(&opDeleteLeaf, 0)
	atomic.StoreUint64(&opReplaceNodeLeaf, 0)
	atomic.StoreUint64(&opReplaceNodeNode, 0)
	atomic.StoreUint64(&deleteRestarts, 0)
}

// Stats updates the given map with tree info and operation stats
func (idx *Tree) Stats(stats map[string]interface{}) {
	liveObjects := atomic.LoadUint64(&idx.liveObjects)
	blocksAllocated := int(atomic.LoadUint64(&idx.nextFreeBlock) + 1)
	bytesAllocated := blocksAllocated * int(blockSize)
	stats["index_kind"] = "trie"
	stats["objects"] = liveObjects
	stats["block_size"] = blockSize
	stats["blocks_allocated"] = blocksAllocated
	stats["bytes_allocated"] = bytesAllocated
	if liveObjects > 0 {
		stats["bytes_per_key"] = float64(bytesAllocated) / float64(liveObjects)
	}
	stats["locks_per_block"] = locksPerBlock
	stats["allocators"] = len(idx.allocators)
	stats["root_node"] = atomic.LoadUint64(&idx.root)
	stats["nodes_allocated"] = atomic.LoadUint64(&nodesAllocated)
	stats["nodes_released"] = atomic.LoadUint64(&nodesReleased)
	stats["nodes_reused"] = atomic.LoadUint64(&nodesReused)

	stats["op_current_epoch"] = atomic.LoadUint64(idx.manager.globalEpoch)
	stats["op_update_leaf_insert"] = atomic.LoadUint64(&opNodeLeafInsert)
	stats["op_update_leaf_update"] = atomic.LoadUint64(&opNodeLeafUpdate)
	stats["op_update_leaf_insert_256"] = atomic.LoadUint64(&opNode256leafInsert)
	stats["op_update_prefix_miss"] = atomic.LoadUint64(&opNodePrefixMiss)
	stats["op_update_restarts"] = atomic.LoadUint64(&updateRestarts)

	stats["op_delete_leaf"] = atomic.LoadUint64(&opDeleteLeaf)
	stats["op_delete_replace_node_node"] = atomic.LoadUint64(&opReplaceNodeNode)
	stats["op_delete_replace_node_leaf"] = atomic.LoadUint64(&opReplaceNodeLeaf)
	stats["op_delete_leaf_256"] = atomic.LoadUint64(&opDeleteLeaf256)
	stats["op_delete_restarts"] = atomic.LoadUint64(&deleteRestarts)
}
