package radix

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
)

const (
	notFound uint32 = iota
	deleteRoot
	deleteLeaf
	deleteLeaf256
	replaceNodeWithLeaf
	replaceNodeWithNode
)

var debugDeleteOperationName = map[uint32]string{
	notFound:            "NOT_FOUND",
	deleteRoot:          "DELETE_ROOT",
	deleteLeaf:          "DELETE_LEAF",
	deleteLeaf256:       "DELETE_LEAF_256",
	replaceNodeWithLeaf: "REPLACE_NODE_WITH_LEAF",
	replaceNodeWithNode: "REPLACE_NODE_WITH_NODE",
}

// DeleteOperation represents the action to remove a key from the tree.
// A delete operation is initiated through calling PrepareUpdate on a Tree or an Allocator
type DeleteOperation struct {
	// the value of the found item, if any
	Value uint64
	// FetchedKey is not used during a delete operation, but is provided as a convienance to the caller
	// to avoid having to allocate a new slice for fetching the key (the DeleteOperation is pooled)
	FetchedKey []byte

	idx          *Tree
	allocator    *Allocator
	releaseAlloc bool

	kind uint32

	raw     uint64
	leafPtr *uint64
	nodePtr *uint64

	parentGen, nodeGen, childGen int32
	parent, node, child          uint64

	data []uint64

	newLeaf      uint64
	count        int
	prefixLen    int
	prefixSlots  int
	leafPosition int

	prefix []byte
}

var deleteOperationPool = sync.Pool{
	New: func() interface{} {
		return &DeleteOperation{}
	},
}

func newDeleteOperation(idx *Tree, alloc *Allocator, releaseAlloc bool) (op *DeleteOperation) {
	op = deleteOperationPool.Get().(*DeleteOperation)
	op.idx = idx
	op.allocator = alloc
	op.releaseAlloc = releaseAlloc
	return
}

func (op *DeleteOperation) prepare(key []byte) (found bool) {
	op.FetchedKey = op.FetchedKey[:0]
	op.Value = 0
	op.allocator.startOp()
S:
	root := atomic.LoadUint64(&op.idx.root)
	if root == 0 {
		op.kind = notFound
		return false
	}
	if isLeaf(root) {
		if byte(root) == key[0] {
			if !op.idx.lockLeafRoot(root) {
				runtime.Gosched()
				goto S
			}
			op.kind = deleteRoot
			op.raw = root
			op.Value = getLeafValue(root)
			return true
		}
		op.kind = notFound
		return false
	}

	op.nodePtr = &op.idx.root
	op.raw = root
	op.parent = 0
	op.child = 0

	var depth, count, prefixLen int

searchLoop:
	for {
		_, op.node, count, prefixLen = explodeNode(op.raw)
		// first fetch the current generation for node (it could actually be shared with multiple nodes)
		op.nodeGen = op.idx.generation(op.node)
		// now verify that the node has not been changed since we read it
		if atomic.LoadUint64(op.nodePtr) != op.raw {
			goto S
		}
		// in case the parent was changed while we where working through it the current node might now be disconnected
		// so we check the current parent generation to safeguard against this.
		if op.parent != 0 && op.parentGen != op.idx.generation(op.parent) {
			goto S
		}

		block := int(op.node >> blockSlotsShift)
		offset := int(op.node & blockSlotsOffsetMask)
		op.data = op.idx.blocks[block].data[offset:]

		data := op.data

		if prefixLen > 0 {
			if prefixLen == 255 {
				op.prefixLen = int(data[0])
				op.prefixSlots = ((op.prefixLen + 15) >> 3)
			} else {
				op.prefixLen = prefixLen
				op.prefixSlots = ((prefixLen + 7) >> 3)
			}
			// optimistic approach, i.e., don't compare the prefix, simply skip the bytes, we do a final check anyway!
			depth += int(op.prefixLen)
			data = data[op.prefixSlots:]
		} else {
			op.prefixLen = 0
			op.prefixSlots = 0
		}
		if depth >= len(key) {
			op.kind = notFound
			return false
		}

		k := key[depth]

		if count >= fullAllocFrom {
			p := &data[int(k)]
			a := atomic.LoadUint64(p)
			if a == 0 {
				op.kind = notFound
				return false
			}
			if isLeaf(a) {
				op.raw = a
				op.leafPtr = p
				op.kind = deleteLeaf256
				op.Value = getLeafValue(a)
				return true
			}
			op.enterNode(p, a)
			depth++
			continue searchLoop
		}
		var (
			b byte
			a uint64
			p *uint64
			i int
		)
		for i = range data[:count] {
			p = &data[i]
			a = atomic.LoadUint64(p)
			b = byte(a)
			if b >= k {
				break
			}
		}
		if b == k {
			if isLeaf(a) {
				op.leafPosition = i
				if count == 2 {
					otherChildPtr := &data[1-op.leafPosition]
					otherChild := atomic.LoadUint64(otherChildPtr)

					if isLeaf(otherChild) {
						if !op.lock() {
							runtime.Gosched()
							goto S
						}
						if op.raw == root {
							if op.prefixLen > 0 {
								op.newLeaf = updateLeafKey(otherChild, key[0])
							} else {
								op.newLeaf = updateLeafKey(otherChild, byte(otherChild))
							}
						} else {
							op.newLeaf = updateLeafKey(otherChild, key[depth-op.prefixLen-1])
						}
						op.count = count
						op.kind = replaceNodeWithLeaf
						op.Value = getLeafValue(a)
						return true
					}

					// otherChild is a node!
					// We are removing a leaf from a node with two children where the other child is a node: NODE: [LEAF, NODE]
					// In order to avoid having nodes with a single child, we need to replace this node with a new node and copy the children of the CHILD NODE there
					// The new node will have prefix CURRENT-PREFIX + CHILD NODE KEY + CHILD NODE PREFIX
					var (
						childKey       byte
						childPrefixLen int
					)
					childKey, op.child, op.count, childPrefixLen = explodeNode(otherChild)

					// fetch child generation
					op.childGen = op.idx.generation(op.child)
					// verify it's still the same
					if atomic.LoadUint64(otherChildPtr) != otherChild {
						goto S
					}

					// we need to lock this node + it's parent + the child node
					if !op.lock3() {
						runtime.Gosched()
						goto S
					}

					// now build the new prefix, start with adding the existing prefix for the current node (the one being replaced)
					op.prefix, _ = appendPrefix(op.prefix[:0], op.data, prefixLen)
					// add the child node key
					op.prefix = append(op.prefix, childKey)
					// update op.data to point to child node data
					childBlock := int(op.child >> blockSlotsShift)
					childOffset := int(op.child & blockSlotsOffsetMask)
					op.data = op.idx.blocks[childBlock].data[childOffset:]
					// append child node prefix, update op.prefixSlots to the number of slots used by the child
					op.prefix, op.prefixSlots = appendPrefix(op.prefix, op.data, childPrefixLen)

					op.kind = replaceNodeWithNode
					op.Value = getLeafValue(a)
					return true
				}
				op.count = count
				if !op.lock() {
					runtime.Gosched()
					goto S
				}
				op.kind = deleteLeaf
				op.Value = getLeafValue(a)
				return true
			}
			op.enterNode(p, a)
			depth++
			continue searchLoop
		}
		op.kind = notFound
		return false
	}
}

// Finalize finalizes the delete operation.
// It returns true if the delete was successful. If false, there was a write conflict and the operation must be restarted.
// The *DeleteOperation may not be used again after the call.
func (op *DeleteOperation) Finalize() (deleted bool) {
	switch op.kind {
	case deleteRoot:
		atomic.StoreUint64(&op.idx.root, 0)
		op.idx.unlockRoot()
		op.allocator.itemCounter--
		deleted = true

	case replaceNodeWithLeaf:
		// PARAMETERS: op.node, op.parent, op.nodePtr, op.newLeaf, op.node, op.parent, op.count, op.prefixSlots
		// replace the node with a leaf (the one not being deleted). the new leaf gets the byte key from the removed node
		atomic.StoreUint64(op.nodePtr, op.newLeaf)
		op.unlock()
		op.allocator.recycle(op.node, op.count+op.prefixSlots)
		op.allocator.itemCounter--
		deleted = true

	case replaceNodeWithNode:
		// PARAMETERS: op.node, op.parent, op.child, op.count, op.block, op.offset, op.count, op.prefixSlots, op.prefix
		newNode, dst := op.idx.allocateNodeWithPrefix(op.allocator, op.count, op.prefix)
		src := op.data[op.prefixSlots : op.prefixSlots+op.count]
		copy(dst, src)
		// insert the new node into the tree
		atomic.StoreUint64(op.nodePtr, buildNode(byte(op.raw), newNode, op.count, len(op.prefix)))
		// unlock
		op.unlock3()
		// recycle the old child node
		op.allocator.recycle(op.child, op.count+op.prefixSlots)
		op.allocator.itemCounter--
		deleted = true

	case deleteLeaf:
		// PARAMETERS: op.node, op.parent, op.count, op.prefixLen, op.block, op.offset, op.count, op.prefixSlots, op.leafPosition
		// shrink node size by 1
		newNode, dst := op.idx.allocateNode(op.allocator, op.count-1, op.prefixLen)
		// copy full node up to the leaf to be removed, including unmodified prefix
		src := op.data[:op.count+op.prefixSlots]
		split := op.prefixSlots + op.leafPosition
		copy(dst, src[:split])
		// copy the rest of the node (after the removed leaf)
		copy(dst[split:], src[split+1:])
		// insert the new node into the tree, no need to compare and swap since this is checked after locking
		atomic.StoreUint64(op.nodePtr, buildNode(byte(op.raw), newNode, op.count-1, op.prefixLen))
		// unlock node+parent
		op.unlock()
		// recycle old node
		op.allocator.recycle(op.node, op.count+op.prefixSlots)
		op.allocator.itemCounter--
		deleted = true

	case deleteLeaf256:
		// PARAMETERS: op.leafPtr, op.raw
		// TODO(oscar): replace with a variable sized node when the threshold size is reached?
		// TODO(oscar): replace with a leaf when there is only one child left
		deleted = atomic.CompareAndSwapUint64(op.leafPtr, op.raw, 0)
		if deleted {
			op.allocator.itemCounter--
		}
	}
	op.allocator.endOp()
	if op.releaseAlloc {
		op.idx.ReleaseAllocator(op.allocator)
	}
	deleteOperationPool.Put(op)
	return deleted
}

// Abort aborts the delete operation, unlocking any locks taken during the prepare call and recycles the *DeleteOperation.
// The *DeleteOperation may not be used again after the call.
func (op *DeleteOperation) Abort() {
	switch op.kind {
	case deleteRoot:
		op.idx.unlockRoot()
	case deleteLeaf, replaceNodeWithLeaf:
		op.unlock()
	case replaceNodeWithNode:
		op.idx.unlockNodes3(op.parent, op.node, op.child)
	}
	op.allocator.endOp()
	if op.releaseAlloc {
		op.idx.ReleaseAllocator(op.allocator)
	}
	deleteOperationPool.Put(op)
}

func (op *DeleteOperation) String() (s string) {
	s = fmt.Sprintf("Operation: %s\n FetchedKey: %s\n %d prefix slots\n", debugDeleteOperationName[op.kind], op.FetchedKey, op.prefixSlots)
	return s
}

func (op *DeleteOperation) lock() bool {
	return op.idx.lockNodes2(op.node, op.nodeGen, op.parent, op.parentGen)
}

func (op *DeleteOperation) unlock() {
	op.idx.unlockNodes2(op.node, op.parent)
}

func (op *DeleteOperation) lock3() bool {
	return op.idx.lockNodes3(op.child, op.childGen, op.node, op.nodeGen, op.parent, op.parentGen)
}

func (op *DeleteOperation) unlock3() {
	op.idx.unlockNodes3(op.child, op.node, op.parent)
}

// enterNode is inlined by compiler
func (op *DeleteOperation) enterNode(p *uint64, a uint64) {
	op.parentGen = op.nodeGen
	op.parent = op.node
	op.nodePtr = p
	op.raw = a
}
