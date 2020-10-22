package radix

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
)

const (
	replaceEmptyRootWithLeaf uint32 = iota
	replaceLeafRootWithNode
	rootLeafUpdate

	nodePrefixMiss
	nodeLeafInsert
	nodeLeafInsert256
	nodeLeafUpdate
	nodeLeafUpdate256
)

var debugOperationName = map[uint32]string{
	replaceEmptyRootWithLeaf: "REPLACE_EMPTY_ROOT_WITH_LEAF",
	replaceLeafRootWithNode:  "REPLACE_LEAF_ROOT_WITH_NODE",
	rootLeafUpdate:           "ROOT_LEAF_UPDATE",
	nodePrefixMiss:           "NODE_PREFIX_MISS",
	nodeLeafInsert:           "NODE_LEAF_INSERT",
	nodeLeafInsert256:        "NODE_256_LEAF_INSERT",
	nodeLeafUpdate:           "NODE_LEAF_UPDATE",
	nodeLeafUpdate256:        "NODE_256_LEAF_UPDATE",
}

// UpdateOperation represents the action to insert a new value or update on existing value in a tree.
// An update operation is initiated through calling PrepareUpdate on a Tree or an Allocator.
type UpdateOperation struct {
	// points to key data used in call to PrepareUpdate
	Key []byte
	// the value of the found item, if any
	Value uint64
	// the key data fetched by the caller after call to PrepareUpdate (and before FinalizeUpdate)
	FetchedKey []byte
	// set true by caller if FetchedKey equals Key
	Match bool

	// the radix tree for this operation
	idx *Tree
	// allocator for this operation
	allocator *Allocator
	// if the allocator should be released when operation is finalized or aborted
	releaseAlloc bool

	kind     uint32
	k        byte
	position int

	nodeGen, parentGen int32

	node, parent uint64
	count        int
	depth        int

	// the extracted node prefix
	prefix []byte
	// the index into prefix where we got a mismatch against the current update key
	prefixMismatch int

	// points to where the node is inserted in the tree
	nodePtr *uint64

	// location of node data
	data        []uint64
	prefixSlots int

	// points to where the leaf is in the tree
	leafPtr *uint64
	// the current leaf value, to be used in CompareAndSwap
	currentLeaf uint64

	// current raw value of node
	raw uint64
}

var updateOperationPool = sync.Pool{
	New: func() interface{} {
		return &UpdateOperation{}
	},
}

// newUpdateOperation returns an UpdateOperation on *Tree idx using *Allocator alloc.
// If releaseAlloc is true, the allocator is released when the operation is completed or aborted.
func newUpdateOperation(idx *Tree, alloc *Allocator, releaseAlloc bool) (op *UpdateOperation) {
	op = updateOperationPool.Get().(*UpdateOperation)
	op.idx = idx
	op.allocator = alloc
	op.releaseAlloc = releaseAlloc
	return
}

func (op *UpdateOperation) prepareUpdate(key []byte) (found bool) {
	op.Key = key
	op.Match = false
	op.Value = 0
	op.FetchedKey = op.FetchedKey[:0]
	op.allocator.startOp()
S:
	root := atomic.LoadUint64(&op.idx.root)
	if root == 0 {
		// index is empty, lock root
		if !op.idx.lockLeafRoot(root) {
			runtime.Gosched()
			goto S
		}
		op.k = key[0]
		op.kind = replaceEmptyRootWithLeaf
		return false
	}
	if isLeaf(root) {
		// root is a leaf, acquire a root lock
		if !op.idx.lockLeafRoot(root) {
			runtime.Gosched()
			goto S
		}
		op.k = key[0]
		op.currentLeaf = root
		// check if key matches
		if byte(root) == key[0] {
			// key matches, this might be a hit, or we will have to insert a prefixed node here
			op.kind = rootLeafUpdate
			op.Value = getLeafValue(root)
			return true
		}
		// we should replace root with a node-2 with two leafs (no prefix)
		op.kind = replaceLeafRootWithNode
		return false
	}

	op.depth = 0
	op.nodePtr = &op.idx.root
	op.raw = root
	op.parent = 0
	op.parentGen = 0
	prefixLen := 0

searchLoop:
	for {
		_, op.node, op.count, prefixLen = explodeNode(op.raw)
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
			op.prefix, op.prefixSlots = appendPrefix(op.prefix[:0], data, prefixLen)
			// now check for prefix mismatch
			for i, a := range op.prefix {
				if key[op.depth+i] != a {
					// part of or all of prefix did not match, we must insert a node here
					// We are going to change data at nodePtrOffset, which is inside the data block of parent
					// We will copy all children of node (node data block must also be protected)
					if !op.lock() {
						runtime.Gosched()
						goto S
					}
					op.kind = nodePrefixMiss
					op.k = key[op.depth]
					op.prefixMismatch = i
					return false
				}
			}
			data = data[op.prefixSlots:]
			op.depth += len(op.prefix)
		} else {
			op.prefix = op.prefix[:0]
			op.prefixSlots = 0
		}

		op.k = key[op.depth]

		// A. Check a node-256
		if op.count >= fullAllocFrom {
			p := &data[int(op.k)]
			a := atomic.LoadUint64(p)
			if a == 0 {
				if !op.lock() {
					runtime.Gosched()
					goto S
				}
				op.kind = nodeLeafInsert256
				op.leafPtr = p
				return false
			}
			if isLeaf(a) {
				if !op.lock() {
					runtime.Gosched()
					goto S
				}
				op.kind = nodeLeafUpdate256
				op.leafPtr = p
				op.currentLeaf = a
				op.Value = getLeafValue(a)
				return true
			}
			op.enterNode(p, a)
			continue searchLoop
		}

		// B. Check a variable sized node
		op.position = op.count
		var (
			p *uint64
			a uint64
			b byte
		)
		for i := range data[:op.count] {
			p = &data[i]
			a = atomic.LoadUint64(p)
			b = byte(a)
			if b >= op.k {
				op.position = i
				break
			}
		}
		if b == op.k {
			if isLeaf(a) {
				if !op.lock() {
					runtime.Gosched()
					goto S
				}
				op.kind = nodeLeafUpdate
				op.leafPtr = p
				op.currentLeaf = a
				op.Value = getLeafValue(a)
				return true
			}
			op.enterNode(p, a)
			continue searchLoop
		}

		if !op.lock() {
			runtime.Gosched()
			goto S
		}
		op.kind = nodeLeafInsert
		return false
	}
}

// Finalize performs the update or insert in the tree, storing the given value at the prepared location.
// In case the value is larger than 55 bits it will be truncated.
// It returns true if the update was successful. If false, there was a write conflict and the operation must be restarted.
// The *UpdateOperation may not be used again after the call.
func (op *UpdateOperation) Finalize(value uint64) (updated bool) {
	switch op.kind {

	case replaceEmptyRootWithLeaf:
		updated = atomic.CompareAndSwapUint64(&op.idx.root, 0, buildLeaf(op.k, value))
		op.idx.unlockRoot()
		if updated {
			op.allocator.itemCounter++
		}

	case replaceLeafRootWithNode:
		newNode, data := op.idx.allocateNode(op.allocator, 2, 0)
		addSortedChildren(data, op.currentLeaf, buildLeaf(op.k, value))
		updated = atomic.CompareAndSwapUint64((*uint64)(&op.idx.root), op.currentLeaf, buildNode(0, newNode, 2, 0))
		op.idx.unlockRoot()
		if updated {
			op.allocator.itemCounter++
		}

	case rootLeafUpdate:
		if op.Match {
			// root is a leaf, and it was match, simply update it
			updated = atomic.CompareAndSwapUint64(&op.idx.root, op.currentLeaf, buildLeaf(op.k, value))
		} else {
			// check how many characters the keys have in common (we know it's >= 1)
			prefixLen := 0
			for op.Key[prefixLen] == op.FetchedKey[prefixLen] {
				prefixLen++
				// in case prefixLen == len(op.Key) or len(op.FetchedKey) we have a violoation of the rule that no key can be the prefix of another key!
				if prefixLen == len(op.Key) || prefixLen == len(op.FetchedKey) {
					panic("Prefix violation")
				}
			}
			// allocate new NODE-2 and try to insert it
			newNode, data := op.idx.allocateNodeWithPrefix(op.allocator, 2, op.Key[:prefixLen])
			addSortedChildren(data,
				buildLeaf(op.Key[prefixLen], value),
				updateLeafKey(op.currentLeaf, op.FetchedKey[prefixLen]))
			updated = atomic.CompareAndSwapUint64(&op.idx.root, op.currentLeaf, buildNode(0, newNode, 2, prefixLen))
			if updated {
				op.allocator.itemCounter++
			}
		}
		op.idx.unlockRoot()

	case nodePrefixMiss:
		// None or only part of the prefix on a node matches, we must replace it with a shorter prefix node and insert the second part (with the node children untouched) as a child node
		// The mismatch was at op.prefix[op.prefixMismatch]
		a := op.Key[op.depth+op.prefixMismatch]
		b := op.prefix[op.prefixMismatch]
		// Clone the existing node but with an updated prefix
		pl := len(op.prefix) - op.prefixMismatch - 1
		updatedNode, dst := op.idx.allocateNodeWithPrefix(op.allocator, op.count, op.prefix[op.prefixMismatch+1:])
		// copy children
		src := op.data[op.prefixSlots:]
		copy(dst, src[:op.count])

		// create a new NODE-2 with the matched part of the prefix (if any) and insert it
		newNode, data := op.idx.allocateNodeWithPrefix(op.allocator, 2, op.Key[op.depth:op.depth+op.prefixMismatch])
		addSortedChildren(data,
			buildLeaf(a, value),
			buildNode(b, updatedNode, op.count, pl))
		atomic.StoreUint64(op.nodePtr, buildNode(byte(op.raw), newNode, 2, op.prefixMismatch))

		op.unlock()
		op.allocator.recycle(op.node, op.count+op.prefixSlots)
		op.allocator.itemCounter++
		updated = true

	case nodeLeafInsert256:
		updated = atomic.CompareAndSwapUint64(op.leafPtr, 0, buildLeaf(op.k, value))
		if updated {
			op.allocator.itemCounter++
		}
		op.unlock()

	case nodeLeafInsert:
		// insert a leaf by making the node bigger using copy-on-write
		if op.count < fullAllocAfter {
			// GROW NODE SIZE BY 1
			newNode, dst := op.idx.allocateNode(op.allocator, op.count+1, len(op.prefix))
			src := op.data
			split := op.prefixSlots + op.position
			// copy full node up to insert position, including unmodified prefix
			copy(dst, src[:split])
			// append the new value as a leaf at op.position
			dst[split] = buildLeaf(op.k, value)
			// copy the rest
			l := op.count - op.position
			copy(dst[split+1:], src[split:split+l])
			// insert the new node into the tree, no need to compare and swap since this is checked after locking
			v := buildNode(byte(op.raw), newNode, op.count+1, len(op.prefix))
			atomic.StoreUint64(op.nodePtr, v)
			op.unlock()
			op.allocator.recycle(op.node, op.count+op.prefixSlots)
			op.allocator.itemCounter++
			updated = true

		} else {
			// SWITCH TO A NODE-256
			newNode, dst := op.idx.allocateNodeWithPrefix(op.allocator, 256, op.prefix)
			// we need to zero it
			for i := range dst[:256] {
				dst[i] = 0
			}
			// put the existing nodes in their new places
			src := op.data[op.prefixSlots : op.prefixSlots+op.count]
			// set the new leaf
			dst[int(op.k)] = buildLeaf(op.k, value)
			// add the existing, while setting obsolete tag on to-be-removed nodes
			for _, a := range src {
				b := byte(a)
				dst[int(b)] = a
			}
			// insert the new node
			v := buildNode(byte(op.raw), newNode, 256, len(op.prefix))
			atomic.StoreUint64(op.nodePtr, v)
			op.unlock()
			// recycle old node
			op.allocator.recycle(op.node, op.count+op.prefixSlots)
			op.allocator.itemCounter++
			updated = true
		}

	case nodeLeafUpdate, nodeLeafUpdate256:
		// These are pure leaf updates, so they never disconnected other nodes from the tree
		if op.Match {
			// compare and swap needed since leaf might have changed before we acquired the lock
			updated = atomic.CompareAndSwapUint64(op.leafPtr, op.currentLeaf, buildLeaf(op.k, value))
		} else {
			// key did not match, replace leaf with a NODE-2
			// check how many characters the keys have in common starting from depth+1 (we know they match at depth)
			prefixLen := 0
			i := op.depth + 1
			for ; i < len(op.Key) && i < len(op.FetchedKey) && op.Key[i] == op.FetchedKey[i]; i++ {
				prefixLen++
			}
			a := op.Key[i]
			b := op.FetchedKey[i]
			// creat a new NODE-2 and try to insert it
			newNode, data := op.idx.allocateNodeWithPrefix(op.allocator, 2, op.Key[op.depth+1:op.depth+1+prefixLen])
			addSortedChildren(data,
				buildLeaf(a, value),
				updateLeafKey(op.currentLeaf, b),
			)
			updated = atomic.CompareAndSwapUint64((*uint64)(op.leafPtr), op.currentLeaf, buildNode(op.k, newNode, 2, prefixLen))
			if updated {
				op.allocator.itemCounter++
			}
		}
		op.unlock()
	}
	op.allocator.endOp()
	if op.releaseAlloc {
		op.idx.ReleaseAllocator(op.allocator)
	}
	updateOperationPool.Put(op)
	return updated
}

// Abort aborts the update operation, unlocking any locks taken during the prepare call and recycles the *UpdateOperation.
// The *UpdateOperation may not be used again after the call.
func (op *UpdateOperation) Abort() {
	switch op.kind {
	case replaceEmptyRootWithLeaf, replaceLeafRootWithNode, rootLeafUpdate:
		op.idx.unlockRoot()
	default:
		op.unlock()
	}
	op.allocator.endOp()
	if op.releaseAlloc {
		op.idx.allocatorQueue.put(op.allocator.id)
	}
	//op.allocator.stats.updateRestarts += op.restarts
	updateOperationPool.Put(op)
}

// String returns a description of the operation
func (op *UpdateOperation) String() (s string) {
	s = fmt.Sprintf("Operation: %s\n Key:        %s\n FetchedKey: %s\n Match=%v\n depth=%d\n Key at depth: %s\n node prefix: %s (%d bytes, %d slots)\nnode: %d\nparent: %d\n", debugOperationName[op.kind], op.Key, op.FetchedKey, op.Match, op.depth, op.Key[op.depth:], op.prefix, len(op.prefix), op.prefixSlots, op.node, op.parent)
	switch op.kind {
	case replaceLeafRootWithNode, rootLeafUpdate:
		return s + fmt.Sprintf(" currentLeaf: %d\n", op.currentLeaf)
	case nodePrefixMiss:
		return s + fmt.Sprintf("\nat node=%d (parent=%d), current node count=%d, prefixMismatched=%d (%s), insert k=%x, insert depth=%d", op.node, op.parent, op.count, op.prefixMismatch, op.Key[op.depth:op.depth+op.prefixMismatch], op.k, op.depth)
	}
	return s
}

// lock is inlined by compiler
func (op *UpdateOperation) lock() bool {
	return op.idx.lockNodes2(op.node, op.nodeGen, op.parent, op.parentGen)
}

// unlock is inlined by compiler
func (op *UpdateOperation) unlock() {
	op.idx.unlockNodes2(op.node, op.parent)
}

// enterNode is inlined by compiler
func (op *UpdateOperation) enterNode(p *uint64, a uint64) {
	op.parentGen = op.nodeGen
	op.parent = op.node
	op.nodePtr = p
	op.raw = a
	op.depth++
}

// addSortedChildren is inlined by compiler
func addSortedChildren(data []uint64, a, b uint64) {
	if byte(a) < byte(b) {
		data[0] = a
		data[1] = b
		return
	}
	data[0] = b
	data[1] = a
}
