package radix

import (
	"sync/atomic"
)

// We use a generation locking where each data block has a number of int32 locks (typically 4096 locks for a 2MB block).
// Nodes are associated with a lock based on the address of their first data slot.
// A even value means unlocked, odd locked.
// We increase by 1 when locking, and increase by 1 again when unlocking.
// This way writers can check if a node might have been changed or detached since they accessed it, and restart their operations.

func (idx *Tree) lockLeafRoot(expectedValue uint64) bool {
	if !atomic.CompareAndSwapInt32(&idx.rootLeafLock, 0, 1) {
		return false
	}
	if atomic.LoadUint64(&idx.root) != expectedValue {
		atomic.AddInt32(&idx.rootLeafLock, -1)
		return false
	}
	return true
}

func (idx *Tree) unlockRoot() {
	atomic.AddInt32(&idx.rootLeafLock, -1)
}

func (idx *Tree) generation(node uint64) int32 {
	block := node >> blockSlotsShift
	lock := node & lockOffsetMask
	return atomic.LoadInt32(&idx.blocks[block].locks[lock])
}

func (idx *Tree) isLocked(node uint64) bool {
	block := node >> blockSlotsShift
	lock := node & lockOffsetMask
	return atomic.LoadInt32(&idx.blocks[block].locks[lock])&1 == 1
}

// lockNodes3 locks three nodes (parent node, node, child node), given their generations have not changed.
// Parent may be zero. It returns true if successful.
func (idx *Tree) lockNodes3(child uint64, childGen int32, node uint64, nodeGen int32, parent uint64, parentGen int32) (locked bool) {
	if parent == 0 {
		return idx.lockNodes2(child, childGen, node, nodeGen)
	}
	if childGen&1 == 1 || nodeGen&1 == 1 || parentGen&1 == 1 {
		// child, node or parent already locked
		return false
	}
	// lock child
	block := child >> blockSlotsShift
	lock := child & lockOffsetMask
	if !atomic.CompareAndSwapInt32(&idx.blocks[block].locks[lock], childGen, childGen+1) {
		return false
	}
	// lock node, if needed
	block2 := node >> blockSlotsShift
	lock2 := node & lockOffsetMask
	lock2Unique := lock2 != lock || block2 != block
	if lock2Unique {
		if !atomic.CompareAndSwapInt32(&idx.blocks[block2].locks[lock2], nodeGen, nodeGen+1) {
			atomic.AddInt32(&idx.blocks[block].locks[lock], -1)
			return false
		}
	}
	// lock parent, if needed
	block3 := parent >> blockSlotsShift
	lock3 := parent & lockOffsetMask
	lock3Unique := (lock3 != lock || block3 != block) && (lock3 != lock2 || block3 != block2)
	if lock3Unique {
		if !atomic.CompareAndSwapInt32(&idx.blocks[block3].locks[lock3], parentGen, parentGen+1) {
			atomic.AddInt32(&idx.blocks[block].locks[lock], -1)
			if lock2Unique {
				atomic.AddInt32(&idx.blocks[block2].locks[lock2], -1)
			}
			return false
		}
	}
	return true
}

// unlockNodes3 releases locks for three nodes.
// Parent may be zero, node and child must be non-zero.
func (idx *Tree) unlockNodes3(child, node, parent uint64) {
	if parent == 0 {
		idx.unlockNodes2(child, node)
		return
	}
	// unlock from top to bottom
	block := parent >> blockSlotsShift
	lock := parent & lockOffsetMask
	if atomic.AddInt32(&idx.blocks[block].locks[lock], 1)&1 != 0 {
		panic("Unlock of unlocked parent")
	}
	block2 := node >> blockSlotsShift
	lock2 := node & lockOffsetMask
	lock2Unique := lock2 != lock || block2 != block
	if lock2Unique {
		if atomic.AddInt32(&idx.blocks[block2].locks[lock2], 1)&1 != 0 {
			panic("Unlock of unlocked node")
		}
	}
	block3 := child >> blockSlotsShift
	lock3 := child & lockOffsetMask
	lock3Unique := (lock3 != lock || block3 != block) && (lock3 != lock2 || block3 != block2)
	if lock3Unique {
		if atomic.AddInt32(&idx.blocks[block3].locks[lock3], 1)&1 != 0 {
			panic("Unlock of unlocked child")
		}
	}
}

// lockNodes2 locks node and parent, given their generations have not changed.
// Parent may be zero. It returns true if successful.
func (idx *Tree) lockNodes2(node uint64, nodeGen int32, parent uint64, parentGen int32) bool {
	if parent == 0 {
		return idx.lockNode(node, nodeGen)
	}
	if nodeGen&1 == 1 || parentGen&1 == 1 {
		// node or parent already locked
		return false
	}
	// first lock node
	block := node >> blockSlotsShift
	lock := node & lockOffsetMask
	if !atomic.CompareAndSwapInt32(&idx.blocks[block].locks[lock], nodeGen, nodeGen+1) {
		return false
	}
	// lock parent, if needed
	block2 := parent >> blockSlotsShift
	lock2 := parent & lockOffsetMask
	lock2Unique := lock2 != lock || block2 != block
	if lock2Unique {
		if !atomic.CompareAndSwapInt32(&idx.blocks[block2].locks[lock2], parentGen, parentGen+1) {
			atomic.AddInt32(&idx.blocks[block].locks[lock], -1)
			return false
		}
	}
	return true
}

// unlockNodes2 unlocks node and parent.
func (idx *Tree) unlockNodes2(node uint64, parent uint64) {
	if parent == 0 {
		idx.unlockNode(node)
		return
	}
	block := parent >> blockSlotsShift
	lock := parent & lockOffsetMask
	if atomic.AddInt32(&idx.blocks[block].locks[lock], 1)&1 != 0 {
		panic("Unlock of unlocked parent")
	}

	block2 := node >> blockSlotsShift
	lock2 := node & lockOffsetMask
	lock2unique := lock2 != lock || block2 != block
	if lock2unique {
		if atomic.AddInt32(&idx.blocks[block2].locks[lock2], 1)&1 != 0 {
			panic("Unlock of unlocked node")
		}
	}
}

// lockNode locks node given it's generation has not changed.
func (idx *Tree) lockNode(node uint64, gen int32) bool {
	if gen&1 == 1 {
		return false
	}
	block := node >> blockSlotsShift
	lock := node & lockOffsetMask
	return atomic.CompareAndSwapInt32(&idx.blocks[block].locks[lock], gen, gen+1)
}

// unlockNode unlocks node.
func (idx *Tree) unlockNode(node uint64) {
	block := node >> blockSlotsShift
	lock := node & lockOffsetMask
	if atomic.AddInt32(&idx.blocks[block].locks[lock], 1)&1 != 0 {
		panic("Unlock of unlocked node in Tree.unlockNode")
	}
}
