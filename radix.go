// Package radix implements a memory efficient and concurrency safe adaptive radix tree (ART).
//
// The typical use case is an in-memory database index where the tree stores a pointer to
// where an object is stored on a permanent storage.
//
// The data structure is free of memory pointers so the overhead from garbage collection is minimal.
//
// Limitations
//
// 1. To save memory, this tree does not store the full keys in the leaves. It only stores as much information as needed to distinguish between the different keys. It is assumed the full keys are retrievable by the user. In the typical use case they would be stored on a permanent storage.
//
// 2. Max 55 bits are available for storing values associated with the inserted keys. The values could be offsets to permanent storage or compressed memory pointers.
//
// 3. The tree accepts any []byte data as keys, but in case you want to use range or min/max searches, the key data needs to be binary comparable.
// For uint64 all you need to do is store them in big-endian order. For int64 the sign bit also needs to be flipped.
// ASCII strings are trivial, but floating point numbers requires more transformations. UTF8 is rather complex (see golang.org/x/text/collate).
//
// 4. When indexing keys of variable length (like strings) the requirement is that no key may be the prefix of another key. A simple solution for this is to append a 0 byte on all keys (given 0 is not used within the keys).
//
// Unsafe usage
//
// For improved performance unsafe operations are used in two cases: when persisting uint64 slices to a state file and when accessing []byte prefixes within the uint64 slices.
package radix

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
)

const (
	blockOffsetMask uint64 = 1<<blockSizeShift - 1
	blockSize              = 1 << blockSizeShift

	blockSlotsShift      = blockSizeShift - 3
	blockSlotsOffsetMask = 1<<blockSlotsShift - 1

	locksShift     uint64 = blockSizeShift - 9
	lockOffsetMask uint64 = 1<<locksShift - 1
	locksPerBlock         = 1 << locksShift

	fullAllocFrom  = 128
	fullAllocAfter = fullAllocFrom - 1
)

const (
	blockWritten = iota
	blockAllocated
)

type nodeBlock struct {
	data  []uint64
	locks [locksPerBlock]int32

	slotsReleased int64
	slotsReused   int64
}

// Options are used when creating a new Tree.
type Options struct {
	NumAllocators int // default is runtime.NumCPU()
}

// A Tree is a concurrent safe adaptive radix tree.
type Tree struct {
	// to avoid having a mutex we must preallocate the whole block array
	blocks        [maxBlockCount]*nodeBlock
	nextFreeBlock uint64

	// the current root node
	root         uint64
	rootLeafLock int32

	manager        *epochManager
	allocators     []*Allocator
	allocatorQueue *slots

	newBlocks    chan int
	done         chan bool
	blockMap     map[int]int
	blockMapLock sync.RWMutex

	stopped int32

	// this value is not guaranteed to be correct until the tree is still and all allocators have flushed their counters
	liveObjects int64
}

func setDefaultOptions(options *Options) {
	if options.NumAllocators <= 0 {
		options.NumAllocators = runtime.NumCPU()
	}
}

// NewTree creates a new Tree.
func NewTree(options *Options) *Tree {
	if options == nil {
		options = new(Options)
	}
	setDefaultOptions(options)

	idx := &Tree{
		newBlocks: make(chan int),
		done:      make(chan bool),
		blockMap:  make(map[int]int),
		manager:   newEpochManager(options.NumAllocators),
	}

	idx.allocators = make([]*Allocator, options.NumAllocators)
	idx.allocatorQueue = newSlots(len(idx.allocators))
	for i := range idx.allocators {
		idx.allocators[i] = newAllocator(idx, i, blockSize, idx.newBlocks)
	}
	go idx.blockAllocator()
	return idx
}

// NewTreeFromState initiates a Tree from state data previously written by
func NewTreeFromState(data io.Reader) (*Tree, error) {
	idx := &Tree{
		newBlocks: make(chan int),
		done:      make(chan bool),
		blockMap:  make(map[int]int),
	}
	if err := idx.loadState(data); err != nil {
		return nil, fmt.Errorf("Failed loading index state : %v", err)
	}
	go idx.blockAllocator()
	return idx, nil
}

// Len returns the current number of items in the tree
// It needs to query all allocators for their counters, so it will block if an allocator is constantly reserved...
func (idx *Tree) Len() (count int) {
	idx.Stop()
	count = int(idx.liveObjects)
	for _, a := range idx.allocators {
		count += int(a.itemCounter)
	}
	idx.Start()
	return
}

// Stop withdraws all allocators to prevent any more write or reads.
// It will blocks until it gets all allocators.
// If already stopped, it returns silently.
func (idx *Tree) Stop() {
	if !atomic.CompareAndSwapInt32(&idx.stopped, 0, 1) {
		return
	}
	for i := 0; i < len(idx.allocators); i++ {
		_ = idx.allocatorQueue.get()
	}
}

// Start releases all allocators withdrawn through a previous call to Stop.
// In case the indexed is not stopped in returns silently.
func (idx *Tree) Start() {
	if !atomic.CompareAndSwapInt32(&idx.stopped, 1, 0) {
		return
	}
	for i := 0; i < len(idx.allocators); i++ {
		idx.allocatorQueue.put(i)
	}
}

// Close stops the index, and then terminates the memory allocation goroutine, which will otherwise leak.
func (idx *Tree) Close() {
	idx.Stop()
	close(idx.done)
}

// WriteState writes the state of a stopped index to the given writer.
// If the indexed is not stopped the result is undefined.
func (idx *Tree) WriteState(out io.Writer) (n int, err error) {
	return idx.writeState(out)
}

func (idx *Tree) blockAllocator() {
	next := atomic.LoadUint64(&idx.nextFreeBlock)
	for {
		idx.blocks[next] = &nodeBlock{
			data: make([]uint64, blockSize>>3),
		}
		atomic.StoreUint64(&idx.nextFreeBlock, next)
		select {
		case <-idx.done:
			return
		case idx.newBlocks <- int(next):
		}
		next++
	}
}

func (idx *Tree) getBlockStatus(block int) (status int) {
	idx.blockMapLock.RLock()
	status = idx.blockMap[block]
	idx.blockMapLock.RUnlock()
	return
}

func (idx *Tree) setBlockStatus(block int, status int) {
	idx.blockMapLock.Lock()
	idx.blockMap[block] = status
	idx.blockMapLock.Unlock()
}

// allocateNode returns the new node and its data block, position at start
func (idx *Tree) allocateNode(a *Allocator, count int, prefixLen int) (n uint64, data []uint64) {
	prefixSlots := (prefixLen + 7) >> 3
	if prefixLen >= 255 {
		prefixSlots++
	}
	count += prefixSlots
	n = a.newNode(count)
	block := int(n >> blockSlotsShift)
	offset := int(n & blockSlotsOffsetMask)
	data = idx.blocks[block].data[offset:]
	return
}

// allocateNodeWithPrefix returns the new node and its data block, positioned after the prefix
func (idx *Tree) allocateNodeWithPrefix(a *Allocator, count int, prefix []byte) (n uint64, data []uint64) {
	prefixLen := len(prefix)
	prefixSlots := (prefixLen + 7) >> 3
	if prefixLen >= 255 {
		prefixSlots++
	}
	count += prefixSlots
	n = a.newNode(count)
	block := int(n >> blockSlotsShift)
	offset := int(n & blockSlotsOffsetMask)
	data = idx.blocks[block].data[offset:]
	if prefixLen > 0 {
		storePrefix(data, prefix)
		data = data[prefixSlots:]
	}
	return
}

// GetAllocator reserves an allocator used for bulk Lookup/Update/Delete operations.
func (idx *Tree) GetAllocator() *Allocator {
	return idx.allocators[idx.allocatorQueue.get()]
}

// ReleaseAllocator returns an allocator previously reserved using GetAllocator
func (idx *Tree) ReleaseAllocator(a *Allocator) {
	idx.allocatorQueue.put(a.id)
}

// Lookup searches the tree for a key and if a candidate leaf is found returns the value stored with it.
// The caller needs to verify if the candidate is equal to the lookup key, since if the key didn't exist, the candidate
// will be another key sharing a prefix with the lookup key.
func (idx *Tree) Lookup(key []byte) (value uint64, found bool) {
	id := idx.allocatorQueue.get()
	value, found = idx.allocators[id].Lookup(key)
	idx.allocatorQueue.put(id)
	return
}

// PrepareUpdate reserves an allocator and uses it to prepare an update operation.
// See Allocator.PrepareUpdate for details
func (idx *Tree) PrepareUpdate(key []byte) (found bool, op *UpdateOperation) {
	id := idx.allocatorQueue.get()
	op = newUpdateOperation(idx, idx.allocators[id], true)
	return op.prepareUpdate(key), op
}

// PrepareDelete reserves an allocator and uses it to prepare a delete operation.
// See Allocator.PrepareDelete for details
func (idx *Tree) PrepareDelete(key []byte) (found bool, op *DeleteOperation) {
	id := idx.allocatorQueue.get()
	op = newDeleteOperation(idx, idx.allocators[id], true)
	if op.prepare(key) {
		return true, op
	}
	op.Abort()
	return false, nil
}

func (idx *Tree) getNodeData(node uint64) []uint64 {
	block := int(node >> blockSlotsShift)
	offset := int(node & blockSlotsOffsetMask)
	if idx.blocks[block] == nil {
		panic(fmt.Sprintf("Invalid node and block : %d %d", node, block))
	}
	return idx.blocks[block].data[offset:]
}

func (idx *Tree) getNodeFromRaw(raw uint64) (count, prefixLen int, data []uint64) {
	var node uint64
	_, node, count, prefixLen = explodeNode(raw)

	block := int(node >> blockSlotsShift)
	offset := int(node & blockSlotsOffsetMask)
	data = idx.blocks[block].data[offset:]

	if prefixLen > 0 {
		var prefixSlots int
		if prefixLen == 255 {
			prefixLen = int(data[0])
			prefixSlots = ((prefixLen + 15) >> 3)
		} else {
			prefixSlots = ((prefixLen + 7) >> 3)
		}
		data = data[prefixSlots:]
	}
	return
}

func (idx *Tree) partialSearch(searchPrefix []byte) (raw uint64, depth int) {
	root := atomic.LoadUint64(&idx.root)
	if isLeaf(root) {
		if len(searchPrefix) == 0 || byte(root) == searchPrefix[0] {
			return root, 0
		}
		return 0, 0
	}
	raw = root
	var buf []byte
searchLoop:
	for {
		_, node, count, prefixLen := explodeNode(raw)
		if depth >= len(searchPrefix) {
			return raw, depth
		}
		data := idx.getNodeData(node)
		if prefixLen > 0 {
			buf, slots := appendPrefix(buf[:0], data, int(prefixLen))
			data = data[slots:]
			for i := range buf {
				if depth+i >= len(searchPrefix) {
					// searchPrefix is part of the current node prefix
					// return current node, and depth exclusive prefix
					return raw, depth
				}
				if searchPrefix[depth+i] != buf[i] {
					return 0, 0
				}
			}
			// the whole prefix matched
			// in case the node prefix consumed all of searchPrefix, we return current node
			if depth+len(buf) >= len(searchPrefix) {
				// prefix exhausted
				return raw, depth
			}
			depth += len(buf)
		}
		k := searchPrefix[depth]
		if count >= fullAllocFrom {
			a := atomic.LoadUint64(&data[int(k)])
			if a != 0 {
				if isLeaf(a) {
					return a, depth
				}
				raw = a
				depth++
				continue searchLoop
			}
			return 0, 0
		}
		var (
			b byte
			a uint64
		)
		for i := range data[:count] {
			a = atomic.LoadUint64(&data[i])
			b = byte(a)
			if b >= k {
				break
			}
		}
		if b == k {
			if isLeaf(a) {
				return a, depth
			}
			raw = a
			depth++
			continue searchLoop
		}
		return 0, depth
	}
}

// MaxKey returns the maximum key having the given searchPrefix, or the maximum key in the whole index if searchIndex is nil.
// Maximum means the last key in the lexicographic order. If keys are uint64 in BigEndian it is also the largest number.
//
// For example, if we store temperature readings using the key "temp_TIMESTAMP" where timestamp is an 8 byte BigEndian ns timestamp
// MaxKey([]byte("temp_")) returns the last made reading.
func (idx *Tree) MaxKey(searchPrefix []byte) uint64 {
	raw, _ := idx.partialSearch(searchPrefix)
	if raw == 0 {
		return 0
	}
	if isLeaf(raw) {
		return getLeafValue(raw)
	}
	// now find the max
searchLoop:
	for {
		_, node, count, prefixLen := explodeNode(raw)
		block := int(node >> blockSlotsShift)
		offset := int(node & blockSlotsOffsetMask)
		ptr := unsafe.Pointer(&idx.blocks[block].data[offset])
		var prefixAdd int
		if prefixLen > 0 {
			if prefixLen == 255 {
				prefixLen = int(*(*uint16)(ptr))
				prefixAdd = ((prefixLen + 9) >> 3) << 3
			} else {
				prefixAdd = ((prefixLen + 7) >> 3) << 3
			}
		}
		if count >= fullAllocFrom {
			// find max, iterate from top
			p := uintptr(prefixAdd + 8*int(255))
			for k := 255; k >= 0; k-- {
				a := atomic.LoadUint64((*uint64)(unsafe.Pointer(uintptr(ptr) + p)))
				if a != 0 {
					if isLeaf(a) {
						return getLeafValue(a)
					}
					raw = a
					continue searchLoop
				}
				p += 8
			}
			return 0 // in the case node is broken...
		}
		// load top child (since they are ordered)
		p := uintptr(prefixAdd) + 8*uintptr(count-1)
		a := atomic.LoadUint64((*uint64)(unsafe.Pointer(uintptr(ptr) + p)))
		if isLeaf(a) {
			return getLeafValue(a)
		}
		raw = a
	}
}

// MinKey returns the minimum key having the given searchPrefix, or the minimum key in the whole index if searchIndex is nil.
// Minimum means the first key in the lexicographic order. If keys are uint64 in BigEndian it is also the smallest number.
func (idx *Tree) MinKey(searchPrefix []byte) uint64 {
	raw, _ := idx.partialSearch(searchPrefix)
	if raw == 0 {
		return 0
	}
	if isLeaf(raw) {
		return getLeafValue(raw)
	}
	// now find the max
searchLoop:
	for {
		_, node, count, prefixLen := explodeNode(raw)
		block := int(node >> blockSlotsShift)
		offset := int(node & blockSlotsOffsetMask)
		ptr := unsafe.Pointer(&idx.blocks[block].data[offset])
		var prefixAdd int
		if prefixLen > 0 {
			if prefixLen == 255 {
				prefixLen = int(*(*uint16)(ptr))
				prefixAdd = ((prefixLen + 9) >> 3) << 3
			} else {
				prefixAdd = ((prefixLen + 7) >> 3) << 3
			}
		}
		if count >= fullAllocFrom {
			// find min, iterate from bottom
			p := uintptr(prefixAdd)
			for k := 0; k < 256; k++ {
				a := atomic.LoadUint64((*uint64)(unsafe.Pointer(uintptr(ptr) + p)))
				if a != 0 {
					if isLeaf(a) {
						return getLeafValue(a)
					}
					raw = a
					continue searchLoop
				}
				p += 8
			}
			return 0 // in the case node is broken...
		}
		// load bottom child (since they are ordered)
		p := uintptr(prefixAdd)
		a := atomic.LoadUint64((*uint64)(unsafe.Pointer(uintptr(ptr) + p)))
		if isLeaf(a) {
			return getLeafValue(a)
		}
		raw = a
	}
}

// NewIterator returns an Iterator for iterating in order over all keys having the given prefix, or the complete index if searchPrefix is nil.
func (idx *Tree) NewIterator(searchPrefix []byte) *Iterator {
	return NewIterator(idx, searchPrefix)
}

// Count returns the number of nodes and leaves in the part of tree having the given prefix, or the whole tree if searchPrefix is nil.
func (idx *Tree) Count(searchPrefix []byte) (nodes, leaves int) {
	return NewCounter(idx).Count(searchPrefix)
}

func (idx *Tree) search(key []byte) (uint64, bool) {
	depth := 0
	root := atomic.LoadUint64(&idx.root)
	if isLeaf(root) {
		if byte(root) == key[0] {
			return getLeafValue(root), true
		}
		return 0, false
	}
	_, node, count, prefixLen := explodeNode(root)
searchLoop:
	for {
		block := node >> (blockSlotsShift)
		offset := int(node & blockSlotsOffsetMask)
		data := idx.blocks[block].data[offset:]
		var prefixSlots int
		if prefixLen > 0 {
			if prefixLen == 255 {
				prefixLen = int(data[0])
				prefixSlots = ((prefixLen + 15) >> 3)
			} else {
				prefixSlots = ((prefixLen + 7) >> 3)
			}
			// we use the optimistic approach, i.e. don't compare the prefix, simply skip the bytes, caller does a final check anyway
			depth += int(prefixLen)
			data = data[prefixSlots:]
		}
		if depth >= len(key) {
			return 0, false
		}
		k := key[depth]
		if count >= fullAllocFrom {
			a := atomic.LoadUint64(&data[int(k)])
			if a == 0 {
				return 0, false
			}
			if isLeaf(a) {
				return getLeafValue(a), true
			}
			_, node, count, prefixLen = explodeNode(a)
			depth++
			continue searchLoop
		}
		var (
			b byte
			a uint64
		)
		for i := range data[:count] {
			a = atomic.LoadUint64(&data[i])
			b = byte(a)
			if b >= k {
				break
			}
		}
		if b == k {
			if isLeaf(a) {
				return getLeafValue(a), true
			}
			_, node, count, prefixLen = explodeNode(a)
			depth++
			continue searchLoop
		}
		return 0, false
	}
}

func (idx *Tree) debugSearch(key []byte) uint64 {
	fmt.Printf("\n--- Search tree for %s ---:\n", key)
	depth := 0
	raw := atomic.LoadUint64(&idx.root)
	if isLeaf(raw) {
		if byte(raw) == key[0] {
			return getLeafValue(raw)
		}
		return 0
	}
searchLoop:
	for {
		idx.debugPrintNode(os.Stdout, raw)
		_, node, count, prefixLen := explodeNode(raw)
		block := node >> blockSlotsShift
		offset := int(node & blockSlotsOffsetMask)
		data := idx.blocks[block].data[offset:]
		var prefixSlots int
		if prefixLen > 0 {
			if prefixLen == 255 {
				prefixLen = int(data[0])
				prefixSlots = ((prefixLen + 15) >> 3)
			} else {
				prefixSlots = ((prefixLen + 7) >> 3)
			}
			// we use the optimistic approach, i.e. don't compare the prefix, simply skip the bytes, caller does a final check anyway
			depth += int(prefixLen)
			data = data[prefixSlots:]
		}
		if depth >= len(key) {
			return 0
		}
		k := key[depth]
		if count >= fullAllocFrom {
			a := atomic.LoadUint64(&data[int(k)])
			if a == 0 {
				return 0
			}
			if isLeaf(a) {
				return getLeafValue(a)
			}
			raw = a
			depth++
			continue searchLoop
		}
		var (
			b byte
			a uint64
		)
		for i := range data[:count] {
			a = atomic.LoadUint64(&data[i])
			b = byte(a)
			if b >= k {
				break
			}
		}
		if b == k {
			if isLeaf(a) {
				return getLeafValue(a)
			}
			raw = a
			depth++
			continue searchLoop
		}
		return 0
	}
}
