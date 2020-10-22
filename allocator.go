package radix

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"strconv"
	"sync/atomic"
)

// An Allocator is responsible for allocating new uint64 slots for storing tree nodes and manages the recycling of removed nodes.
// Since lookups are lock free an node might be recycled through an update operation while a reader is accessing it. To prevent the node
// from being used again until the read operation is done readers reserve the current epoch using the allocator.
// It's normally not needed to work directly with an allocator, but for bulk lookup/delete/update operations you will get
// better performance by acquiring an allocator and using it directly.
//
//  a := trie.GetAllocator()
//  for {
//    v := a.Lookup(key)
//    ...
//  }
//  trie.ReleaseAllocator(a)
//
// An allocator is not safe for concurrent use.
type Allocator struct {
	idx *Tree

	id        int
	empty     bool
	nextFree  uint64
	block     int
	blockSize uint64
	offset    uint64
	recycler  [][]recycledItem
	newBlocks chan int

	manager     *epochManager
	globalEpoch *uint64
	reserveSlot *uint64
	counter     uint64

	// the local item counter, could be negative
	itemCounter int64

	stats *OperationStats

	// cache padding
	_ [8]uint64
}

type recycledItem struct {
	n uint64
	e uint64
}

func newAllocator(idx *Tree, id int, blocksize uint64, newBlocks chan int) *Allocator {
	if id > idx.manager.n {
		panic(fmt.Sprintf("ID %d overflows number of allocators in manager", id))
	}
	return &Allocator{
		idx:         idx,
		manager:     idx.manager,
		id:          id,
		reserveSlot: idx.manager.reservations[id],
		globalEpoch: idx.manager.globalEpoch,
		empty:       true,
		nextFree:    blocksize,
		blockSize:   blocksize,
		recycler:    make([][]recycledItem, 258),
		newBlocks:   newBlocks,
		stats:       new(OperationStats),
	}
}

// FlushStats updates tree global stats with the allocators private counters.
func (a *Allocator) FlushStats() {
	atomic.AddInt64(&a.idx.liveObjects, a.itemCounter)
	a.itemCounter = 0
}

// unused reports the number of bytes unused in the current block
func (a *Allocator) unused() (bytes int) {
	return int(a.blockSize - a.nextFree)
}

// Lookup searches the tree for a key and if a candidate leaf is found returns the value stored with it.
// The caller needs to verify that the candidate is equal to the lookup key, since if the key didn't exist, the candidate
// will be another key sharing a prefix with the lookup key.
func (a *Allocator) Lookup(key []byte) (value uint64, found bool) {
	a.startOp()
	value, found = a.idx.search(key)
	a.endOp()
	return
}

// PrepareUpdate prepares an update operation for the given key. It returns the present value found while searching for the key
// and an UpdateOperation used to finalize or abort the operation.
// If the present value is 0 it's a simple insert operation:
//
//  v, op := a.PrepareUpdate([]byte("hello"))
//  if v == 0 {
//      // this is an insert
//      if inserted := op.Finalize(3); !inserted {
//         // write conflict, we need to restart with a new PrepareUpdate
//      }
//  }
//
// If the value is non-zero the caller must fetch the full key for this value into the op.FetchedKey field. The caller should compare this value
// to the update key (op.Key) and set op.Match accordingly before finalizing.
// Since operation structs are pooled it's best to reuse the slice at op.FetchedKey.
//
//  if v != 0 {
//    op.FetchedKey = fetchKeyFromSomeStorage(op.FetchedKey[:0], v)
//    if bytes.Equal(op.Key, op.FetchedKey) {
//       // this is an update
//       // do some update logic
//       op.Match = true
//       if updated := op.Finalize(3); !updated {
//          // write conflict, we need to restart with a new PrepareUpdate
//       }
//    } else {
//      // this is an insert
//      // do some insert logic
//      if inserted := op.Finalize(3); !inserted {
//          // write conflict, we need to restart with a new PrepareUpdate
//      }
//    }
//  }
//
func (a *Allocator) PrepareUpdate(key []byte) (found bool, op *UpdateOperation) {
	op = newUpdateOperation(a.idx, a, false)
	return op.prepareUpdate(key), op
}

// PrepareDelete prepares an delete operation on the given key. It returns the present value found while searching for the key
// and an DeleteOperation used to finalize or abort the operation.
// If returns a nil op if and only if v is 0. In this case there is nothing to delete.
//
//  v, op := a.PreparDelete([]byte("hello"))
//  if v == 0 {
//      // key didn't exist
//  }
//
// If the value is non-zero the caller must fetch the full key for this value into the op.FetchedKey field. The caller should compare this value
// to the update key (op.Key) to verify we are deleting the right key. If they don't match, call op.Abort.
// Since operation structs are pooled it's best to reuse the slice at op.FetchedKey.
//
//  if v != 0 {
//    op.FetchedKey = fetchKeyFromSomeStorage(op.FetchedKey[:0], v)
//    if bytes.Equal(op.Key, op.FetchedKey) {
//       // it was a match, go ahead with delete
//       if deleted := op.Finalize(); !deleted {
//         // write conflict, we need to restart with a new PrepareDelete
//       }
//    } else {
//      // wrong key, abort
//      op.Abort()
//    }
//  }
//
func (a *Allocator) PrepareDelete(key []byte) (found bool, op *DeleteOperation) {
	op = newDeleteOperation(a.idx, a, false)
	if op.prepare(key) {
		return true, op
	}
	op.Abort()
	return false, nil

}

// startOp fetches the current global epoch and makes an allocator specific reservation with this epoch, meaning that only memory blocks recycled before this epoch are safe to reuse.
func (a *Allocator) startOp() {
	atomic.StoreUint64(a.reserveSlot, atomic.LoadUint64(a.globalEpoch))
}

// endOp clears the reservation made by a previous call to startOp
func (a *Allocator) endOp() {
	atomic.StoreUint64(a.reserveSlot, maxUint64)
}

func (a *Allocator) recycle(node uint64, slots int) {
	if slots > len(a.recycler) {
		return
	}
	e := atomic.LoadUint64(a.globalEpoch)
	a.recycler[slots] = append(a.recycler[slots], recycledItem{node, e})
	a.counter++
	if a.counter >= 1000 {
		atomic.AddUint64(a.globalEpoch, 1)
		a.counter = 0
		a.FlushStats()
	}
	//a.slotsReleased += uint64(slots)
}

func (a *Allocator) printStats(out io.Writer, i int) {
	if a.empty {
		fmt.Fprintf(out, "Allocator %d - EMPTY (blocksize %d)\n", i, a.blockSize)
	} else {
		fmt.Fprintf(out, "Allocator %d -  current block %d (offset %d, blocksize %d), next free position at %d\n", i, a.block, a.offset, a.blockSize, a.nextFree)
	}
	for j, k := range a.recycler {
		if len(k) > 0 || cap(k) > 0 {
			fmt.Fprintf(out, "    recycled size %d - %d entries (cap %d)\n", j, len(k), cap(k))
		}
	}
}

// newNode returns a node with room for the given number of slots, node is a global offset in 8 byte multiples
func (a *Allocator) newNode(slots int) (n uint64) {
	minReserved := a.manager.minReservation()
	for t := slots; t < len(a.recycler); t++ {
		l := len(a.recycler[t])
		if l > 0 {
			for i, item := range a.recycler[t] {
				if item.e < minReserved {
					// pick this item
					n = item.n
					a.recycler[t][i] = a.recycler[t][l-1]
					a.recycler[t] = a.recycler[t][:l-1]

					unusedSlots := t - slots
					if unusedSlots >= 2 {
						n2 := n + uint64(slots)
						a.recycler[unusedSlots] = append(a.recycler[unusedSlots], recycledItem{n2, item.e})
						//a.slotsReleased += uint64(unusedSlots)
					}
					//a.slotsReused += uint64(slots)
					return n
				}
			}
		}
		// skip the slots+1 size, since this would leave us with 1 lost unused slot
		if t == slots {
			t++
		}
	}
	return a.allocateNode(slots)
}

// allocateNode allocates a new block of memory and returns it's global offset counted in 8 byte multiples
func (a *Allocator) allocateNode(slots int) (n uint64) {
	// a.offset and a.nextFree are still in bytes
	n = (a.offset + a.nextFree) >> 3
	a.nextFree += uint64(slots) * 8
	if a.nextFree >= blockSize {
		a.block = <-a.newBlocks
		a.offset = uint64(a.block) * blockSize
		// avoid the 0 slot in block 0 !
		if a.offset == 0 {
			a.offset = 8
		}
		n = a.offset >> 3
		a.nextFree = uint64(slots) * 8
		a.empty = false
	}
	//a.slotsAllocated += uint64(slots)
	return
}

func (a *Allocator) loadState(reader *bufio.Reader) error {
	// reset recyclers
	for i := range a.recycler {
		a.recycler[i] = a.recycler[i][:0]
	}
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			return err
		}
		// an single \n means allocator section is over
		if len(line) == 1 {
			return nil
		}
		space := bytes.IndexByte(line, ' ')
		if space < 0 {
			return fmt.Errorf("Corrupted state file, ignoring bad line %v", line)
		}
		// trim \n !
		values := line[space+1 : len(line)-1]
		v, _ := strconv.ParseUint(string(values), 10, 64)

		switch string(line[:space]) {
		case "empty":
			a.empty = bytes.Equal(line[space+1:], []byte("true"))
		case "next_free":
			a.nextFree = v
		case "block":
			a.block = int(v)
		case "block_size":
			a.blockSize = v
		case "offset":
			a.offset = v
		case "recycler":
			// recycler <size> <count>
			space = bytes.IndexByte(values, ' ')
			if space < 0 {
				return fmt.Errorf("Corrupted state file, ignoring bad line %v", line)
			}
			size, _ := strconv.Atoi(string(values[:space]))
			count, _ := strconv.Atoi(string(values[space+1:]))
			if count <= cap(a.recycler[size]) {
				a.recycler[size] = a.recycler[size][0:count]
			} else {
				a.recycler[size] = make([]recycledItem, count)
			}
			var buf [8]byte
			for i := 0; i < count; i++ {
				_, err := io.ReadFull(reader, buf[:])
				if err != nil {
					return err
				}
				a.recycler[size][i] = recycledItem{binary.LittleEndian.Uint64(buf[:]), 0}
			}

		}
	}
}

func (a *Allocator) writeState(out io.Writer) (int, error) {
	written := 0

	// write metadata: blocksize, number of blocks, root node location
	n, err := out.Write([]byte(fmt.Sprintf("empty %v\nnext_free %d\nblock %d\nblock_size %d\noffset %d\n", a.empty, a.nextFree, a.block, a.blockSize, a.offset)))
	if err != nil {
		return n, err
	}
	written += n

	var buf []byte

	// write recyclers
	for i := range a.recycler {

		// skip empty lists
		if len(a.recycler[i]) == 0 {
			continue
		}

		n, err := out.Write([]byte(fmt.Sprintf("recycler %d %d\n", i, len(a.recycler[i]))))
		if err != nil {
			return n, err
		}
		written += n

		sz := 8 * len(a.recycler[i])
		if cap(buf) < sz {
			buf = make([]byte, sz)
		} else {
			buf = buf[:sz]
		}

		o := 0
		for _, item := range a.recycler[i] {
			binary.LittleEndian.PutUint64(buf[o:o+8], item.n)
			o += 8
		}
		n, err = out.Write(buf)
		if err != nil {
			return n, err
		}
		written += n
	}

	// a single \n to denote end of allocator section
	n, err = out.Write([]byte("\n"))
	if err != nil {
		return n, err
	}
	written += n

	return written, nil
}
