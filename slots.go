package radix

import (
	"runtime"
	"sync/atomic"
)

type item struct {
	// cacheline padding
	_ [8]uint64
	v uint64
}

// slots is a cache friendly Get/Put pool with a small fixed number of items [0 - n).
// It's used internally for keeping track of available allocators.
type slots struct {
	slots []item
}

// newSlots returns a new Slots with n items.
// From start, all items are all in the pool (available)
func newSlots(n int) *slots {
	return &slots{
		slots: make([]item, n),
	}
}

// free counts the number of currently available items
func (s *slots) available() (c int) {
	for i := 0; i < len(s.slots); i++ {
		if atomic.LoadUint64(&s.slots[i].v) == 0 {
			c++
		}
	}
	return
}

// get returns an item from the pool.
// A call will block if no items are available.
func (s *slots) get() int {
	for {
		for i := 0; i < len(s.slots); i++ {
			// this looks silly, but by simple benchmarks it looks like this if still faster since CompareAndSwap is a relatively slow operation
			a := atomic.LoadUint64(&s.slots[i].v)
			if a == 0 && atomic.CompareAndSwapUint64(&s.slots[i].v, 0, 1) {
				return i
			}
		}
		runtime.Gosched()
	}
}

// put marks item i as available again.
func (s *slots) put(i int) {
	atomic.StoreUint64(&s.slots[i].v, 0)
}
