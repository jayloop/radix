package radix

import (
	"testing"
)

func TestSlots(t *testing.T) {
	N := 100
	s := newSlots(N)
	assertTrue(t, s.available() == N)
	reserved := make(map[int]bool)
	// remove all items, asserting they are not reserved
	for i := 0; i < N; i++ {
		a := s.get()
		assertTrue(t, a >= 0 && a < N)
		assertFalse(t, reserved[a])
		reserved[a] = true
	}
	assertTrue(t, s.available() == 0)
	// return all items
	for i := 0; i < N; i++ {
		s.put(i)
	}
	assertTrue(t, s.available() == N)
}
