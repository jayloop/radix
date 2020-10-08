package radix

import "sync/atomic"

const (
	maxUint64 = 1<<64 - 1
)

type epochManager struct {
	n            int
	globalEpoch  *uint64
	slots        []*reserveSlot
	reservations []*uint64
}

type reserveSlot struct {
	// cacheline padding, very helpful for fast lookups
	_ [7]uint64
	r uint64
}

func newEpochManager(n int) *epochManager {
	m := &epochManager{
		n:            n,
		globalEpoch:  new(uint64),
		slots:        make([]*reserveSlot, n),
		reservations: make([]*uint64, n),
	}
	for i := range m.reservations {
		s := &reserveSlot{
			r: maxUint64,
		}
		m.slots[i] = s
		m.reservations[i] = &s.r
	}
	return m
}

func (m *epochManager) minReservation() (minReservation uint64) {
	minReservation = maxUint64
	for _, r := range m.reservations {
		r := atomic.LoadUint64(r)
		if r < minReservation {
			minReservation = r
		}
	}
	return
}
