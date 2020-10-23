package radix

import (
	"sync/atomic"
	"time"
)

type counterPosition struct {
	raw       uint64
	nextChild int
}

// A Counter counts objects in a Tree
type Counter struct {
	NodesWithPrefix   int
	NodeSizes         []int
	TotalPrefixLength int
	TotalPrefixBytes  int
	LargePrefixes     int
	Elapsed           time.Duration

	idx   *Tree
	stack []counterPosition
}

// NewCounter returns a counter on tree
func NewCounter(tree *Tree) *Counter {
	return &Counter{
		idx:       tree,
		NodeSizes: make([]int, 257),
	}
}

func (c *Counter) reset() {
	c.stack = c.stack[:0]
	c.NodesWithPrefix = 0
	c.TotalPrefixLength = 0
	c.TotalPrefixBytes = 0
	c.LargePrefixes = 0
	for i := range c.NodeSizes {
		c.NodeSizes[i] = 0
	}
}

// Count counts nodes and leaves having the given prefix which may be empty
func (c *Counter) Count(searchPrefix []byte) (nodes, leaves int) {
	start := time.Now()
	c.reset()
	alloc := c.idx.GetAllocator()
	defer c.idx.ReleaseAllocator(alloc)
	alloc.startOp()
	defer alloc.endOp()
	var p counterPosition

	p.raw, _ = c.idx.partialSearch(searchPrefix)
	if p.raw == 0 {
		return
	}
	if isLeaf(p.raw) {
		leaves++
		return
	}
	nodes++

searchLoop:
	for {
		_, node, count, prefixLen := explodeNode(p.raw)
		data := c.idx.getNodeData(node)
		if prefixLen > 0 {
			prefixSlots := ((prefixLen + 7) >> 3)
			if prefixLen == 255 {
				c.LargePrefixes++
				prefixLen = int(data[0])
				prefixSlots = ((prefixLen + 15) >> 3)
			}
			c.NodesWithPrefix++
			c.TotalPrefixBytes += prefixSlots * 8
			c.TotalPrefixLength += prefixLen
			data = data[prefixSlots:]
		}

		c.NodeSizes[count]++

		for k := p.nextChild; k < count; k++ {
			a := atomic.LoadUint64(&data[k])
			if a != 0 {
				if isLeaf(a) {
					leaves++
				} else {
					nodes++
					next := k + 1
					if count >= fullAllocFrom {
						// since it's a node-256 we need to check if there are any more non-zero children in this node
						for ; next < count; next++ {
							if atomic.LoadUint64(&data[next]) != 0 {
								break
							}
						}
					}
					if next < count {
						c.stack = append(c.stack, counterPosition{p.raw, next})
					}
					p.raw = a
					p.nextChild = 0
					continue searchLoop
				}
			}
		}
		if len(c.stack) == 0 {
			break
		}
		p, c.stack = c.stack[len(c.stack)-1], c.stack[:len(c.stack)-1]
	}

	c.Elapsed = time.Since(start)
	return nodes, leaves
}
