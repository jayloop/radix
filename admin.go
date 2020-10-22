package radix

import (
	"fmt"
	"io"
	"strconv"
	"sync/atomic"

	"github.com/jayloop/table"
)

// Admin provides some index debug and admin functions for use by a CLI or terminal.
// It takes a writer and a slice with the command and arguments.
func (idx *Tree) Admin(out io.Writer, argv []string) {
	if len(argv) == 0 {
		fmt.Fprint(out, "available commands for index:\ninfo\nroot\nnode <id>\nblocks\nallocators\n")
		return
	}
	switch argv[0] {
	case "info":
		t := table.New("NAME", "VALUE")
		stats := make(map[string]interface{})
		idx.Stats(stats)
		t.Precision(1, 2)
		for k, v := range stats {
			t.Row(k, v)
		}
		t.Sort(0)
		t.Print(out)

	case "allocators":
		switch {
		case len(argv) == 1:
			// withdraw all allocators
			var a []int
			for i := 0; i < len(idx.allocators); i++ {
				a = append(a, idx.allocatorQueue.get())
			}
			for i, a := range idx.allocators {
				a.printStats(out, i)
			}
			// release allocators again
			for i := range a {
				idx.allocatorQueue.put(a[i])
			}

			// case argv[1] == "reservations":
			// 	printReservations(out)
		}

	case "blocks":
		next := int(atomic.LoadUint64(&idx.nextFreeBlock))
		t := table.New("BLOCK", "STATUS")
		t.FormatHeader(table.Format(table.Yellow))
		for i := 0; i <= next; i++ {
			if i == next {
				t.Row(i, "allocated")
			} else {
				t.Row(i, "in use")
			}
		}
		t.Print(out)

	case "root":
		root := atomic.LoadUint64(&idx.root)
		if root == 0 {
			fmt.Fprint(out, "Index is empty\n")
			return
		}
		idx.debugPrintNode(out, root)

	case "node":
		if len(argv) != 2 {
			fmt.Fprint(out, "usage: node <id>\n")
			return
		}
		node, _ := strconv.Atoi(argv[1])
		idx.debugPrintNode(out, uint64(node))

	default:
		fmt.Fprintf(out, "Unknown command '%s'\n", argv[0])
	}
}

func (idx *Tree) debugPrintNode(out io.Writer, node uint64) {
	if isLeaf(node) {
		debugPrintLeaf(out, node)
		return
	}

	key, value, count, prefixLen := explodeNode(node)

	block := value >> blockSlotsShift
	offset := value & blockSlotsOffsetMask

	if int(block) > len(idx.blocks) || int(offset) > len(idx.blocks[block].data) {
		fmt.Fprintf(out, "Invalid node\n")
		return
	}

	data := idx.blocks[block].data[offset:]

	fmt.Fprintf(out, "NODE %d (%064b): key=%s, data=%d, count=%d, prefixLen=%d\n", node, node, printableChar(key), value, count, prefixLen)

	var prefix []byte
	if prefixLen > 0 {
		prefix, slots := appendPrefix(prefix, data, prefixLen)
		fmt.Fprintf(out, "Prefix: %s\n", prefix)
		data = data[slots:]
	}

	for i := range data[:count] {
		a := atomic.LoadUint64(&data[i])
		if isLeaf(a) {
			debugPrintLeaf(out, a)
		} else {
			key, value, count, prefixLen := explodeNode(a)
			fmt.Fprintf(out, "[%s] NODE: 0x%016x %d (data=%d, count=%d, prefixLen=%d)\n", printableChar(key), a, a, value, count, prefixLen)
		}
	}
}

func printableChar(c byte) string {
	switch {
	case c >= 32 && c <= 126:
		return "'" + string(c) + "'"
	case c >= '0' && c <= '9', c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z':
		return "'" + string(c) + "'"
	default:
		return fmt.Sprintf("%02x", c)
	}
}

func debugPrintLeaf(out io.Writer, leaf uint64) {
	key := byte(leaf)
	fmt.Fprintf(out, "[%s] LEAF: 0x%016x %d (value=%d)\n", printableChar(key), leaf, leaf, getLeafValue(leaf))
}
