package radix

// Bit 9 is used to distinguish between leaves (1) and nodes (0)

// Node structure:
// [ count (8 bits) - prefix length (8 bits) - value (39 bits) - 0 (1 bit) - key (8 bits) ]

// Leaf structure:
// [ value (55 bits) - 1 (1 bit) - key (8 bits) ]

const (
	valueMask2  = (1 << 39) - 1
	countShift  = 56
	prefixShift = 48
	valueShift  = 9
	leafBit     = 1 << 8
)

// buildNode builds a 8 byte node from its key, value (data pointer), number of children and prefix length
func buildNode(key byte, value uint64, children, prefixLen int) uint64 {
	if prefixLen > 255 {
		prefixLen = 255
	}
	return uint64(children-1)<<countShift | uint64(prefixLen)<<prefixShift | (value << valueShift) | uint64(key)
}

// explodeNode explodes the 8 byte node data into it's four values
func explodeNode(node uint64) (key byte, value uint64, children, prefixLen int) {
	return byte(node), (node >> valueShift) & valueMask2, 1 + int(byte(node>>countShift)), int(byte(node >> prefixShift))
}

func getNodeValue(node uint64) uint64 {
	return (node >> valueShift) & valueMask2
}

// buildLeaf builds a 8 byte leaf from its key and value (max 55 bits, any overflow is truncated)
func buildLeaf(key byte, value uint64) uint64 {
	return value<<valueShift | leafBit | uint64(key)
}

func updateLeafKey(leaf uint64, key byte) uint64 {
	return (leaf>>8)<<8 | uint64(key)
}

// getLeafValue returns the 55 bits of information stored at a leaf, excluding the LSB bit
func getLeafValue(node uint64) (value uint64) {
	return node >> 9
}

func isLeaf(node uint64) bool {
	return node&leafBit == leafBit
}
