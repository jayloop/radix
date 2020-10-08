package radix

func storePrefixSafe(dst []uint64, prefix []byte) {
	if len(prefix) >= 255 {
		dst[0] = uint64(len(prefix))
		dst = dst[1:]
	}
	k := 0
	l := len(prefix)
	for i := range dst {
		var a uint64
		for j := 0; j < 8; j++ {
			a = (a << 8) | uint64(prefix[k])
			k++
			if k == l {
				a = a << (8 * (7 - uint64(j)))
				//dst[i] = a
				break
			}
		}
		dst[i] = a
	}
}

// loadPrefix loads the prefix of given length stored at src and appends it to dst
func loadPrefixSafe(dst []byte, src []uint64, length int) ([]byte, int) {
	// prefix is stored as 8-byte multiples (uint64)
	var (
		slots int
		dl    int
	)
	if length == 255 {
		// prefixLen == 255 means it's >= 255, the real length is for simplicity stored in the next 8 byte slot (the unsafe version uses only 2 bytes for length)
		length = int(src[0])
		slots = (length + 7) >> 3
		dl = slots << 3
		src = src[1 : slots+1]
		slots++
	} else {
		slots = (length + 7) >> 3
		dl = slots << 3
		src = src[:slots]
	}
	// fix size of dst
	if cap(dst) >= dl {
		dst = dst[:dl]
	} else {
		dst = make([]byte, dl)
	}
	k := 0
	for _, a := range src {
		//binary.BigEndian.PutUint64(dst[k:k+8], a)
		for j := 7; j >= 0; j-- {
			dst[k+j] = byte(a)
			a = a >> 8
		}
		k += 8
	}
	return dst[:length], slots
}
