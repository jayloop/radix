package radix

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/snappy"
)

// writeState writes the complete index state to the given filename.
// It assumes are allocators are locked by caller.
func (idx *Tree) writeState(out io.Writer) (int, error) {
	written := 0

	// write metadata: blocksize, number of blocks, number of allocators, root node location
	n, err := out.Write([]byte(fmt.Sprintf("objects %d\nblocksize %d\nallocated_blocks %d\nallocators %d\nroot_node %d\nepoch %d\n", atomic.LoadUint64(&idx.liveObjects), blockSize, idx.nextFreeBlock, len(idx.allocators), atomic.LoadUint64(&idx.root), atomic.LoadUint64(idx.manager.globalEpoch))))
	if err != nil {
		return 0, err
	}
	written += n

	// write allocator states
	for i := range idx.allocators {
		n, err := out.Write([]byte(fmt.Sprintf("allocator %d\n", i)))
		if err != nil {
			return n + written, err
		}
		written += n
		n, err = idx.allocators[i].writeState(out)
		if err != nil {
			return n + written, err
		}
		written += n
	}

	// compress and write index blocks concurrently
	var workers sync.WaitGroup
	type writeJob struct {
		block int
		buf   []byte
	}
	toCompress := make(chan int)
	toWrite := make(chan *writeJob, 32)
	jobPool := sync.Pool{New: func() interface{} { return &writeJob{buf: make([]byte, blockSize)} }}

	for i := 0; i < runtime.NumCPU(); i++ {
		workers.Add(1)
		go func() {
			for b := range toCompress {
				src := byteSliceFromUint64(idx.blocks[b].data)
				j := jobPool.Get().(*writeJob)
				j.buf = snappy.Encode(j.buf[0:], src)
				j.block = b
				toWrite <- j
			}
			workers.Done()
		}()
	}

	writerDone := make(chan error)
	go func() {
		// writer
		var writeTime time.Duration
		for job := range toWrite {
			n, err := out.Write([]byte(fmt.Sprintf("block %d\ncompression snappy\nsize %d\n", job.block, len(job.buf))))
			if err != nil {
				writerDone <- err
				return
			}
			written += n
			start := time.Now()
			n, err = out.Write(job.buf)
			jobPool.Put(job)
			if err != nil {
				writerDone <- err
				return
			}
			written += n
			writeTime += time.Since(start)
		}
		writerDone <- nil
	}()

	next := int(atomic.LoadUint64(&idx.nextFreeBlock))
	for i := 0; i < next; i++ {
		toCompress <- i
	}
	close(toCompress)
	workers.Wait()

	close(toWrite)
	err = <-writerDone

	return written, err
}

// loadState rebuilds index state from data previously written with writeState.
// It assumes the index is empty.
func (idx *Tree) loadState(data io.Reader) error {
	var (
		reader     = bufio.NewReader(data)
		allocators int
		nextBlock  = -1
		readBuf    []byte
	)
	// reset block map
	idx.blockMap = make(map[int]int)
	for {
		line, err := reader.ReadBytes('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		// trim \n
		line = line[:len(line)-1]
		space := bytes.IndexByte(line, ' ')
		v, _ := strconv.ParseUint(string(line[space+1:]), 10, 64)

		fmt.Printf("=> %s\n", line)

		switch string(line[:space]) {
		case "objects":
			idx.liveObjects = v

		case "blocksize":
			// in case loaded blocksize differs from existing ones, we need to throw away all blocks
			if v != blockSize {
				return fmt.Errorf("Cannot lode state file. Unsupported blocksize %d", v)
			}

		case "allocated_blocks":
			idx.nextFreeBlock = v

		case "epoch":
			atomic.StoreUint64(idx.manager.globalEpoch, v)

		case "root_node":
			idx.root = v

		case "allocators":
			allocators = int(v)
			idx.manager = newEpochManager(allocators)
			idx.allocators = make([]*Allocator, allocators)
			idx.allocatorQueue = newSlots(allocators)
			for i := range idx.allocators {
				idx.allocators[i] = newAllocator(idx, i, blockSize, idx.newBlocks)
			}

		case "allocator":
			// allocator X
			if int(v) > len(idx.allocators)-1 {
				return fmt.Errorf("Corrupted index state file? Can't load allocator id %d since there are only %d allocators", v, len(idx.allocators))
			}
			if err := idx.allocators[v].loadState(reader); err != nil {
				return err
			}
			idx.blockMap[idx.allocators[v].block] = blockAllocated

		case "block":
			// block X
			if int(v) > len(idx.blocks)-1 {
				return fmt.Errorf("Corrupted index state file? Can't load data block id %d since there are only %d blocks", v, len(idx.blocks))
			}
			nextBlock = int(v)

		case "compression":

		case "size":
			if nextBlock < 0 {
				return fmt.Errorf("Corrupted index state file? size header before first block")
			}
			if cap(readBuf) >= int(v) {
				readBuf = readBuf[:int(v)]
			} else {
				readBuf = make([]byte, int(v))
			}

			if _, err := io.ReadFull(reader, readBuf); err != nil {
				return err
			}

			idx.blocks[nextBlock] = &nodeBlock{
				data: make([]uint64, blockSize>>3),
			}

			dst := byteSliceFromUint64(idx.blocks[nextBlock].data)
			if _, err := snappy.Decode(dst, readBuf); err != nil {
				return fmt.Errorf("Corrupted index state file? Decompressing data block failed : %w", err)
			}

			if idx.blockMap[int(nextBlock)] != blockAllocated {
				idx.blockMap[int(nextBlock)] = blockWritten
			}
		}
	}
	return nil
}
