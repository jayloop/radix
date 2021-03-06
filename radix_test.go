package radix

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestInteger1MDense(t *testing.T) {
	rand.Seed(0)
	N := 1000000
	testSliceDatabaseN(t, N, fillIntegers(N))
}

func TestInteger1MSparse(t *testing.T) {
	rand.Seed(0)
	N := 1000000
	testSliceDatabaseN(t, N, generateRandomIntegers(N))
}

func TestInteger10MSparse(t *testing.T) {
	rand.Seed(0)
	N := 10000000
	testSliceDatabaseN(t, N, generateRandomIntegers(N))
}

func TestInteger10MDense(t *testing.T) {
	rand.Seed(0)
	N := 10000000
	testSliceDatabaseN(t, N, fillIntegers(N))
}

func TestRepeatedInsertDelete(t *testing.T) {
	db := loadURLDatabase(t)
	db = db[:10000]
	num := runtime.NumCPU()
	trie := NewTree(nil)
	for i := 0; i < 100; i++ {
		insertAll(trie, db, num, true)
		lookupAll(t, trie, db, num)
		countAll(t, trie)
		deleteAll(t, trie, db, num)
	}
}

func testSliceDatabaseN(t *testing.T, N int, db [][]byte) {
	num := runtime.NumCPU()
	trie := buildTreeFromDB(db, num, true)
	trie.Admin(os.Stdout, []string{"info"})

	lookupAll(t, trie, db, num)
	countAll(t, trie)
	iterateAll(t, trie)
	testMinMax(t, trie, db)

	deleteAll(t, trie, db, num)
	trie.Admin(os.Stdout, []string{"info"})
	trie.Close()
}

func testMinMax(t *testing.T, trie *Tree, db [][]byte) {
	min, ok := trie.MinKey(nil)
	assertTrue(t, ok)
	max, ok := trie.MaxKey(nil)
	assertTrue(t, ok)
	fmt.Printf("Min key : %x, Max key : %x\n", db[min], db[max])
	assertTrue(t, bytes.Compare(db[min], db[max]) == -1)
}

func iterateAll(t *testing.T, trie *Tree) {
	start := time.Now()
	i := trie.NewIterator(nil)
	count := 0
	for i.Next() {
		_ = i.Value()
		count++
	}
	stop := time.Since(start)
	fmt.Printf("%d items iterated in %v. %.2f M per second\n", count, stop, float64(count)/stop.Seconds()/1000000)
	assertTrue(t, count == trie.Len())
}

func countAll(t *testing.T, trie *Tree) {
	start := time.Now()
	c := NewCounter(trie)
	nodes, leaves := c.Count(nil)
	stop := time.Since(start)
	fmt.Printf("%d leaves and %d nodes counted in %v (nodes with prefix %d, total prefix bytes: %d)\n", leaves, nodes, stop, c.NodesWithPrefix, c.TotalPrefixBytes)
	assertTrue(t, leaves == trie.Len())
}

func buildTreeFromDB(database [][]byte, num int, bench bool) *Tree {
	trie := NewTree(&Options{
		NumAllocators: num,
	})
	insertAll(trie, database, num, bench)
	return trie
}

func insertAll(trie *Tree, database [][]byte, num int, bench bool) {
	var (
		start = time.Now()
		wg    sync.WaitGroup
		N     = len(database)
		part  = N / num
	)
	for j := 0; j < num; j++ {
		wg.Add(1)
		from, to := j*part, (j+1)*part
		if j == num-1 {
			to = N
		}
		go func() {
			a := trie.GetAllocator()
			for i, key := range database[from:to] {
			S:
				found, op := a.PrepareUpdate(key)
				if found {
					if bytes.Equal(key, database[op.Value]) {
						// duplicate key in database, abort
						op.Abort()
						fmt.Printf("Duplicate key in test database : %s (it should be unique)\n", key)
						continue
					} else {
						op.Match = false
						op.FetchedKey = database[op.Value]
					}
				}
				if !op.Finalize(uint64(from + i)) {
					goto S
				}
			}
			a.FlushStats()
			trie.ReleaseAllocator(a)
			wg.Done()
		}()
	}
	wg.Wait()
	stop := time.Since(start)
	if bench {
		fmt.Printf("%d inserts in %v. %.2f M per second\n", N, stop, float64(N)/stop.Seconds()/1000000)
	}
}

func lookupAll(t *testing.T, trie *Tree, database [][]byte, num int) {
	var (
		start = time.Now()
		N     = len(database)
		part  = N / num
		wg    sync.WaitGroup
	)
	for j := 0; j < num; j++ {
		wg.Add(1)
		from, to := j*part, (j+1)*part
		if j == num-1 {
			to = N
		}
		go func() {
			a := trie.GetAllocator()
			for i, key := range database[from:to] {
				if result, _ := a.Lookup(key); result != uint64(from+i) {
					t.Errorf("Lookup failed! Got %d expected %d, while searching for %s", result, i+1, key)
					c := NewCounter(trie)
					nodes, leaves := c.Count(nil)
					fmt.Printf("nodes %d, leaves %d\n", nodes, leaves)
					panic("done")
				}
			}
			trie.ReleaseAllocator(a)
			wg.Done()
		}()
	}
	wg.Wait()
	stop := time.Since(start)
	fmt.Printf("%d lookups in %v. %.2f M per second\n", N, stop, float64(N)/stop.Seconds()/1000000)
}

func deleteAll(t *testing.T, trie *Tree, database [][]byte, num int) {
	var (
		start = time.Now()
		N     = len(database)
		part  = N / num
		wg    sync.WaitGroup
	)
	for j := 0; j < num; j++ {
		wg.Add(1)
		from, to := j*part, (j+1)*part
		if j == num-1 {
			to = N
		}
		go func() {
			a := trie.GetAllocator()
			for i := from; i < to; i++ {
			S:
				found, op := a.PrepareDelete(database[i])
				if found {
					if bytes.Equal(database[i], database[op.Value]) {
						if !op.Finalize() {
							fmt.Printf("Write conflict, restarting\n")
							goto S
						}
					} else {
						t.Logf("Delete: Mismatch %s != %s", database[i], database[op.Value])
						op.Abort()
					}
				} else {
					t.Logf("Delete: Not found ! %s (length %d)", database[i], len(database[i]))
					op.Abort()
				}
			}
			trie.ReleaseAllocator(a)
			wg.Done()
		}()
	}
	wg.Wait()
	stop := time.Since(start)
	fmt.Printf("%d deletes in %v. %.2f M per second\n", N, stop, float64(N)/stop.Seconds()/1000000)
	if trie.Len() != 0 {
		t.Errorf("Tree was not empty after deleting all keys!")
	}
}

var (
	testFiles     = make(map[string][][]byte)
	testFileBytes = make(map[string]int64)
)

func loadTestFile(path string, expectedSize int) (db [][]byte, bytes int64) {
	if db, ok := testFiles[path]; ok {
		return db, testFileBytes[path]
	}
	if expectedSize > 0 {
		// preallocate to avoid repeated slice capacity growth
		db = make([][]byte, 0, expectedSize)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, 0
	}
	defer f.Close()
	reader := bufio.NewReader(f)
	for {
		row, err := reader.ReadSlice('\n')
		if err != nil {
			break
		}
		row = row[:len(row)-1]
		if len(row) == 0 {
			continue
		}
		key := make([]byte, len(row)+1)
		copy(key, row)
		db = append(db, key)
		bytes += int64(len(key))
	}
	testFiles[path] = db
	testFileBytes[path] = bytes
	return
}

func loadURLDatabase(t *testing.T) [][]byte {
	db, _ := loadTestFile("testdata/DOI-2011.txt", 17000000)
	if db == nil {
		t.Skip("Testfile DOI-2011.txt not found")
	}
	return db
}

func BenchmarkMinReservation(b *testing.B) {
	m := newEpochManager(8)
	for i := 0; i < b.N; i++ {
		m.minReservation()
	}
}

func fillIntegers(N int) [][]byte {
	database := make([][]byte, N)
	// allocate one big slice to make sure all values are adjacent in memory
	buf := make([]byte, N*8)
	for i := 0; i < N; i++ {
		b := buf[i*8 : i*8+8]
		binary.BigEndian.PutUint64(b, uint64(i))
		database[i] = b
	}
	return database
}

func generateRandomIntegers(N int) [][]byte {
	database := make([][]byte, N)
	// allocate one big slice to make sure all values are adjacent in memory
	buf := make([]byte, N*8)
	for i := range database {
		b := buf[i*8 : i*8+8]
		binary.BigEndian.PutUint64(b, rand.Uint64())
		database[i] = b
	}
	return database
}

func generateRandomBytes(N int) [][]byte {
	database := make([][]byte, N)
	fmt.Printf("Generating %d random byte slices... ", N)
	r := rand.New(rand.NewSource(0))
	for i := range database {
		l := 8 + rand.Intn(128)
		b := make([]byte, l)
		r.Read(b)
		b[l-1] = 0
		database[i] = b
	}
	fmt.Printf("done\n")
	return database
}

func assertTrue(t *testing.T, value bool, comment ...string) {
	if !value {
		if len(comment) > 0 {
			t.Errorf("Failed: %s", comment[0])
		} else {
			t.Errorf("Failed: got false, expected true")
		}
	}
}

func assertFalse(t *testing.T, value bool) {
	if value {
		t.Errorf("Failed: got true, expected false")
	}
}

func assertEqual(t *testing.T, value interface{}, expected interface{}) {
	var equal bool
	switch value.(type) {
	case []byte:
		equal = string(value.([]byte)) == string(expected.([]byte))
	case int:
		equal = value.(int) == expected.(int)
	case uint64:
		equal = value.(uint64) == expected.(uint64)
	case bool:
		equal = value.(bool) == expected.(bool)
	default:
		equal = reflect.DeepEqual(value, expected)
	}
	if !equal {
		_, file, line, _ := runtime.Caller(1)
		t.Errorf("Failed: got %v, expected %v (%s:%d)", value, expected, file, line)
	}
}
