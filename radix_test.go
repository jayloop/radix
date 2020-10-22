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

func TestRepeatedBuild(t *testing.T) {
	db := loadURLDatabase(t)
	db = db[:10000]
	for i := 0; i < 100; i++ {
		trie := buildTreeFromDB(db, 8, true)
		lookupAll(t, trie, db, 8)
		countAll(t, trie)
		deleteAll(t, trie, db, 8)
	}
}

func testSliceDatabaseN(t *testing.T, N int, database [][]byte) {
	num := runtime.NumCPU()

	// --- Build ---
	trie := buildTreeFromDB(database, num, true)

	// --- Lookup ---
	lookupAll(t, trie, database, num)

	// --- Count ---
	countAll(t, trie)

	// --- Iterate ---
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

	trie.Admin(os.Stdout, []string{"info"})

	// --- Delete ---
	deleteAll(t, trie, database, num)
	trie.Admin(os.Stdout, []string{"info"})

	trie.Close()
}

func countAll(t *testing.T, trie *Tree) {
	start := time.Now()
	c := NewCounter(trie)
	nodes, leaves := c.Count(nil)
	stop := time.Since(start)
	fmt.Printf("%d leaves and %d nodes counted in %v (nodes with prefix %d, total prefix bytes: %d, node256: %d)\n", leaves, nodes, stop, c.NodesWithPrefix, c.TotalPrefixBytes, c.Node256)
	assertTrue(t, leaves == trie.Len())
}

func buildTreeFromDB(database [][]byte, num int, bench bool) *Tree {
	trie := NewTree(&Options{
		NumAllocators: num,
	})
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
	return trie
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

var urlDatabase [][]byte

func loadTestFile(path string) (db [][]byte) {
	f, err := os.Open(path)
	if err != nil {
		return nil
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
	}
	return
}

func loadURLDatabase(t *testing.T) [][]byte {
	if urlDatabase != nil {
		return urlDatabase
	}
	urlDatabase = loadTestFile("testdata/DOI-2011.txt")
	if urlDatabase == nil {
		t.Skip("Testfile DOI-2011.txt not found")
	}
	return urlDatabase
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
