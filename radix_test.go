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

const URLTestFile = "testdata/DOI-2011.txt"

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

func TestInteger50MSparse(t *testing.T) {
	rand.Seed(0)
	N := 50000000
	testSliceDatabaseN(t, N, generateRandomIntegers(N))
}

func TestInteger50MDense(t *testing.T) {
	rand.Seed(0)
	N := 50000000
	testSliceDatabaseN(t, N, fillIntegers(N))
}

func TestURL(t *testing.T) {
	db := loadURLDatabase(t)
	testSliceDatabaseN(t, len(db), db)
}

func TestCrash(t *testing.T) {
	db := [][]byte{
		[]byte("0\x00"),
		[]byte("h0\x00"),
	}
	testSliceDatabaseN(t, len(db), db)
}

func TestRepeatedBuild(t *testing.T) {
	db := loadURLDatabase(t)
	db = db[:10000]
	for i := 0; i < 100; i++ {
		clearGlobalStats()
		trie := buildTreeFromDB(t, db, 8)
		trie.Admin(os.Stdout, []string{"info"})
		lookupAll(t, trie, db, 8)
		countAll(t, trie)
		deleteAll(t, trie, db, 8)
	}
}

func testSliceDatabaseN(t *testing.T, N int, database [][]byte) {
	num := runtime.NumCPU()

	// --- Build ---
	trie := buildTreeFromDB(t, database, num)

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
	clearGlobalStats()
	deleteAll(t, trie, database, 8)
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
func buildTreeFromDB(t *testing.T, database [][]byte, num int) *Tree {
	trie := NewTree(nil)
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
				v, op := a.PrepareUpdate(key)
				if v != 0 {
					if bytes.Equal(key, database[v-1]) {
						// duplicate key in database, abort
						op.Abort()
						t.Logf("Duplicate key in test database : %s (it should be unique)\n", key)
						continue
					} else {
						op.Match = false
						op.FetchedKey = database[v-1]
					}
				}

				if !op.Finalize(uint64(from + i + 1)) {
					fmt.Printf("Write conflict, restarting\n")
					goto S
				}
			}
			trie.ReleaseAllocator(a)
			wg.Done()
		}()
	}
	wg.Wait()
	stop := time.Since(start)
	fmt.Printf("%d inserts in %v. %.2f M per second\n", N, stop, float64(N)/stop.Seconds()/1000000)
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
				if result := a.Lookup(key); result != uint64(from+i+1) {
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
				v, op := a.PrepareDelete(database[i])
				if v != 0 {
					if bytes.Equal(database[i], database[v-1]) {
						if !op.Finalize() {
							fmt.Printf("Write conflict, restarting\n")
							goto S
						}
					} else {
						t.Logf("Delete: Mismatch %s != %s", database[i], database[v-1])
						op.Abort()
					}
				} else {
					t.Logf("Delete: Not found ! %s (length %d)", database[i], len(database[i]))
					v := a.Lookup(database[i])
					t.Logf("Lookup for same key gave v == %d", v)
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

func loadURLDatabase(t *testing.T) [][]byte {
	if urlDatabase != nil {
		return urlDatabase
	}
	f, err := os.Open(URLTestFile)
	if err != nil {
		t.Skipf("Could not load URL test file %s, aborting test", URLTestFile)
		return nil
	}
	defer f.Close()
	t.Logf("Loading URL:s from %s...", URLTestFile)
	scanner := bufio.NewScanner(f)
	var size int
	for scanner.Scan() {
		r := scanner.Text()
		key := make([]byte, len(r)+1)
		copy(key, r)
		urlDatabase = append(urlDatabase, key)
		size += len(r)
	}
	t.Logf("Done. %d URLS loaded (total %d bytes, average URL size %d bytes).", len(urlDatabase), size, size/len(urlDatabase))
	return urlDatabase
}

// fort short prefixes the safe version is almost as fast, but it grows linearly with prefix length, while the unsafe version is almost as fast for any prefix length
var (
	dst    = make([]uint64, 2)
	prefix = []byte("hellohiii")
)

func BenchmarkStorePrefix(b *testing.B) {
	for i := 0; i < b.N; i++ {
		storePrefixSafe(dst, prefix)
	}
}

func BenchmarkStorePrefixUnsafe(b *testing.B) {
	for i := 0; i < b.N; i++ {
		storePrefix(dst, prefix)
	}
}

func BenchmarkLoadPrefix(b *testing.B) {
	dst := make([]uint64, 1)
	prefix := []byte("hellohi")
	storePrefixSafe(dst, prefix)
	buf := make([]byte, len(prefix))
	for i := 0; i < b.N; i++ {
		loadPrefixSafe(buf, dst, len(prefix))
	}
}

func BenchmarkLoadPrefixUnsafe(b *testing.B) {
	dst := make([]uint64, 1)
	prefix := []byte("hellohi")
	storePrefix(dst, prefix)
	buf := make([]byte, len(prefix))
	for i := 0; i < b.N; i++ {
		appendPrefix(buf, dst, len(prefix))
	}
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
	fmt.Printf("Generating database with integers [0 ... %d] ... ", N)
	for i := 0; i < N; i++ {
		b := buf[i*8 : i*8+8]
		binary.BigEndian.PutUint64(b, uint64(i))
		database[i] = b
	}
	fmt.Printf("done\n")
	return database
}

func generateRandomIntegers(N int) [][]byte {
	database := make([][]byte, N)
	// allocate one big slice to make sure all values are adjacent in memory
	buf := make([]byte, N*8)
	fmt.Printf("Generating %d random uint64 numbers... ", N)
	for i := range database {
		b := buf[i*8 : i*8+8]
		binary.BigEndian.PutUint64(b, rand.Uint64())
		database[i] = b
	}
	fmt.Printf("done\n")
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
