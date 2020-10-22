package radix

import (
	"math/rand"
	"runtime"
	"testing"
)

var workers = runtime.NumCPU()

func treeInsert(b *testing.B, path string, num int) {
	db := loadTestFile(path)
	if db == nil {
		b.Skipf("Testfile %s not found", path)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buildTreeFromDB(db, num, false)
	}
}

func treeSearch(b *testing.B, path string) {
	db := loadTestFile(path)
	if db == nil {
		b.Skipf("Testfile %s not found", path)
	}
	trie := buildTreeFromDB(db, runtime.NumCPU(), false)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for _, w := range db {
			trie.Lookup(w)
		}
	}
}

func BenchmarkUUIDsTreeInsert(b *testing.B) {
	treeInsert(b, "testdata/uuid.txt", 1)
}

func BenchmarkUUIDsTreeSearch(b *testing.B) {
	treeSearch(b, "testdata/uuid.txt")
}

func BenchmarkWordsTreeInsert(b *testing.B) {
	treeInsert(b, "testdata/words.txt", 1)
}

func BenchmarkWordsTreeSearch(b *testing.B) {
	treeSearch(b, "testdata/words.txt")
}

func BenchmarkHSKTreeInsert(b *testing.B) {
	treeInsert(b, "testdata/hsk_words.txt", 1)
}

func BenchmarkHSKTreeSearch(b *testing.B) {
	treeSearch(b, "testdata/hsk_words.txt")
}

func BenchmarkDOITreeInsert(b *testing.B) {
	treeInsert(b, "testdata/DOI-2011.txt", 1)
}

func BenchmarkDOITreeSearch(b *testing.B) {
	treeSearch(b, "testdata/DOI-2011.txt")
}

func BenchmarkConcurrentDOITreeInsert(b *testing.B) {
	treeInsert(b, "testdata/DOI-2011.txt", workers)
}

func BenchmarkInteger50MSparse(b *testing.B) {
	rand.Seed(0)
	db := generateRandomIntegers(50000000)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		buildTreeFromDB(db, workers, false)
	}
}

func BenchmarkInteger50MDense(b *testing.B) {
	rand.Seed(0)
	db := fillIntegers(50000000)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		buildTreeFromDB(db, workers, false)
	}
}

func BenchmarkInteger10MDense(b *testing.B) {
	rand.Seed(0)
	db := fillIntegers(10000000)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		buildTreeFromDB(db, workers, false)
	}
}

func BenchmarkInteger10MSparse(b *testing.B) {
	rand.Seed(0)
	db := generateRandomIntegers(10000000)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		buildTreeFromDB(db, workers, false)
	}
}
