package radix

import (
	"bytes"
	"runtime"
	"testing"
)

func TestIterator(t *testing.T) {
	database := loadURLDatabase(t)
	trie := buildTreeFromDB(t, database, runtime.NumCPU())
	// now iterate
	var (
		i     = trie.NewIterator(nil)
		count = 0
		dup   = 0
		seen  = make(map[uint64]struct{})
	)
	for i.Next() {
		v := i.Value()
		if _, ok := seen[v]; ok {
			t.Logf("Failed: Key %s already seen during iteration\n", database[v-1])
			dup++
		}
		seen[v] = struct{}{}
		count++
	}
	t.Logf("Iteration done, %d items seen, %d duplicates", count, dup)
	assertTrue(t, dup == 0)
	assertTrue(t, count == trie.Len())

	// now test prefix iteration
	// grep "^http://bioinformatics.oxfordjournals.org/" DOI-2011-unique.csv | wc -l => 7236
	// etc
	var tests = []struct {
		prefix string
		count  int
	}{
		{"http://bioinformatics.oxfordjournals.org/", 7236},
		{"http://ajpregu.physiology.org/cgi/doi/10.1152/ajpregu.008", 182},
		{"http://circres.ahajournals.org/cgi/doi/10.1161/CIRCRESAHA.108.19", 56},
		{"http://bybil.", 1258},
	}
	for _, tt := range tests {
		t.Run(tt.prefix, func(t *testing.T) {
			tt := tt
			t.Parallel()
			i := NewIterator(trie, []byte(tt.prefix))
			count := 0
			for i.Next() {
				v := i.Value()
				key := database[v-1]
				assertTrue(t, bytes.HasPrefix(key, []byte(tt.prefix)), "Prefix iteration returned a key without the search prefix")
				count++
			}
			assertTrue(t, count == tt.count, "Prefix iteration did not return the expected number of items")
		})
	}
}
