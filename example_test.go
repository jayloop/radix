package radix_test

import "github.com/jayloop/radix"

func ExampleTree_GetAllocator() {
	t := radix.NewTree(nil)
	a := t.GetAllocator()
	_ = a.Lookup([]byte("hello"))
	t.ReleaseAllocator(a)
}

func ExampleNewTree() {
	_ = radix.NewTree(&radix.Options{
		NumAllocators: 4,
	})
}
