package util

// An StringHeap is a min-heap of ints.

type HeapValue struct {
	StringValue string
	Index       int
	Value       []string
}

type StringHeap []HeapValue

func (h StringHeap) Len() int           { return len(h) }
func (h StringHeap) Less(i, j int) bool { return h[i].StringValue < h[j].StringValue }
func (h StringHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *StringHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(HeapValue))
}

func (h *StringHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
