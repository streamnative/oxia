package oxia

import "github.com/streamnative/oxia/common/compare"

type ResultAndChannel struct {
	gr GetResult
	ch chan GetResult
}

type ResultHeap []*ResultAndChannel

func (h ResultHeap) Len() int {
	return len(h)
}

func (h ResultHeap) Less(i, j int) bool {
	return compare.CompareWithSlash([]byte(h[i].gr.Key), []byte(h[j].gr.Key)) < 0
}

func (h ResultHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *ResultHeap) Push(x any) {
	*h = append(*h, x.(*ResultAndChannel))
}

func (h *ResultHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
