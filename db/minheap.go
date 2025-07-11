package db

import "bytes"

// MinHeapRecord is a record that is used to store the records in the min heap.
// It is used to store the records in the min heap for the compaction process.
// The record is the KVRecord and the segment file path is the path to the segment file that contains the record.
type MinHeapRecord struct {
	Record          KVRecord
	SegmentFilePath string
}

// MinHeap is a min heap of MinHeapRecord.
// If the keys are the same, the segment file path is used to break the tie.
type MinHeap []*MinHeapRecord

func (h MinHeap) Len() int { return len(h) }
func (h MinHeap) Less(i, j int) bool {
	if bytes.Compare(h[i].Record.Key, h[j].Record.Key) == 0 {
		segmentID1, _ := GetSegmentIDFromSegmentFilePath(h[i].SegmentFilePath)
		segmentID2, _ := GetSegmentIDFromSegmentFilePath(h[j].SegmentFilePath)
		return segmentID1 > segmentID2
	}
	return bytes.Compare(h[i].Record.Key, h[j].Record.Key) < 0
}
func (h MinHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *MinHeap) Push(x interface{}) { *h = append(*h, x.(*MinHeapRecord)) }
func (h *MinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (h *MinHeap) Peek() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	return x
}
