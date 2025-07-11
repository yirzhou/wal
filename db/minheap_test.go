package db

import (
	"container/heap"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMinHeap(t *testing.T) {
	// 2 different keys
	// for one key, there are three segments.
	records := []MinHeapRecord{
		{KVRecord{Key: []byte("apple"), Value: []byte("val-1")}, "segment-000001"},
		{KVRecord{Key: []byte("apple"), Value: []byte("val-2")}, "segment-000002"},
		{KVRecord{Key: []byte("apple"), Value: []byte("val-3")}, "segment-000003"},
		{KVRecord{Key: []byte("banana"), Value: []byte("val-4")}, "segment-000004"}}
	pq := MinHeap{}
	heap.Init(&pq)

	for _, record := range records {
		fmt.Println(record)
		heap.Push(&pq, &record)
	}

	results := make([]*MinHeapRecord, 0)
	for len(pq) > 0 {
		result := heap.Pop(&pq).(*MinHeapRecord)
		results = append(results, result)
	}
	assert.Equal(t, []byte("apple"), results[0].Record.Key)
	assert.Equal(t, []byte("apple"), results[1].Record.Key)
	assert.Equal(t, []byte("apple"), results[2].Record.Key)
	assert.Equal(t, []byte("banana"), results[3].Record.Key)
	assert.Equal(t, []byte("val-3"), results[0].Record.Value)
	assert.Equal(t, []byte("val-2"), results[1].Record.Value)
	assert.Equal(t, []byte("val-1"), results[2].Record.Value)
	assert.Equal(t, []byte("val-4"), results[3].Record.Value)
}
