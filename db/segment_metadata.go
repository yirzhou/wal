package db

// SegmentMetadata is the metadata for a segment file.
type SegmentMetadata struct {
	id       uint64
	level    int
	minKey   []byte
	maxKey   []byte
	filePath string
	// Store the sparse index here
	index sparseIndex
}
