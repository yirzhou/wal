package db

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetNewLevels(t *testing.T) {
	compactionPlan := &CompactionPlan{
		baseSegments: []SegmentMetadata{{
			id:    1,
			level: 0,
		}},
		overlappingSegments: []SegmentMetadata{
			{id: 2, level: 1},
		},
	}

	levels := [][]SegmentMetadata{
		{{id: 1, level: 0}},
		{{
			id:    2,
			level: 1,
		},
			{id: 3,
				level: 1,
			},
		},
	}

	segmentMetadatList := []SegmentMetadata{
		{id: 4, level: 1}, {id: 5, level: 1},
	}

	newLevels := getNewLevels(compactionPlan, levels, segmentMetadatList)
	assert.Equal(t, [][]SegmentMetadata{
		{},
		{SegmentMetadata{
			id: 3, level: 1,
		}, SegmentMetadata{
			id: 4, level: 1,
		}, SegmentMetadata{
			id: 5, level: 1,
		}},
	}, newLevels)
}

func TestDeleteOldSegmentsAndIndexes(t *testing.T) {
	config := NewDefaultConfiguration().WithBaseDir(t.TempDir())
	kv, err := Open(config)
	assert.NoError(t, err)

	defer kv.CloseAndCleanUp()
	// Create a few segments and indexes files
	os.Create(kv.getSegmentFilePath(1))
	os.Create(kv.getSegmentFilePath(2))
	os.Create(kv.getSegmentFilePath(3))
	os.Create(kv.getSparseIndexFilePath(1))
	os.Create(kv.getSparseIndexFilePath(2))
	os.Create(kv.getSparseIndexFilePath(3))

	compactionPlan := &CompactionPlan{
		baseSegments: []SegmentMetadata{
			{id: 1, level: 0},
			{id: 2, level: 0},
		},
		overlappingSegments: []SegmentMetadata{
			{id: 3, level: 1},
		},
	}
	err = kv.deleteOldSegmentsAndIndexes(compactionPlan)
	assert.NoError(t, err)
	// Check that the segments and indexes files are deleted
	assert.NoFileExists(t, kv.getSegmentFilePath(1))
	assert.NoFileExists(t, kv.getSegmentFilePath(2))
	assert.NoFileExists(t, kv.getSegmentFilePath(3))
	assert.NoFileExists(t, kv.getSparseIndexFilePath(1))
	assert.NoFileExists(t, kv.getSparseIndexFilePath(2))
	assert.NoFileExists(t, kv.getSparseIndexFilePath(3))
}

func TestFlushManifestFileWithLevels(t *testing.T) {
	levels := [][]SegmentMetadata{
		{{id: 1, level: 0, filePath: "segment1", minKey: []byte("a"), maxKey: []byte("b")}},
		{{id: 2, level: 1, filePath: "segment2", minKey: []byte("c"), maxKey: []byte("d")}},
	}

	manifestFilePath := filepath.Join(t.TempDir(), "MANIFEST")
	err := flushManifestFileWithLevels(manifestFilePath, levels)
	assert.NoError(t, err)

	// Check that the manifest file is created
	assert.FileExists(t, manifestFilePath)

	// Check that the manifest file contains the correct segments
	manifestFile, err := os.Open(manifestFilePath)
	assert.NoError(t, err)
	defer manifestFile.Close()
	defer os.Remove(manifestFilePath)

	// Read the manifest file and check that the segments are correct
	manifestFile.Seek(0, io.SeekStart)
	record, err := ReadSegmentMetadata(manifestFile)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), record.id)
	assert.Equal(t, uint32(0), record.level)
	assert.Equal(t, []byte("a"), record.minKey)
	assert.Equal(t, []byte("b"), record.maxKey)
	assert.Equal(t, "segment1", record.filePath)

	record, err = ReadSegmentMetadata(manifestFile)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), record.id)
	assert.Equal(t, uint32(1), record.level)
	assert.Equal(t, []byte("c"), record.minKey)
	assert.Equal(t, []byte("d"), record.maxKey)
	assert.Equal(t, "segment2", record.filePath)

	_, err = ReadSegmentMetadata(manifestFile)
	assert.Equal(t, io.EOF, err)
}

// This test may take a while to run.
func TestPerformMerge(t *testing.T) {
	config := NewDefaultConfiguration().WithBaseDir(t.TempDir()).WithSegmentFileSizeThresholdLX(128).WithCheckpointSize(128)
	kv, err := Open(config)
	assert.NoError(t, err)
	defer kv.CloseAndCleanUp()

	keyCount := 20

	for i := range keyCount {
		kv.putInternal([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("value%d", i)))
	}
	for i := range keyCount {
		kv.putInternal([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("value%d", i+keyCount)))
	}

	// Also remove some keys.
	for i := range keyCount {
		if i%2 == 0 {
			kv.Delete([]byte(fmt.Sprintf("key%d", i)))
		}
	}

	compactionPlan := kv.getNextCompactionPlan()
	assert.NotNil(t, compactionPlan)

	segmentMetadataList, err := kv.performMerge(compactionPlan)
	assert.NoError(t, err)
	assert.Greater(t, len(segmentMetadataList), 0)

	// Collect all segments and verify that the key value pairs are correct.
	// Also verify that the sparse index is correct.
	kvMap := make(map[string][]byte)
	for _, segmentMetadata := range segmentMetadataList {
		// At this point, it's the temp segment file.
		segmentFilePath := filepath.Join(kv.getCheckpointDir(), GetTempSegmentFileName(segmentMetadata.id))
		segmentFile, err := os.Open(segmentFilePath)
		assert.NoError(t, err)
		defer segmentFile.Close()

		segmentFile.Seek(0, io.SeekStart)
		record, err := getNextKVRecord(segmentFile)
		assert.NoError(t, err)
		kvMap[string(record.Key)] = record.Value

		sparseIndex := segmentMetadata.sparseIndex
		// Get the temp sparse index file path
		sparseIndexFileName := GetTempSparseIndexFileName(segmentMetadata.id)
		sparseIndexFile, err := os.Open(filepath.Join(kv.getCheckpointDir(), sparseIndexFileName))
		assert.NoError(t, err)
		defer sparseIndexFile.Close()

		// Check that the sparse index is correct.
		segmentID := segmentMetadata.id
		for _, indexEntry := range sparseIndex {
			// Get temp segment file
			tempSegmentFileName := GetTempSegmentFileName(segmentID)
			tempSegmentFile, err := os.Open(filepath.Join(kv.getCheckpointDir(), tempSegmentFileName))
			assert.NoError(t, err)
			defer tempSegmentFile.Close()

			tempSegmentFile.Seek(indexEntry.offset, io.SeekStart)
			record, err := getNextKVRecord(tempSegmentFile)
			if err != nil {
				assert.Equal(t, io.EOF, err)
				break
			}
			assert.NotNil(t, record)
			assert.Equal(t, indexEntry.key, record.Key)
		}
	}

	for k, v := range kvMap {
		value, err := kv.Get([]byte(k))
		assert.NoError(t, err)
		assert.Equal(t, v, value)
	}
}

// TestDoCompaction tests that the compaction works correctly. It also tests the new recovery logic.
func TestDoCompaction(t *testing.T) {
	config := NewDefaultConfiguration().WithBaseDir(t.TempDir()).WithSegmentFileSizeThresholdLX(128).WithCheckpointSize(128)
	kv, err := Open(config)
	assert.NoError(t, err)

	kvMap := make(map[string][]byte)
	// Put some records
	keyCount := 100
	for i := range keyCount {
		kv.putInternal([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("value%d", i)))
		kvMap[fmt.Sprintf("key%d", i)] = []byte(fmt.Sprintf("value%d", i))
	}
	for i := range keyCount {
		kv.putInternal([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("value%d", i+keyCount)))
		kvMap[fmt.Sprintf("key%d", i)] = []byte(fmt.Sprintf("value%d", i+keyCount))
	}
	// Also remove some keys.
	for i := range keyCount {
		if i%2 == 0 {
			kv.Delete([]byte(fmt.Sprintf("key%d", i)))
			delete(kvMap, fmt.Sprintf("key%d", i))
		}
	}

	// Do the compaction right now
	err = kv.doCompaction()
	assert.NoError(t, err)

	// Close the KV Store and reopen it.
	kv.Close()

	// Reopen the KV
	kv, err = Open(config)
	assert.NoError(t, err)
	defer kv.CloseAndCleanUp()

	// Check that the records are still there
	for i := range keyCount {
		value, err := kv.Get([]byte(fmt.Sprintf("key%d", i)))
		assert.NoError(t, err)
		assert.Equal(t, kvMap[fmt.Sprintf("key%d", i)], value)
	}
}
