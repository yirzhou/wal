package db

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// generateRandomString generates a random string of a given length.
func generateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	seededRand := rand.New(rand.NewSource(time.Now().UnixNano()))
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func getTestDir() string {
	timestamp := strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
	prefix := "kv-store-test-" + timestamp
	return filepath.Join("/tmp", prefix)
}

func TestOpen(t *testing.T) {
	dir := getTestDir()
	kv, err := Open(NewDefaultConfiguration().WithBaseDir(dir))
	defer kv.CloseAndCleanUp()
	if err != nil {
		t.Fatalf("Error creating KVStore: %v", err)
	}
}

func TestPutGet(t *testing.T) {
	dir := getTestDir()
	kv, err := Open(NewDefaultConfiguration().WithBaseDir(dir))
	defer kv.CloseAndCleanUp()
	if err != nil {
		t.Fatalf("Error creating KVStore: %v", err)
	}

	keyVal := make(map[string]string)

	// Put 100 key-value pairs.
	for range 100 {
		key := generateRandomString(10)
		value := generateRandomString(10)
		kv.Put([]byte(key), []byte(value))
		keyVal[key] = value
	}

	// Confirm the key-value pairs are stored.
	for key, val := range keyVal {
		value, err := kv.Get([]byte(key))
		assert.NoError(t, err)
		assert.Equal(t, []byte(val), value)
	}
}

func TestRecoveryNormal(t *testing.T) {
	dir := getTestDir()
	kv, err := Open(NewDefaultConfiguration().WithBaseDir(dir))
	if err != nil {
		t.Fatalf("Error creating KVStore: %v", err)
	}
	// Put 100 key-value pairs.
	for i := range 100 {
		kv.Put([]byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("value-%d", i)))
	}

	// Record the last sequence number and current segment ID.
	lastSequenceNum := kv.GetLastSequenceNum()
	segmentID := kv.GetCurrentSegmentID()

	// Close the KVStore.
	kv.Close()
	// Recover the KVStore.
	kv, err = Open(NewDefaultConfiguration().WithBaseDir(dir))
	if err != nil {
		t.Fatalf("Error recovering KVStore: %v", err)
	}
	defer kv.CloseAndCleanUp()
	// Check if the last sequence number and current segment ID are the same.
	assert.Equal(t, lastSequenceNum, kv.GetLastSequenceNum())
	assert.Equal(t, segmentID, kv.GetCurrentSegmentID())

	// Check if the key-value pairs are recovered.
	for i := range 100 {
		value, err := kv.Get([]byte(fmt.Sprintf("key-%d", i)))
		assert.NoError(t, err)
		assert.Equal(t, []byte(fmt.Sprintf("value-%d", i)), value)
	}
}

func TestRecoveryWithCorruptedWALFile(t *testing.T) {
	dir := getTestDir()
	kv, err := Open(NewDefaultConfiguration().WithBaseDir(dir))
	if err != nil {
		t.Fatalf("Error creating KVStore: %v", err)
	}

	// Put 100 key-value pairs.
	for i := range 100 {
		kv.Put([]byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("value-%d", i)))
	}

	segmentID := kv.GetCurrentSegmentID()

	// Close the DB
	kv.Close()

	// Corrupt the last bit of the latest WAL file.
	walFilePath := filepath.Join(dir, "/logs/"+getWalFileNameFromSegmentID(segmentID))
	stat, err := os.Stat(walFilePath)
	if err != nil {
		t.Fatalf("Error getting file size: %v", err)
	}
	os.Truncate(walFilePath, stat.Size()-1)

	// The database should be recovered from all segments and WALs.
	// The last bit will only corrupt the last record in the WAL file, which is the "key-99" record.
	// So the key-value pairs from 0 to 98 should be recovered, but the key-value pair for "key-99" should be lost.
	kv, err = Open(NewDefaultConfiguration().WithBaseDir(dir))
	defer kv.CloseAndCleanUp()
	if err != nil {
		t.Fatalf("Error recovering KVStore: %v", err)
	}

	// Check if the key-value pairs are recovered.
	for i := range 99 {
		value, err := kv.Get([]byte(fmt.Sprintf("key-%d", i)))
		assert.NoError(t, err)
		assert.Equal(t, []byte(fmt.Sprintf("value-%d", i)), value)
	}
	// Check if the key-value pair for "key-99" is lost.
	value, err := kv.Get([]byte("key-99"))
	assert.NoError(t, err)
	assert.Nil(t, value)
}

func TestRecoveryWithCorruptedSparseIndexFile(t *testing.T) {
	dir := getTestDir()
	kv, _ := Open(NewDefaultConfiguration().WithBaseDir(dir))

	// Put 100 key-value pairs.
	for i := range 100 {
		kv.Put([]byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("value-%d", i)))
	}

	segmentID := kv.GetLastSegmentID()

	// Close the DB
	kv.Close()

	// Corrupt the sparse index file.
	sparseIndexFilePath := filepath.Join(dir, "/checkpoints/"+getSparseIndexFileNameFromSegmentId(segmentID))
	stat, _ := os.Stat(sparseIndexFilePath)
	os.Truncate(sparseIndexFilePath, stat.Size()-1)

	// The database should be recovered from all segments and WALs.
	kv, _ = Open(NewDefaultConfiguration().WithBaseDir(dir))
	defer kv.CloseAndCleanUp()

	// Check if the key-value pairs are recovered.
	for i := range 100 {
		value, err := kv.Get([]byte(fmt.Sprintf("key-%d", i)))
		assert.NoError(t, err)
		assert.Equal(t, []byte(fmt.Sprintf("value-%d", i)), value)
	}
}

func TestRecoveryWithCorruptedCheckpoint(t *testing.T) {
	dir := getTestDir()
	kv, err := Open(NewDefaultConfiguration().WithBaseDir(dir))
	if err != nil {
		t.Fatalf("Error creating KVStore: %v", err)
	}

	// Put 100 key-value pairs.
	for i := range 100 {
		kv.Put([]byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("value-%d", i)))
	}

	// Close the DB
	kv.Close()

	// Corrupt the checkpoint file.
	checkpointFilePath := filepath.Join(dir, "/checkpoints/CHECKPOINT")
	os.Truncate(checkpointFilePath, 0)

	// The database should be recovered from all segments and WALs.
	kv, err = Open(NewDefaultConfiguration().WithBaseDir(dir))
	defer kv.CloseAndCleanUp()
	if err != nil {
		t.Fatalf("Error recovering KVStore: %v", err)
	}

	// Check if the key-value pairs are stored.
	for i := range 100 {
		value, err := kv.Get([]byte(fmt.Sprintf("key-%d", i)))
		assert.NoError(t, err)
		assert.Equal(t, []byte(fmt.Sprintf("value-%d", i)), value)
	}
}

func TestRecoveryNormalWithVariousCheckpointSizes(t *testing.T) {
	checkpointSize := 1024
	for checkpointSize <= 1024*1024 {
		dir := getTestDir()
		kv, _ := Open(NewDefaultConfiguration().WithBaseDir(dir).WithCheckpointSize(int64(checkpointSize)))

		// Put 100 key-value pairs.
		for i := range 100 {
			kv.Put([]byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("value-%d", i)))
		}

		// Close the DB
		kv.Close()

		// Recover the DB
		kv, _ = Open(NewDefaultConfiguration().WithBaseDir(dir).WithCheckpointSize(int64(checkpointSize)))
		defer kv.CloseAndCleanUp()

		// Check if the key-value pairs are recovered.
		for i := range 100 {
			value, err := kv.Get([]byte(fmt.Sprintf("key-%d", i)))
			assert.NoError(t, err)
			assert.Equal(t, []byte(fmt.Sprintf("value-%d", i)), value, "checkpointSize: %d, key: %s, value: %s", checkpointSize, fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i))
		}
		checkpointSize *= 2
	}
}

func TestDelete(t *testing.T) {
	dir := getTestDir()
	kv, _ := Open(NewDefaultConfiguration().WithBaseDir(dir))
	defer kv.CloseAndCleanUp()

	// Put 100 key-value pairs.
	for i := range 100 {
		kv.Put([]byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("value-%d", i)))
	}

	// Delete 100 key-value pairs.
	for i := range 100 {
		kv.Delete([]byte(fmt.Sprintf("key-%d", i)))
	}

	// Check if the key-value pairs are deleted.
	for i := range 100 {
		value, err := kv.Get([]byte(fmt.Sprintf("key-%d", i)))
		assert.NoError(t, err)
		assert.Nil(t, value)
	}

	// Put 100 key-value pairs again.
	for i := range 100 {
		kv.Put([]byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("value-%d", i)))
	}

	// Check if the key-value pairs are recovered.
	for i := range 100 {
		value, err := kv.Get([]byte(fmt.Sprintf("key-%d", i)))
		assert.NoError(t, err)
		assert.Equal(t, []byte(fmt.Sprintf("value-%d", i)), value)
	}
}
