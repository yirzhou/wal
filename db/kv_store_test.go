package db

import (
	"fmt"
	"math/rand"
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
	kv, err := Open(dir)
	defer kv.CloseAndCleanUp()
	if err != nil {
		t.Fatalf("Error creating KVStore: %v", err)
	}
}

func TestPutGet(t *testing.T) {
	dir := getTestDir()
	kv, err := Open(dir)
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

func TestRecovery(t *testing.T) {
	dir := getTestDir()
	kv, err := Open(dir)
	defer kv.CloseAndCleanUp()
	if err != nil {
		t.Fatalf("Error creating KVStore: %v", err)
	}
	// Put 100 key-value pairs.
	for i := range 100 {
		kv.Put([]byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("value-%d", i)))
	}

	// Record the last sequence number and current segment ID.
	lastSequenceNum := kv.GetLastSequenceNum()
	segmentID := kv.wal.GetCurrentSegmentID()

	// Close the KVStore.
	kv.Close()
	// Recover the KVStore.
	kv, err = Open(dir)
	if err != nil {
		t.Fatalf("Error recovering KVStore: %v", err)
	}
	defer kv.CloseAndCleanUp()
	// Check if the last sequence number and current segment ID are the same.
	assert.Equal(t, lastSequenceNum, kv.GetLastSequenceNum())
	assert.Equal(t, segmentID, kv.wal.GetCurrentSegmentID())

	// Check if the key-value pairs are recovered.
	for i := range 100 {
		value, err := kv.Get([]byte(fmt.Sprintf("key-%d", i)))
		assert.NoError(t, err)
		assert.Equal(t, []byte(fmt.Sprintf("value-%d", i)), value)
	}
}
