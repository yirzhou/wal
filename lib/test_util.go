package lib

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// getTestDir returns a temporary directory for the test.
func GetTestDir() string {
	timestamp := strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
	prefix := "kv-store-test-" + timestamp
	return filepath.Join("/tmp", prefix)
}

// GetTempDirForTest creates a temporary directory for the test and returns the path.
func GetTempDirForTest(t *testing.T) string {
	dir := t.TempDir()
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	return dir
}
