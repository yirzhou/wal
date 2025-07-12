package db

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"wal/lib"
)

const (
	checkpointDir              = "checkpoints"
	logsDir                    = "logs"
	checkpointFile             = "CHECKPOINT"
	currentFile                = "CURRENT"
	walFilePrefix              = "wal-"
	segmentFilePrefix          = "segment-"
	manifestFilePrefix         = "MANIFEST-"
	sparseIndexFilePrefix      = "index-"
	segmentThresholdL0         = 4
	segmentFileSizeThresholdLX = 10 * 1024 * 1024 // 10MiB
)

// Write to WAL -> Update Memtable -> (When ready) -> Flush Memtable to SSTable -> Checkpoint -> Delete old WAL
// In this process, the flush is done by the memtable.

// Pair is a key-value pair.
type Pair struct {
	Key   string
	Value []byte
}

type KVStore struct {
	lock            sync.RWMutex
	wal             *WAL
	memState        *MemState
	activeSegmentID uint64
	dir             string
	// Outer slice index is the level number.
	// Inner slice holds all the segment files for that level.
	levels                     [][]SegmentMetadata
	segmentFileSizeThresholdLX int64

	// A channel to signal the compaction loop to stop.
	shutdownChan chan struct{}

	// The configuration for the KVStore.
	config *Configuration
}

// compactionLoop is the main loop for the compaction process.
// It runs in the background and checks for work every 30 seconds.
// It also listens to the shutdownChan and stops when it receives a signal.
func (s *KVStore) compactionLoop() {
	// A Ticker is more efficient than time.Sleep for periodic tasks.
	ticker := time.NewTicker(time.Duration(s.config.GetCompactionIntervalMs()) * time.Millisecond) // Check every 30 seconds
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// The ticker fired, it's time to check for work.
			log.Println("Background check for compaction work...")
			if err := s.doCompaction(); err != nil {
				log.Printf("Error during background compaction: %v", err)
			}
		case <-s.shutdownChan:
			// We received a shutdown signal, exit the loop.
			log.Println("Stopping compaction loop.")
			return
		}
	}
}

// tryRecoverFromCurrentFile tries to recover the last segment ID from the CURRENT file.
func tryRecoverFromCurrentFile(currentFilePath string) ([][]SegmentMetadata, uint64, error) {
	// Check if the CURRENT file exists
	if _, err := os.Stat(currentFilePath); os.IsNotExist(err) {
		log.Println("tryRecoverFromCurrentFile: CURRENT file does not exist")
		return nil, 0, err
	}
	// Read the CURRENT file
	content, err := os.ReadFile(currentFilePath)
	if err != nil {
		log.Println("tryRecoverFromCurrentFile: Error reading CURRENT file:", err)
		return nil, 0, err
	}
	// Get the content of the CURRENT file
	checksumBytes := make([]byte, 4)
	copy(checksumBytes, content[:4])
	checksum := binary.LittleEndian.Uint32(checksumBytes)
	// Compute the checksum of the content
	computedChecksum := ComputeChecksum(content[4:])
	// Check if the checksum is correct
	if checksum != computedChecksum {
		log.Println("tryRecoverFromCurrentFile: Bad checksum")
		return nil, 0, lib.ErrBadChecksum
	}

	// Get the manifest file path
	manifestFilePath := string(content[4:])
	manifestFileName := filepath.Base(manifestFilePath)
	// Get the segment ID from the manifest file path
	segmentID, err := GetSegmentIDFromManifestFileName(manifestFileName)
	if err != nil {
		log.Println("tryRecoverFromCurrentFile: Error getting segment ID from manifest file:", err)
		return nil, 0, err
	}
	// Read the manifest file and recover the levels layout
	manifestFile, err := os.Open(manifestFilePath)
	if err != nil {
		log.Println("tryRecoverFromCurrentFile: Error opening manifest file:", err)
		return nil, 0, err
	}
	defer manifestFile.Close()
	// Read the manifest file and recover the levels layout
	manifestFile.Seek(0, io.SeekStart)
	segmentMetadataList := make([]SegmentMetadata, 0)
	for {
		manifestFile.Seek(0, io.SeekCurrent)
		segmentMetadata, err := ReadSegmentMetadata(manifestFile)
		if err != nil {
			// Handle EOF
			if err == io.EOF {
				log.Println("tryRecoverFromCurrentFile: Reached EOF")
				break
			}
			log.Println("tryRecoverFromCurrentFile: Error reading segment metadata:", err)
			return nil, 0, err
		}
		segmentMetadataList = append(segmentMetadataList, *segmentMetadata)
	}
	// Sort the segment metadata list by level and id'
	slices.SortFunc(segmentMetadataList, func(a, b SegmentMetadata) int {
		if a.level != b.level {
			return int(a.level - b.level)
		}
		return int(a.id - b.id)
	})
	// Handle the case where the segment metadata list is empty
	highestLevel := uint32(0)
	if len(segmentMetadataList) != 0 {
		highestLevel = segmentMetadataList[len(segmentMetadataList)-1].level
	}
	levels := make([][]SegmentMetadata, highestLevel+1)
	// Initialize every level.
	for i := range highestLevel + 1 {
		levels[i] = make([]SegmentMetadata, 0)
	}
	for _, segmentMetadata := range segmentMetadataList {
		levels[segmentMetadata.level] = append(levels[segmentMetadata.level], segmentMetadata)
	}
	return levels, segmentID, nil
}

// GetSegmentIDFromManifestFileName returns the segment ID from a manifest file name.
func GetSegmentIDFromManifestFileName(fileName string) (uint64, error) {
	segmentID, err := strconv.ParseUint(strings.TrimPrefix(fileName, manifestFilePrefix), 10, 64)
	if err != nil {
		return 0, err
	}
	return segmentID, nil
}

// recoverMemStateFromLevels recovers the memstate from the level,s by adding sparse index entries to the memstate.
func recoverMemStateFromLevels(indexDir string, levels [][]SegmentMetadata, memState *MemState) error {
	for _, level := range levels {
		for idx, segmentMetadata := range level {
			sparseIndexFilePath := filepath.Join(indexDir, segmentMetadata.GetSparseIndexFileName())
			sparseIndexFile, err := os.Open(sparseIndexFilePath)
			if err != nil {
				log.Println("recoverMemStateFromLevels: Error opening sparse index file:", err)
				return err
			}
			defer sparseIndexFile.Close()
			sparseIndexFile.Seek(0, io.SeekStart)
			for {
				offset, _ := sparseIndexFile.Seek(0, io.SeekCurrent)
				sparseIndexRecord, err := getNextSparseIndexRecord(sparseIndexFile)
				if err != nil {
					if err == io.EOF {
						break
					}
					log.Println("recoverMemStateFromLevels: Error reading sparse index record:", err)
					if idx == len(level)-1 {
						os.Truncate(sparseIndexFilePath, offset)
						break
					} else {
						// Cannot recover from this error
						log.Fatalf("recoverMemStateFromLevels: Cannot recover from error: %v", err)
					}
				}
				if sparseIndexRecord == nil {
					// No more records.
					break
				}
				memState.AddSparseIndexEntry(segmentMetadata.id, sparseIndexRecord.Key, sparseIndexRecord.Offset)
			}
		}
	}
	return nil
}

// Open opens/creates the log file and initializes the KVStore object.
//
// 1. Try to get the last checkpoint file path from the checkpoint file and the WAL files.
//
// 2. If the checkpoint is found, open it and read the last segment file path.
//
// - Load the segment file into memtable.
//
// - For all WAL files with segment ID greater than the last segment ID in the checkpoint file, replay them into memtable in ascending order.
//
// - Truncate the WAL file (likely the last one) to the last good offset.
//
// 3. If the checkpoint is not found, simply start from the beginning of the WAL files and replay them into memtable in ascending order.
//
// - Truncate the WAL file (likely the last one) to the last good offset.
//
// 4. Create and return the KVStore object.
// TODOs:
// 1. Add a compaction job that removes old checkpoints and sparse index files.
func Open(config *Configuration) (*KVStore, error) {
	// Create the master directory if it doesn't exist.
	err := os.MkdirAll(config.GetBaseDir(), 0755)
	if err != nil {
		log.Fatalf("Error creating master directory: %v", err)
	}

	// Create logs directory if it doesn't exist.
	err = os.MkdirAll(filepath.Join(config.GetBaseDir(), logsDir), 0755)
	if err != nil {
		log.Fatalf("Error creating logs directory: %v", err)
	}

	// Create checkpoint directory if it doesn't exist.
	err = os.MkdirAll(filepath.Join(config.GetBaseDir(), checkpointDir), 0755)
	if err != nil {
		log.Fatalf("Error creating checkpoint directory: %v", err)
	}

	// Read the "CURRENT" file
	levels, lastSegmentID, err := tryRecoverFromCurrentFile(filepath.Join(config.GetBaseDir(), checkpointDir, currentFile))
	if err != nil && !os.IsNotExist(err) {
		log.Println("Open: Error recovering from CURRENT file:", err)
	}

	// Recover the sparse indexes from levels metadata.
	memState := NewMemState()
	err = recoverMemStateFromLevels(filepath.Join(config.GetBaseDir(), checkpointDir), levels, memState)
	if err != nil {
		log.Println("Open: Error recovering from levels:", err)
		return nil, err
	}

	// Recover from WAL files.
	wal, err := recoverFromWALs(lastSegmentID, filepath.Join(config.GetBaseDir(), logsDir), memState, config.GetCheckpointSize())
	if err != nil {
		log.Println("Open: Error recovering from WAL files:", err)
		return nil, err
	}

	// Return the fully initialized, ready-to-use KVStore object.
	kv := &KVStore{
		wal:                        wal,
		memState:                   memState,
		activeSegmentID:            lastSegmentID + 1,
		dir:                        config.GetBaseDir(),
		levels:                     levels,
		segmentFileSizeThresholdLX: config.GetSegmentFileSizeThresholdLX(),
		shutdownChan:               make(chan struct{}),
		config:                     config,
	}
	// Start the compaction loop.
	if config.GetCompactionIntervalMs() > 0 {
		go kv.compactionLoop()
	}
	return kv, nil
}

// GetCurrentSegmentIDSafe returns the ID of the current segment.
func (s *KVStore) GetCurrentSegmentIDSafe() uint64 {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.activeSegmentID
}

// This method assumes that the lock is currently withheld.
func (s *KVStore) GetCurrentSegmentIDUnsafe() uint64 {
	return s.activeSegmentID
}

// This method assumes that the lock is currently withheld.
func (s *KVStore) AllocateSegmentIDUnsafe() uint64 {
	s.activeSegmentID++
	return s.activeSegmentID
}

// This method assumes that the lock is currently not withheld.
func (s *KVStore) AllocateNewSegmentID() uint64 {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.activeSegmentID++
	return s.activeSegmentID
}

func (kv *KVStore) findKeyInL0(key []byte) ([]byte, error) {
	levelZero := kv.levels[0]
	for i := len(levelZero) - 1; i >= 0; i-- {
		segmentMetadata := levelZero[i]
		segmentFilePath := segmentMetadata.filePath
		offset := kv.memState.FindKeyInSparseIndex(segmentMetadata.id, key)
		if offset == -1 {
			continue
		}
		value, err := kv.searchInSegmentFile(segmentFilePath, offset, key)
		if err != nil {
			log.Println("findKeyInL0: Error searching in segment file:", err)
			return nil, err
		}
		if value != nil {
			return value, nil
		}
	}
	return nil, nil
}

func binSearchSegmentMetadata(level []SegmentMetadata, key []byte) int {
	l := 0
	r := len(level) - 1
	searchKeyStr := string(key)
	for l <= r {
		mid := (l + r) / 2
		minKeyStr := string(level[mid].minKey)
		maxKeyStr := string(level[mid].maxKey)
		if searchKeyStr >= minKeyStr && searchKeyStr <= maxKeyStr {
			return mid
		}
		if searchKeyStr < minKeyStr {
			r = mid - 1
		} else {
			l = mid + 1
		}
	}
	return -1
}

// getInternalV2 is the internal implementation of Get using leveled compaction.
func (kv *KVStore) getInternalV2(key []byte) ([]byte, error) {
	// Find the key in L0 first.
	value, err := kv.findKeyInL0(key)
	if err != nil {
		log.Println("getInternal: Error finding key in L0:", err)
		return nil, err
	}
	if value != nil {
		return value, nil
	}

	// Find from L1 onwards.
	for i := 1; i < len(kv.levels); i++ {
		level := kv.levels[i]
		// Do a binary search on the segment list based on minKey and maxKey.
		idx := binSearchSegmentMetadata(level, key)
		if idx == -1 {
			continue
		}
		// Found level.
		log.Println("getInternalV2: Found key in level:", i)
		segmentMetadata := level[idx]
		segmentFilePath := segmentMetadata.filePath
		offset := kv.memState.FindKeyInSparseIndex(segmentMetadata.id, key)
		if offset == -1 {
			continue
		}
		log.Println("getInternalV2: Found key in segment file:", segmentFilePath, "with offset:", offset)
		// Try to search from the segment file.
		value, err := kv.searchInSegmentFile(segmentFilePath, offset, key)
		if err != nil {
			log.Println("getInternal: Error searching in segment file:", err)
			return nil, err
		}
		if value != nil {
			log.Println("getInternal: Found key in segment file:", string(key))
			return value, nil
		}
	}
	return nil, nil
}

// Deprecated: Use getInternalV2 instead.
// getInternal is the internal implementation of Get using the old way of going through all sparse indexes from newest to oldest (without leveled compaction).
func (kv *KVStore) getInternal(key []byte) ([]byte, error) {
	// Search in the segment files.
	segmentIDs, err := kv.memState.GetAllSegmentIDsDescendingOrder()
	if err != nil {
		log.Println("Get: Error getting all segment files:", err)
		return nil, err
	}
	log.Println("Get: Searching in segment files:", segmentIDs)
	var value []byte = nil
	for _, segmentID := range segmentIDs {
		offset := kv.memState.FindKeyInSparseIndex(segmentID, key)
		if offset == -1 {
			// The segment file does contain the key.
			continue
		}
		segmentFilePath := filepath.Join(kv.dir, checkpointDir, getSegmentFileNameFromSegmentId(segmentID))
		value, err = kv.searchInSegmentFile(segmentFilePath, offset, key)
		if err != nil {
			// Technically we shouldn't get an error here.
			log.Printf("Get: Error searching in segment file: %v\n", err)
			return nil, err
		}
		// This is the value we are looking for since we are searching in the segment files in descending order.
		if value != nil {
			log.Println("Get: Found key in segment file:", string(key))
			break
		}
	}
	return value, nil
}

// Get returns the value for a given key. The value is nil if the key is not found.
func (kv *KVStore) Get(key []byte) ([]byte, error) {
	kv.lock.RLock()
	defer kv.lock.RUnlock()
	value, err := kv.memState.Get(key)
	if err == nil {
		// Directly return the value from the memtable.
		log.Printf("Get: Found key [%s] in memtable with value [%s]\n", string(key), string(value))
	} else {
		// Search in L0 first.
		value, err = kv.getInternalV2(key)
		if err != nil {
			log.Printf("Get: Error searching for key %s: %v\n", string(key), err)
			return nil, err
		}
	}
	// At this point, the value is nil if the key is not found in the segment files.
	if value == nil {
		log.Println("Get: Key not found in segment files:", string(key))
	} else {
		// If the value is a tombstone, it means the key is deleted.
		if bytes.Equal(value, lib.TOMBSTONE) {
			log.Println("Get: Key is a tombstone:", string(key))
			value = nil
		}
	}
	return value, nil
}

// Put writes a key-value pair to the KVStore.
func (kv *KVStore) Put(key, value []byte) error {
	// Makes sure that the key and value are not nil.
	if key == nil || value == nil || bytes.Equal(key, lib.CHECKPOINT) || bytes.Equal(value, lib.TOMBSTONE) {
		log.Printf("Put: Invalid key or value: %s, %s\n", string(key), string(value))
		return errors.New("invalid key or value")
	}
	return kv.putInternal(key, value)
}

// putInternal is the internal implementation of the Put method.
func (kv *KVStore) putInternal(key, value []byte) error {
	kv.lock.Lock()
	defer kv.lock.Unlock()

	// 1. Write to WAL
	walErr := kv.wal.Append(key, value)

	// 2. Check if the memtable is ready to be flushed (checkpoint)
	if walErr != nil && walErr != lib.ErrCheckpointNeeded {
		log.Println("Error writing to WAL:", walErr)
		return walErr
	}

	// 3. Update Memtable
	memStateErr := kv.memState.Put(key, value)
	if memStateErr != nil {
		log.Println("Error updating Memtable:", memStateErr)
		return memStateErr
	}

	// 4. Check if the WAL is ready to be checkpointed
	if walErr == lib.ErrCheckpointNeeded {
		// checkpoint the memtable
		err := kv.doCheckpoint()
		if err != nil {
			log.Println("Error checkpointing:", err)
			return err
		}
	}
	return nil
}

// Delete deletes a key-value pair from the KVStore.
// It writes a tombstone to the WAL and the memtable.
// TODO: It also updates the offset in the sparse index.
func (kv *KVStore) Delete(key []byte) error {
	// Ensures that the key is not CHECKPOINT.
	if bytes.Equal(key, lib.CHECKPOINT) {
		log.Printf("Delete: Invalid key: %s\n", string(key))
		return errors.New("invalid key")
	}
	err := kv.putInternal(key, lib.TOMBSTONE)
	if err != nil {
		log.Printf("Delete: Error [%v] deleting key [%s]\n", err, string(key))
		return err
	}
	return nil
}

// compareWalFilesAscending compares two WAL file names in ascending order.
func compareWalFilesAscending(a, b string) int {
	segmentIDA, err := getSegmentIDFromWalFileName(a)
	if err != nil {
		return 1
	}
	segmentIDB, err := getSegmentIDFromWalFileName(b)
	if err != nil {
		return -1
	}
	return int(segmentIDA - segmentIDB)
}

// getWalDir returns the directory where the WAL files are stored.
func (kv *KVStore) getWalDir() string {
	return filepath.Join(kv.dir, logsDir)
}

// getCheckpointDir returns the directory where the checkpoint files are stored.
func (kv *KVStore) getCheckpointDir() string {
	return filepath.Join(kv.dir, checkpointDir)
}

// getManifestFile returns the manifest file.
func (kv *KVStore) getManifestFilePath(segmentID uint64) string {
	return filepath.Join(kv.dir, checkpointDir, fmt.Sprintf("%s%06d", manifestFilePrefix, segmentID))
}

// flushManifestFileWithLevels flushes the manifest file for the given levels.
func flushManifestFileWithLevels(filePath string, levels [][]SegmentMetadata) error {
	file, err := os.OpenFile(filePath, AppendFlags, 0644)
	if err != nil {
		log.Println("flushManifestFileWithLevels: Error opening manifest file:", err)
		return err
	}
	defer file.Close()
	for _, level := range levels {
		for _, segment := range level {
			bytes := segment.GetBytes()
			_, err = file.Write(bytes)
			if err != nil {
				log.Println("flushManifestFileWithLevels: Error writing to manifest file:", err)
				return err
			}
			err = file.Sync() // Sync after each segment.
			if err != nil {
				log.Println("flushManifestFileWithLevels: Error syncing manifest file:", err)
				return err
			}
		}
	}
	return nil
}

// getSegmentFile returns the segment file for a given segment ID.
func (kv *KVStore) searchInSegmentFile(segmentFilePath string, offset int64, key []byte) ([]byte, error) {
	log.Println("searchInSegmentFile: Searching in segment file:", segmentFilePath, "with offset:", offset, "and key:", string(key))
	segmentFile, err := os.Open(segmentFilePath)
	if err != nil {
		log.Println("searchInSegmentFile: Error opening segment file:", err)
		return nil, err
	}
	defer segmentFile.Close()

	// Seek to the offset.
	_, _ = segmentFile.Seek(offset, io.SeekStart)

	var value []byte = nil
	for {
		_, err = segmentFile.Seek(0, io.SeekCurrent)
		if err != nil {
			log.Println("searchInSegmentFile: Error seeking to current position:", err)
			return nil, err
		}
		// Check if we've found the record.
		record, err := getNextKVRecord(segmentFile)
		if err != nil {
			if err == io.EOF {
				// No more records.
				break
			}
			log.Println("searchInSegmentFile: Error recovering next record:", err)
			return nil, err
		}
		// Found an exact match.
		if bytes.Equal(record.Key, key) {
			// Update the value to the latest one.
			value = record.Value
		} else if bytes.Compare(record.Key, key) > 0 {
			// The key is greater than the current key. We don't need to search further.
			break
		}
	}
	return value, nil
}

// getSegmentIDFromWalFileName returns the segment ID from a WAL file name.
func getSegmentIDFromWalFileName(fileName string) (uint64, error) {
	segmentId := strings.TrimPrefix(fileName, walFilePrefix)
	segmentIdInt, err := strconv.ParseUint(segmentId, 10, 64)
	if err != nil {
		return 0, err
	}
	return segmentIdInt, nil
}

// getSegmentIDFromSegmentFilePath returns the segment ID from a segment file path.
func GetSegmentIDFromIndexFilePath(filePath string) (uint64, error) {
	fileName := filepath.Base(filePath)
	segmentId := strings.TrimPrefix(fileName, sparseIndexFilePrefix)
	segmentIdInt, err := strconv.ParseUint(segmentId, 10, 64)
	if err != nil {
		return 0, err
	}
	return segmentIdInt, nil
}

// getSegmentIDFromSegmentFilePath returns the segment ID from a segment file path.
func GetSegmentIDFromSegmentFilePath(filePath string) (uint64, error) {
	fileName := filepath.Base(filePath)
	segmentId := strings.TrimPrefix(fileName, segmentFilePrefix)
	segmentIdInt, err := strconv.ParseUint(segmentId, 10, 64)
	if err != nil {
		return 0, err
	}
	return segmentIdInt, nil
}

// getSegmentIDFromSegmentFileName returns the segment ID from a segment file name.
func getSegmentIDFromSegmentFileName(fileName string) (uint64, error) {
	segmentId := strings.TrimPrefix(fileName, segmentFilePrefix)
	segmentIdInt, err := strconv.ParseUint(segmentId, 10, 64)
	if err != nil {
		return 0, err
	}
	return segmentIdInt, nil
}

// getSegmentFileNameFromSegmentId constructs the filename for a segment file from a segment ID.
func getSegmentFileNameFromSegmentId(segmentId uint64) string {
	return fmt.Sprintf("%s%06d", segmentFilePrefix, segmentId)
}

// getSparseIndexFileNameFromSegmentId constructs the filename for a sparse index file from a segment ID.
func getSparseIndexFileNameFromSegmentId(segmentId uint64) string {
	return fmt.Sprintf("%s%06d", sparseIndexFilePrefix, segmentId)
}

// tryGetLastCheckpoint tries to get the last checkpoint file path from the checkpoint file and the WAL files.
func tryGetLastCheckpoint(checkpointDir, walDir string) string {
	// Create the checkpoint directory if it doesn't exist.
	err := os.MkdirAll(checkpointDir, 0755)
	if err != nil {
		log.Println("Error creating checkpoint directory:", err)
		return ""
	}

	// Create the WAL directory if it doesn't exist.
	err = os.MkdirAll(walDir, 0755)
	if err != nil {
		log.Println("Error creating WAL directory:", err)
		return ""
	}

	checkpointFilePath, _ := tryGetLastCheckpointFromFile(checkpointDir)
	if checkpointFilePath != "" {
		return checkpointFilePath
	}
	checkpointFilePath, _ = tryGetLastCheckpointFromWalFiles(walDir)
	if checkpointFilePath != "" {
		return checkpointFilePath
	}
	return ""
}

// tryGetLastCheckpoint tries to get the last checkpoint file path.
// If the file does not exist, it returns an empty string and no error.
func tryGetLastCheckpointFromFile(checkpointDir string) (string, error) {
	checkpointFilePath := filepath.Join(checkpointDir, checkpointFile)
	// Creates the file if it doesn't exist.
	checkpointFile, err := os.OpenFile(checkpointFilePath, os.O_RDONLY, 0644)
	if err != nil {
		log.Println("Error opening checkpoint file:", err)
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}
	defer checkpointFile.Close()

	// Read the file.
	bytes, err := io.ReadAll(checkpointFile)
	if err != nil {
		log.Println("Error reading checkpoint file:", err)
		return "", err
	}
	// Check the bytes length.
	if len(bytes) < 4 {
		log.Println("Checkpoint file is too short")
		return "", lib.ErrCheckpointCorrupted
	}

	// Verify the checksum of the file.
	checksum := binary.LittleEndian.Uint32(bytes[:4])
	if checksum != ComputeChecksum(bytes[4:]) {
		log.Println("Checksum mismatch in checkpoint file")
		return "", lib.ErrBadChecksum
	}
	// Return the segment file path.
	return string(bytes[4:]), nil
}

// getLastCheckpointFilePathFromWalFile tries to get the last checkpoint file path from a WAL file.
// If the WAL file does not contain a CHECKPOINT record, it returns an empty string and no error.
func getLastCheckpointFilePathFromWalFile(walFilePath string) (string, error) {
	walFile, err := os.Open(walFilePath)
	if err != nil {
		return "", err
	}
	defer walFile.Close()
	// Read the file from the beginning.
	_, err = walFile.Seek(0, io.SeekStart)
	if err != nil {
		log.Println("Error seeking to start:", err)
		return "", err
	}
	lastCheckpointFilePath := ""
	for {
		_, err := walFile.Seek(0, io.SeekCurrent)
		if err != nil {
			log.Println("Error seeking to current position:", err)
			break
		}
		// Read the first record.
		record, err := recoverNextRecord(walFile)
		if err != nil && err != io.EOF {
			log.Println("Error recovering next record:", err)
			break
		}

		if record == nil {
			// EOF
			break
		}
		// Return the segment file path. If the record is not a CHECKPOINT record, return an empty string.
		if string(record.Key) == string(lib.CHECKPOINT) {
			log.Println("Found CHECKPOINT record in WAL file:", walFilePath)
			lastCheckpointFilePath = string(record.Value)
			// Keep updating the last checkpoint file.
		}
	}
	return lastCheckpointFilePath, nil
}

// tryGetLastCheckpointFromWalFiles tries to get the last checkpoint file path from the WAL files.
func tryGetLastCheckpointFromWalFiles(walDir string) (string, error) {
	walFiles, err := listWALFiles(walDir)
	if err != nil {
		log.Println("tryGetLastCheckpointFromWalFiles: Error listing WAL files:", err)
		return "", err
	}
	// Sort all files by segment ID in reverse order.
	slices.SortFunc(walFiles, compareWalFilesAscending)
	slices.Reverse(walFiles)
	// Get the last checkpoint file path from the WAL files (reverse order).
	for _, walFile := range walFiles {
		walPath := filepath.Join(walDir, walFile)
		walPath, err := getLastCheckpointFilePathFromWalFile(walPath)
		if err != nil {
			log.Println("tryGetLastCheckpointFromWalFiles: Error getting last checkpoint file path from WAL file:", err)
			// Skip this file.
		} else if walPath != "" {
			return walPath, nil
		}
	}
	return "", nil
}

// getNewSegmentFilePath returns the path to the next segment file.
func (kv *KVStore) getSegmentFilePath(segmentID uint64) string {
	segmentFileName := getSegmentFileNameFromSegmentId(segmentID)
	return filepath.Join(kv.dir, checkpointDir, segmentFileName)
}

// getNewSparseIndexFilePath returns the path to the next sparse index file.
func (kv *KVStore) getSparseIndexFilePath(segmentID uint64) string {
	sparseIndexFileName := getSparseIndexFileNameFromSegmentId(segmentID)
	return filepath.Join(kv.dir, checkpointDir, sparseIndexFileName)
}

// RollToNewSegment rolls to a new segment.
// It creates a new WAL file and updates the WAL object.
// It also updates the current segment ID and the last sequence number.
func (kv *KVStore) RollToNewSegment() error {
	segmentID := kv.GetCurrentSegmentIDUnsafe() + 1
	walFileName := getWalFileNameFromSegmentID(segmentID)
	walFilePath := filepath.Join(kv.dir, logsDir, walFileName)
	walFile, err := os.OpenFile(walFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println("Error opening WAL file:", err)
		return err
	}
	// Close the current file
	err = kv.wal.activeFile.Close()
	if err != nil {
		log.Println("Error closing current WAL file:", err)
		return err
	}
	kv.wal.activeFile = walFile
	// kv.wal.activeSegmentID = segmentID
	kv.wal.lastSequenceNum = 0

	// Update the active segment ID.
	kv.activeSegmentID = segmentID

	return nil
}

// listWALFiles lists all the WAL files in the directory.
func listWALFiles(dir string) ([]string, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	walFiles := make([]string, 0)
	for _, file := range files {
		if strings.HasPrefix(file.Name(), walFilePrefix) {
			walFiles = append(walFiles, file.Name())
		}
	}
	return walFiles, nil
}

// listSegmentFiles lists all the segment files in the directory.
func listSegmentFiles(dir string) ([]string, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	segmentFiles := make([]string, 0)
	for _, file := range files {
		if strings.HasPrefix(file.Name(), segmentFilePrefix) {
			segmentFiles = append(segmentFiles, file.Name())
		}
	}
	return segmentFiles, nil
}

// getHighestSegmentID returns the highest segment ID from the list of WAL files. If any file is not named correctly, it will be skipped.
// Note that zero will be returned if no files are found.
func getHighestSegmentID(files []string) uint64 {
	highestId := uint64(0)
	for _, file := range files {
		segmentId := strings.TrimPrefix(file, "wal-")
		segmentIdInt, err := strconv.ParseUint(segmentId, 10, 64)
		if err != nil {
			log.Println("Error parsing segment ID:", err)
			// Skip this file.
		} else {
			highestId = max(highestId, segmentIdInt)
		}
	}
	return highestId
}

// getWalFileNameFromSegmentID constructs the filename for a WAL file from a segment ID.
func getWalFileNameFromSegmentID(segmentId uint64) string {
	return fmt.Sprintf("%s%06d", walFilePrefix, segmentId)
}

// recoverFromWALs recovers the WAL files and returns the WAL object.
func recoverFromWALs(lastSegmentID uint64, walDir string, memState *MemState, checkpointSize int64) (*WAL, error) {
	// Create the WAL directory if it doesn't exist.
	err := os.MkdirAll(walDir, 0755)
	if err != nil {
		log.Println("Error creating WAL directory:", err)
		return nil, err
	}
	wal := &WAL{
		dir: walDir,
	}
	walFiles, err := listWALFiles(walDir)
	if err != nil {
		log.Println("recoverFromWALs: Error listing WAL files:", err)
		return nil, err
	}
	// If no WAL files are found, create a new WAL file.
	if len(walFiles) == 0 {
		log.Println("recoverFromWALs: No WAL files found, creating a new WAL file")
		walFile, err := os.OpenFile(filepath.Join(walDir, getWalFileNameFromSegmentID(lastSegmentID+1)), AppendFlags, 0644)
		if err != nil {
			log.Println("recoverFromWALs: Error creating new WAL file:", err)
			return nil, err
		}
		wal.activeFile = walFile
		wal.dir = walDir
		wal.segmentSize = checkpointSize
		wal.lastSequenceNum = 0
		return wal, nil
	}
	// Sort all files by segment ID in ascending order.
	slices.SortFunc(walFiles, compareWalFilesAscending)
	// Get the last checkpoint file path from the WAL files (ascending order).
	for idx, walFile := range walFiles {
		segmentID, err := getSegmentIDFromWalFileName(walFile)
		if err != nil {
			log.Println("Error getting segment ID from WAL file:", err)
			// Skip this file if the name does not match the expected format.
			continue
		}
		// Process the WAL file only if the segment ID is greater than the last segment ID.
		if segmentID > lastSegmentID {
			log.Println("Recovering from WAL file:", walFile)
			isLastWal := idx == len(walFiles)-1
			// Open the WAL file.
			walFile, err := os.OpenFile(filepath.Join(walDir, walFile), AppendFlags, 0644)
			if err != nil {
				log.Printf("Error opening WAL file: %s: %v\n", walFile.Name(), err)
				log.Fatalf("Error opening WAL file: %s: %v\n", walFile.Name(), err)
			}

			if !isLastWal {
				// The last WAL file will be kept open for future appends.
				defer walFile.Close()
			}

			lastSequenceNum, err := recoverFromWALFile(walFile, memState, isLastWal)
			if err != nil {
				log.Fatalf("Error processing WAL file: %s: %v", walFile.Name(), err)
			}
			if isLastWal {
				// Construct the WAL
				wal = &WAL{
					activeFile:      walFile,
					dir:             walDir,
					segmentSize:     checkpointSize,
					lastSequenceNum: lastSequenceNum,
				}
			}
		}
	}
	return wal, nil
}

// recoverFromSparseIndexFile recovers the sparse index from the sparse index file.
// It returns the last good offset and an error.
func recoverFromSparseIndexFile(filePath string, memState *MemState) (int64, error) {
	segmentID, err := GetSegmentIDFromIndexFilePath(filePath)
	if err != nil {
		log.Println("recoverFromSparseIndexFile: Error getting segment ID from index file:", err)
		return 0, err
	}
	sparseIndexFile, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		log.Println("recoverFromSparseIndexFile: Error opening sparse index file:", err)
		return 0, err
	}
	defer sparseIndexFile.Close()
	var offset int64 = 0
	sparseIndexFile.Seek(0, io.SeekStart)
	for {
		offset, err = sparseIndexFile.Seek(0, io.SeekCurrent)
		if err != nil {
			log.Println("recoverFromSparseIndexFile: Error seeking:", err)
			return 0, err
		}
		record, err := getNextSparseIndexRecord(sparseIndexFile)
		if err != nil {
			if err == io.EOF {
				log.Println("recoverFromSparseIndexFile: End of file reached")
				break
			}
			log.Println("recoverFromSparseIndexFile: Error getting next sparse index record:", err)
			// Return a special error to indicate that the checkpoint file is corrupted.
			return offset, lib.ErrSparseIndexCorrupted
		}
		if record == nil {
			log.Println("recoverFromSparseIndexFile: End of file reached")
			break
		}
		log.Printf("recoverFromSparseIndexFile: Adding sparse index entry: %s, %d\n", string(record.Key), record.Offset)
		// Add the entry to the sparse index in memory.
		memState.AddSparseIndexEntry(segmentID, record.Key, record.Offset)
	}
	return offset, nil
}

// recoverFromWALFile processes a WAL file and returns the last sequence number.
// It also updates the in-memory state.
// If the WAL file is the last one, it truncates the file to the last good offset.
func recoverFromWALFile(reader *os.File, memState *MemState, isLastWal bool) (uint64, error) {
	var goodOffset int64 = 0
	var lastSequenceNum uint64 = 0
	_, err := reader.Seek(0, io.SeekStart)
	if err != nil {
		log.Printf("Error seeking to start of WAL file: %s: %v\n", reader.Name(), err)
		return 0, err
	}
	// Scan the file to populate lastSequenceNum.
	for {
		offset, err := reader.Seek(0, io.SeekCurrent)
		if err != nil {
			log.Println("Error seeking:", err)
			return 0, err
		}

		record, err := recoverNextRecord(reader)
		// Check the current offset of the reader.
		if err != nil {
			// If we get an End-Of-File error, it's a clean stop.
			// This is the expected way to finish recovery.
			if err == io.EOF {
				log.Println("Completed recovery of WAL file:", reader.Name())
				goodOffset = offset
				break
			}
			// If we get a bad checksum, it means the last write was torn.
			// We stop here and trust the log up to this point.
			if err == lib.ErrBadChecksum || err == io.ErrUnexpectedEOF {
				if !isLastWal {
					// Having corruption in an intermediate WAL file is a fatal error!
					log.Fatalf("Bad checksum or unexpected EOF in WAL file: %s", reader.Name())
				}
				log.Println("Bad checksum or unexpected EOF", err.Error())
				goodOffset = offset
				break
			}
			// Any other error is unexpected.
			log.Printf("Error recovering next record in WAL file: %s: %v", reader.Name(), err)
			return 0, err
		}

		lastSequenceNum = record.SequenceNum
		// Update the in-memory state.
		memState.Put(record.Key, record.Value)
	}

	if isLastWal {
		// Truncate the file if it's the last WAL file.
		// This is done to remove the old records that have been recovered.
		log.Println("Truncating to", goodOffset, "for WAL file:", reader.Name())
		err = reader.Truncate(goodOffset)
		if err != nil {
			log.Println("Error truncating file:", err)
			return 0, err
		}
		// Move to the good offset for future appends if it's the last WAL file.
		_, err = reader.Seek(goodOffset, io.SeekStart)
		if err != nil {
			log.Println("Error seeking to good offset::", err)
			return 0, err
		}
	}
	return lastSequenceNum, nil
}

// Close closes the WAL file.
func (kv *KVStore) Close() error {
	// Signal the compaction loop to stop.
	close(kv.shutdownChan)
	// Close the WAL file.
	return kv.wal.Close()
}

func (kv *KVStore) CloseAndCleanUp() error {
	err := kv.Close()
	if err != nil {
		log.Println("CloseAndCleanUp: Error closing WAL:", err)
		return err
	}
	err = kv.CleanUpDirectories()
	if err != nil {
		log.Println("CloseAndCleanUp: Error cleaning up directories:", err)
		return err
	}
	return nil
}

// CleanUpDirectories cleans up the checkpoint and WAL directories.
func (kv *KVStore) CleanUpDirectories() error {
	checkpointDir := filepath.Join(kv.dir, checkpointDir)
	walDir := filepath.Join(kv.dir, logsDir)
	err := os.RemoveAll(checkpointDir)
	if err != nil {
		log.Println("CleanUpDirectories: Error cleaning up checkpoint directory:", err)
		return err
	}
	err = os.RemoveAll(walDir)
	if err != nil {
		log.Println("CleanUpDirectories: Error cleaning up WAL directory:", err)
		return err
	}
	// Delete the master directory.
	err = os.RemoveAll(kv.dir)
	if err != nil {
		log.Println("CleanUpDirectories: Error cleaning up master directory:", err)
		return err
	}
	return nil
}

// GetLastSequenceNum returns the last sequence number.
func (kv *KVStore) GetLastSequenceNum() uint64 {
	return kv.wal.lastSequenceNum
}

// Print prints the memtable.
func (kv *KVStore) Print() {
	log.Println("Last sequence number:", kv.GetLastSequenceNum())
	kv.memState.Print()
}

// tryRecoverSparseIndex tries to recover the sparse index from the checkpoint file. Returns the last segment ID.
func tryRecoverSparseIndex(dir string, memState *MemState) (uint64, error) {
	// Read the checkpoint if exists.
	lastCheckpointFilePath := tryGetLastCheckpoint(filepath.Join(dir, checkpointDir), filepath.Join(dir, logsDir))
	lastSegmentID := uint64(0)

	if lastCheckpointFilePath != "" {
		log.Println("tryRecoverSparseIndex: Last checkpoint found:", lastCheckpointFilePath)
		checkpointFileName := filepath.Base(lastCheckpointFilePath)
		segmentID, err := getSegmentIDFromSegmentFileName(checkpointFileName)
		if err != nil {
			log.Println("tryRecoverSparseIndex: Error getting segment ID from last checkpoint file:", err)
			return 0, lib.ErrCheckpointCorrupted
		}
		lastSegmentID = segmentID
		log.Println("tryRecoverSparseIndex: Last segment ID:", lastSegmentID)

		// Recover sparse index from sparse index file.
		sparseIndexFilePath := filepath.Join(dir, checkpointDir, getSparseIndexFileNameFromSegmentId(segmentID))
		offset, err := recoverFromSparseIndexFile(sparseIndexFilePath, memState)
		if err != nil {
			log.Println("tryRecoverSparseIndex: Error recovering from sparse index file:", err)
			if err == lib.ErrSparseIndexCorrupted {
				// Truncate the sparse index file to the last good offset.
				os.Truncate(sparseIndexFilePath, offset)
			}
			return lastSegmentID, err
		}
	}
	return lastSegmentID, nil
}
