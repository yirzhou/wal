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
	checkpointDir         = "checkpoints"
	logsDir               = "logs"
	checkpointFile        = "CHECKPOINT"
	walFilePrefix         = "wal-"
	segmentFilePrefix     = "segment-"
	sparseIndexFilePrefix = "index-"
)

// Write to WAL -> Update Memtable -> (When ready) -> Flush Memtable to SSTable -> Checkpoint -> Delete old WAL
// In this process, the flush is done by the memtable.

// Pair is a key-value pair.
type Pair struct {
	Key   string
	Value []byte
}

type KVStore struct {
	lock     sync.RWMutex
	wal      *WAL
	memState *MemState
	dir      string
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
		// Search in the segment files.
		segmentIDs, err := kv.memState.GetAllSegmentIDsDescendingOrder()
		if err != nil {
			log.Println("Get: Error getting all segment files:", err)
			return nil, err
		}
		log.Println("Get: Searching in segment files:", segmentIDs)
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

// updateCheckpointFile atomically updates the checkpoint file with the new segment file path.
func (kv *KVStore) updateCheckpointFile(segmentFilePath string) error {
	// Get the current unix timestamp as a string.
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	tempCheckpointFilePath := filepath.Join(kv.dir, checkpointDir, fmt.Sprintf("%s.%s", checkpointFile, timestamp))
	tempCheckpointFile, err := os.OpenFile(tempCheckpointFilePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Println("Error opening temp checkpoint file:", err)
		return err
	}
	defer tempCheckpointFile.Close()

	// Compute the checksum of the segment file path.
	checksum := ComputeChecksum([]byte(segmentFilePath))
	bytes := make([]byte, 4+len(segmentFilePath))
	binary.LittleEndian.PutUint32(bytes[:4], checksum)
	copy(bytes[4:], segmentFilePath)

	// Write the segment file path to the temp checkpoint file.
	_, err = tempCheckpointFile.Write(bytes)
	if err != nil {
		log.Println("Error writing to temp checkpoint file:", err)
		return err
	}
	tempCheckpointFile.Sync()

	// Rename the temp checkpoint file to the actual checkpoint file.
	err = os.Rename(tempCheckpointFilePath, filepath.Join(kv.dir, checkpointDir, checkpointFile))
	if err != nil {
		log.Println("Error renaming temp checkpoint file:", err)
		return err
	}
	return nil
}

// cleanUpWALFiles removes all WAL files with segment ID less than or equal to the last segment ID.
func (kv *KVStore) cleanUpWALFiles(lastSegmentID uint64) error {
	walFiles, err := listWALFiles(kv.getWalDir())
	if err != nil {
		log.Println("Error listing WAL files during cleanup:", err)
		return err
	} else {
		// Remove all WAL files.
		for _, walFile := range walFiles {
			segmentID, err := getSegmentIDFromWalFileName(walFile)
			if err != nil {
				log.Printf("Error getting segment ID from WAL file %s: %v\n", walFile, err)
				// Skip this file. The file name is potentially corrupted.
				continue
			}
			if segmentID <= lastSegmentID {
				err = os.Remove(filepath.Join(kv.getWalDir(), walFile))
				if err != nil {
					log.Printf("Error removing WAL file %s: %v\n", walFile, err)
					// Skip this file for now which will be picked up by future checkpoints or cleanup jobs.
				} else {
					log.Printf("Removed WAL file %s\n", walFile)
				}
			}
		}
	}
	return nil
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

// doCheckpoint is the main function that performs a checkpoint.
// It creates a new segment file, converts the memtable to an SSTable,
// and writes the SSTable to the segment file.
// It also appends a special CHECKPOINT record to the WAL and updates the checkpoint file.
// Finally, it removes all old WAL files.
// TODO: checkpointing on normal shutdown.
func (kv *KVStore) doCheckpoint() error {

	// Create the checkpoint directory if it doesn't exist.
	checkpointDir := filepath.Join(kv.dir, checkpointDir)
	err := os.MkdirAll(checkpointDir, 0755)
	if err != nil {
		log.Println("Error creating checkpoint directory:", err)
		return err
	}

	segmentID := kv.wal.GetCurrentSegmentID()

	// 2. Roll to a new segment.
	err = kv.wal.RollToNewSegment()
	if err != nil {
		log.Println("Error rolling to a new segment:", err)
		return err
	}

	// 1. Persist the memtable to a new segment file.
	segmentFilePath := kv.getSegmentFilePath(segmentID)

	// 3. Append a special CHECKPOINT record to the WAL.
	kv.wal.Append([]byte(lib.CHECKPOINT), []byte(segmentFilePath))

	// 4. Persist the memtable to a new segment file.
	err = kv.memState.Flush(segmentFilePath)
	if err != nil {
		log.Println("Error flushing memtable to segment file:", err)
		return err
	}

	// 5. Persist the sparse index to a new sparse index file.
	sparseIndexFilePath := kv.getSparseIndexFilePath(segmentID)
	err = kv.memState.FlushSparseIndex(sparseIndexFilePath)
	if err != nil {
		log.Println("Error flushing sparse index to sparse index file:", err)
		return err
	}

	// 6. Atomically update a CHECKPOINT file which records the last segment file path.
	err = kv.updateCheckpointFile(segmentFilePath)
	if err != nil {
		log.Println("Error updating checkpoint file:", err)
		return err
	}

	// 7. Remove all old WAL files. This process can be done in the background without holding back the main thread.
	err = kv.cleanUpWALFiles(segmentID)
	if err != nil {
		log.Println("Error cleaning up WAL files:", err)
		return err
	}

	// TODO: 8. Remove all old checkpoints via compaction.
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
		wal.activeSegmentID = lastSegmentID + 1
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
					activeSegmentID: segmentID,
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

// Open opens/creates the log file and initializes the KVStore object.
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
// 2. Delete API
// 3. Recovery sparse index
func Open(config *Configuration) (*KVStore, error) {
	// Create the master directory if it doesn't exist.
	err := os.MkdirAll(config.GetBaseDir(), 0755)
	if err != nil {
		log.Fatalf("Error creating master directory: %v", err)
	}

	// Read the checkpoint if exists.
	lastSegmentID := uint64(0)

	memState := NewMemState()
	lastSegmentID, err = tryRecoverSparseIndex(config.GetBaseDir(), memState)
	if err != nil {
		// Try to recover from the segment file.
		memState, err = tryRecoverFromCheckpoint(filepath.Join(config.GetBaseDir(), checkpointDir, getSegmentFileNameFromSegmentId(lastSegmentID)))
		if err != nil {
			log.Println("Open: Error recovering from checkpoint:", err)
			return nil, lib.ErrCheckpointCorrupted
		}
		log.Println("Open: Recovered from segment:", lastSegmentID)
	}

	// Recover from WAL files.
	wal, err := recoverFromWALs(lastSegmentID, filepath.Join(config.GetBaseDir(), logsDir), memState, config.GetCheckpointSize())
	if err != nil {
		log.Println("Open: Error recovering from WAL files:", err)
		return nil, err
	}

	// Return the fully initialized, ready-to-use KVStore object.
	return &KVStore{
		wal:      wal,
		memState: memState,
		dir:      config.GetBaseDir(),
	}, nil
}
