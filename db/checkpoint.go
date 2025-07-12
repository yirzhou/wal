package db

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"
	"wal/lib"
)

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

	segmentID := kv.GetCurrentSegmentIDUnsafe()

	// 2. Roll to a new segment.
	err = kv.RollToNewSegment()
	if err != nil {
		log.Println("Error rolling to a new segment:", err)
		return err
	}

	// 1. Persist the memtable to a new segment file.
	segmentFilePath := kv.getSegmentFilePath(segmentID)

	// 3. Append a special CHECKPOINT record to the WAL.
	kv.wal.Append([]byte(lib.CHECKPOINT), []byte(segmentFilePath))

	// 4. Persist the memtable to a new segment file.
	minKey, maxKey, err := kv.memState.Flush(segmentFilePath)
	if err != nil {
		log.Println("Error flushing memtable to segment file:", err)
		return err
	}

	// Initialize the first level if it's not already initialized.
	if len(kv.levels) < 1 {
		kv.levels = make([][]SegmentMetadata, 1)
	}
	// Add the segment file to Level 0.
	kv.levels[0] = append(kv.levels[0], SegmentMetadata{
		id:       segmentID,
		level:    0,
		minKey:   minKey,
		maxKey:   maxKey,
		filePath: segmentFilePath,
	})

	// 5. Persist the sparse index to a new sparse index file.
	sparseIndexFilePath := kv.getSparseIndexFilePath(segmentID)
	err = kv.memState.FlushSparseIndex(sparseIndexFilePath)
	if err != nil {
		log.Println("Error flushing sparse index to sparse index file:", err)
		return err
	}

	// 6. Atomically update a CHECKPOINT file which records the last segment file path.
	manifestFilePath, err := kv.flushManifestFile(segmentID)
	if err != nil {
		log.Println("Error flushing manifest file:", err)
		return err
	}
	err = kv.updateCheckpointFile(manifestFilePath)
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

// updateCheckpointFile atomically updates the checkpoint file with the new segment file path.
func (kv *KVStore) updateCheckpointFile(manifestFilePath string) error {
	// Get the current unix timestamp as a string.
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	tempCheckpointFilePath := filepath.Join(kv.dir, checkpointDir, fmt.Sprintf("%s.%s", currentFile, timestamp))
	tempCheckpointFile, err := os.OpenFile(tempCheckpointFilePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Println("Error opening temp checkpoint file:", err)
		return err
	}
	defer tempCheckpointFile.Close()

	// Compute the checksum of the segment file path.
	checksum := ComputeChecksum([]byte(manifestFilePath))
	bytes := make([]byte, 4+len(manifestFilePath))
	binary.LittleEndian.PutUint32(bytes[:4], checksum)
	copy(bytes[4:], manifestFilePath)

	// Write the segment file path to the temp checkpoint file.
	_, err = tempCheckpointFile.Write(bytes)
	if err != nil {
		log.Println("Error writing to temp checkpoint file:", err)
		return err
	}
	tempCheckpointFile.Sync()

	// Rename the temp checkpoint file to the actual checkpoint file.
	err = os.Rename(tempCheckpointFilePath, filepath.Join(kv.dir, checkpointDir, currentFile))
	if err != nil {
		log.Println("Error renaming temp checkpoint file:", err)
		return err
	}
	return nil
}

// flushManifestFile flushes the manifest file for a given segment ID.
func (kv *KVStore) flushManifestFile(segmentID uint64) (string, error) {
	manifestFilePath := kv.getManifestFilePath(segmentID)
	manifestFile, err := os.OpenFile(manifestFilePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Println("Error opening manifest file:", err)
		return "", err
	}
	defer manifestFile.Close()

	// Write the segment metadata to the manifest file.
	for _, level := range kv.levels {
		for _, segment := range level {
			bytes := segment.GetBytes()
			_, err := manifestFile.Write(bytes)
			if err != nil {
				log.Println("Error writing to manifest file:", err)
				return "", err
			}
			err = manifestFile.Sync() // Sync after each segment.
			if err != nil {
				log.Println("Error syncing manifest file:", err)
				return "", err
			}
		}
	}
	return manifestFilePath, nil
}

// FlushSparseIndex flushes the sparse index to the sparse index file.
func (m *MemState) FlushSparseIndex(filePath string) error {
	segmentID, err := GetSegmentIDFromIndexFilePath(filePath)
	if err != nil {
		log.Println("Error getting segment ID from segment file path:", err)
		return err
	}
	sparseIndexFile, err := os.OpenFile(filePath, AppendFlags, 0644)
	if err != nil {
		log.Println("Error creating sparse index file:", err)
		return err
	}
	defer sparseIndexFile.Close()
	sparseIndexFile.Seek(0, io.SeekStart)
	for _, entry := range m.sparseIndexMap[segmentID] {
		sparseIndexFile.Seek(0, io.SeekCurrent)
		_, err := sparseIndexFile.Write(GetSparseIndexBytes(segmentID, entry.key, entry.offset))
		if err != nil {
			log.Println("FlushSparseIndex: Error writing to sparse index file:", err)
			return err
		}
	}
	err = sparseIndexFile.Sync()
	if err != nil {
		log.Println("FlushSparseIndex: Error syncing sparse index file:", err)
		return err
	}
	log.Println("FlushSparseIndex: Successfully flushed sparse index to file:", filePath)
	return nil
}
