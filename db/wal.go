package db

import (
	"encoding/binary"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"wal/lib"
)

const (
	headerSize   = 20
	checksumSize = 4
	// CheckpointSize = 64 * 1024 // 64KiB
	CheckpointSize = 1024 // 1KiB for testing
	AppendFlags    = os.O_RDWR | os.O_CREATE | os.O_APPEND
)

type WAL struct {
	// Lock is needed because the WAL can be a standalone component used by other components so it must take care of its own concurrency.
	mu sync.Mutex

	// The directory where the WAL files are stored.
	dir string

	// The file handle for the current, active segment we are writing to.
	activeFile *os.File

	// The ID of the active segment (e.g., 1 for wal-0001).
	activeSegmentID uint64

	// The max size for each segment file before we roll to a new one.
	segmentSize int64

	// The last sequence number we have written to the active segment.
	lastSequenceNum uint64
}

// prepareRecordData prepares the record data for the log.
func prepareRecordData(sequenceNum uint64, key, value []byte) []byte {
	keySize := uint32(len(key))
	valueSize := uint32(len(value))
	bytes := make([]byte, headerSize+keySize+valueSize)
	binary.LittleEndian.PutUint64(bytes[4:12], sequenceNum)
	binary.LittleEndian.PutUint32(bytes[12:16], keySize)
	binary.LittleEndian.PutUint32(bytes[16:20], valueSize)

	copy(bytes[headerSize:], key)
	copy(bytes[headerSize+keySize:], value)

	return bytes
}

// createLogRecord creates a log record for the given sequence number, key, and value.
func createLogRecord(sequenceNum uint64, key, value []byte) LogRecord {
	recordData := prepareRecordData(sequenceNum, key, value)

	return LogRecord{
		// The checksum contains the entire record except for the first 4 bytes.
		CheckSum:    ComputeChecksum(recordData[4:]),
		SequenceNum: sequenceNum,
		KeySize:     uint32(len(key)),
		ValueSize:   uint32(len(value)),
		Key:         key,
		Value:       value,
	}
}

// Append appends a new record to the log.
func (l *WAL) Append(key, value []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// 1. Increment the sequence number.
	l.lastSequenceNum++

	// 2. Create the record using this new sequence number.
	record := createLogRecord(l.lastSequenceNum, key, value)

	// 3. Encode the record to its binary format.
	encodedRecord := record.Serialize()

	// 4. Write and Sync.
	_, err := l.activeFile.Write(encodedRecord)
	if err != nil {
		return err
	}

	err = l.activeFile.Sync() // fsync()
	if err != nil {
		log.Println("Error syncing file:", err)
		return err
	}

	// 5. Check if the log has reached its size threshold.
	fileInfo, err := l.activeFile.Stat()
	if err != nil {
		log.Println("Error getting file info:", err)
		return err
	}

	// 6. Check if the segment size has been reached. If so, roll to a new segment
	if fileInfo.Size() >= l.segmentSize {
		// Special error to signal that the log has reached its size threshold.
		// This is used to trigger a checkpoint.
		log.Println("WAL is ready to be checkpointed; segment ID:", l.GetCurrentSegmentID())
		return lib.ErrCheckpointNeeded
	}
	return nil
}

// GetCurrentSegmentID returns the ID of the current segment.
func (l *WAL) GetCurrentSegmentID() uint64 {
	return l.activeSegmentID
}

func (l *WAL) GetLastSegmentID() uint64 {
	if l.activeSegmentID == 1 {
		log.Panicln("No segments have been written yet -- this function should not have been called.")
		return 0
	}
	return l.activeSegmentID - 1
}

// GetLastSegmentID returns the ID of the last segment.
// It panics if no segments have been written yet.
func (l *WAL) GetLastSegmentFile() (*os.File, error) {
	lastSegmentID := l.GetLastSegmentID()
	if lastSegmentID == 0 {
		log.Panicln("No segments have been written yet -- this function should not have been called.")
		return nil, errors.New("no segments have been written yet")
	}

	lastSegmentFileName := getWalFileNameFromSegmentID(lastSegmentID)
	lastSegmentFilePath := filepath.Join(l.dir, lastSegmentFileName)
	lastSegmentFile, err := os.OpenFile(lastSegmentFilePath, os.O_RDONLY, 0644)
	if err != nil {
		log.Println("Error opening last segment file:", err)
		return nil, err
	}
	return lastSegmentFile, nil
}

// RollToNewSegment rolls to a new segment.
func (l *WAL) RollToNewSegment() error {
	// 1. Close the current segment.
	err := l.activeFile.Close()
	if err != nil {
		return err
	}
	// 2. Open a new segment file.
	newSegmentID := l.activeSegmentID + 1
	newSegmentFileName := getWalFileNameFromSegmentID(newSegmentID)
	newSegmentFilePath := filepath.Join(l.dir, newSegmentFileName)
	newSegmentFile, err := os.OpenFile(newSegmentFilePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	// 3. Update the active file and segment ID.
	l.activeFile = newSegmentFile
	l.activeSegmentID = newSegmentID

	log.Println("rollToNewSegment: Rolled to new segment:", newSegmentID)

	// 4. Return nil.
	return nil
}

// recoverNextRecord reads the next record from the WAL file.
// It returns the record and the sequence number of the next record.
// It returns an error if the checksum is invalid.
// It returns an error if the record is not found.
// It returns an error if the file is not found.
// It returns an error if the file is not readable.
// It returns an error if the file is not seekable.
func recoverNextRecord(reader io.Reader) (*LogRecord, error) {
	buf := make([]byte, headerSize)

	_, err := io.ReadFull(reader, buf)
	if err != nil {
		// EOF is handled
		if err != io.EOF {
			log.Println("Error reading next WAL record:", err)
		}
		return nil, err
	}

	// Read the header fields.
	checksum := binary.LittleEndian.Uint32(buf[:checksumSize])
	sequenceNum := binary.LittleEndian.Uint64(buf[checksumSize:headerSize])
	keySize := binary.LittleEndian.Uint32(buf[checksumSize+8 : headerSize])
	valueSize := binary.LittleEndian.Uint32(buf[checksumSize+12 : headerSize])

	// Assuming a 64-bit system, the sum of keySize and valueSize is always positive.
	keyValueSize := int(keySize) + int(valueSize)
	payloadBuf := make([]byte, keyValueSize)
	_, err = io.ReadFull(reader, payloadBuf)
	if err != nil {
		if err != io.EOF {
			log.Println("Error reading next WAL record:", err)
		}
		return nil, err
	}

	// Compute checksum of entire record.
	dataToVerify := append(buf[checksumSize:], payloadBuf...)
	computedChecksum := ComputeChecksum(dataToVerify)
	if computedChecksum != checksum {
		// Bad checksum.
		log.Println("Error reading next WAL record: checksum mismatch")
		return nil, lib.ErrBadChecksum
	}

	return &LogRecord{
		CheckSum:    checksum,
		SequenceNum: sequenceNum,
		KeySize:     keySize,
		ValueSize:   valueSize,
		Key:         payloadBuf[:keySize],
		Value:       payloadBuf[keySize:],
	}, nil
}

// Close shuts down the log file.
func (l *WAL) Close() error {
	// Implementation will:
	// 1. Lock the mutex.
	// 2. Defer unlocking.
	// 3. Close the file handle: l.file.Close()
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.activeFile.Close()
}
