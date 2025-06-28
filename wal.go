package main

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
)

const (
	headerSize   = 20
	checksumSize = 4
)

var ErrBadChecksum = errors.New("wal: checksum mismatch")

type WAL struct {
	// We need a mutex to protect access from multiple goroutines
	// in the future. It's good practice to include it from the start.
	mu sync.Mutex

	// The file handle to our wal.log file.
	file *os.File

	path string

	lastSequenceNum uint64

	// We'll add more fields later, like the current sequence number.
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
	_, err := l.file.Write(encodedRecord)
	if err != nil {
		return err
	}

	return l.file.Sync() // fsync()
}

func recoverNextRecord(reader io.Reader) (*LogRecord, error) {
	buf := make([]byte, headerSize)

	_, err := io.ReadFull(reader, buf)
	if err != nil {
		// EOF is handled
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
		return nil, err
	}

	// Compute checksum of entire record.
	dataToVerify := append(buf[checksumSize:], payloadBuf...)
	computedChecksum := ComputeChecksum(dataToVerify)
	if computedChecksum != checksum {
		// Bad checksum.
		return nil, ErrBadChecksum
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
	return l.file.Close()
}
