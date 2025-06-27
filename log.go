package main

import (
	"encoding/binary"
	"io"
	"os"
	"sync"
)

const (
	headerSize   = 20
	checksumSize = 4
)

type Log struct {
	// We need a mutex to protect access from multiple goroutines
	// in the future. It's good practice to include it from the start.
	mu sync.Mutex

	// The file handle to our wal.log file.
	file *os.File

	path string

	lastSequenceNum uint64

	// We'll add more fields later, like the current sequence number.
}

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

func (l *Log) Append(key, value []byte) error {
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

func (l *Log) recover() error {
	for {
		buf := make([]byte, headerSize)
		n, err := l.file.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if n < headerSize {
			// The header itself is not even valid. Stop here.
			break
		}

		// Read the header fields.
		checksum := binary.LittleEndian.Uint32(buf[:checksumSize])
		sequenceNum := binary.LittleEndian.Uint64(buf[checksumSize:headerSize])
		keySize := binary.LittleEndian.Uint32(buf[checksumSize+8 : headerSize])
		valueSize := binary.LittleEndian.Uint32(buf[checksumSize+12 : headerSize])

		// Assuming a 64-bit system, the sum of keySize and valueSize is always positive.
		keyValueSize := int(keySize) + int(valueSize)
		keyValue := make([]byte, keyValueSize)
		_, err = l.file.Read(keyValue)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		// Append the header and the key and value to the buffer.
		entireBytes := append(buf[checksumSize:], keyValue...)
		// Compute the checksum of the entire record.
		computedChecksum := ComputeChecksum(entireBytes)
		if computedChecksum != checksum {
			// Invalid checksum. Stop here
			break
		}

		// Update the last sequence number if this record has a higher sequence number.
		if sequenceNum > l.lastSequenceNum {
			l.lastSequenceNum = sequenceNum
		}
	}
	return nil
}

// Close shuts down the log file.
func (l *Log) Close() error {
	// Implementation will:
	// 1. Lock the mutex.
	// 2. Defer unlocking.
	// 3. Close the file handle: l.file.Close()
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file.Close()
}
