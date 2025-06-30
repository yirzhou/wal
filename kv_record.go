package main

import (
	"encoding/binary"
	"fmt"
)

// Simple abstraction for a log record.
// +----------+---------------+-----------+-------------+-----+-------+
// | Checksum | Key Size  | Value Size  | Key | Value |
// | (4 bytes)| (4 bytes) |  (4 bytes)  | ... |  ...  |
// +----------+---------------+-----------+-------------+-----+-------+
// | <----------- Header (12 bytes) -----------------> | <- Payload -> |
type KVRecord struct {
	CheckSum  uint32
	KeySize   uint32
	ValueSize uint32
	Key       []byte
	Value     []byte
}

// Size returns the size of the log record.
func (lr *KVRecord) Size() int {
	return 12 + int(lr.KeySize) + int(lr.ValueSize)
}

// getBytesForSSTableEntry returns the bytes for an SSTable entry.
// The entry is the following:
// Checksum (4 bytes) + key size (4 bytes) + value size (4 bytes) + key (variable size) + value (variable size)
func getKVRecordBytes(key, value []byte) []byte {
	// Put the key and value into the bytes first.
	keySize := uint32(len(key))
	valueSize := uint32(len(value))
	bytes := make([]byte, 12+keySize+valueSize)
	binary.LittleEndian.PutUint32(bytes[4:8], keySize)
	binary.LittleEndian.PutUint32(bytes[8:12], valueSize)
	copy(bytes[12:], key)
	copy(bytes[12+keySize:], value)
	// Compute the checksum and put it in the first 4 bytes.
	checksum := ComputeChecksum(bytes[4:])
	binary.LittleEndian.PutUint32(bytes[:4], checksum)
	return bytes
}

// DeserializeKVRecord converts a byte array to a KVRecord according to the specified format.
func DeserializeKVRecord(data []byte) (KVRecord, error) {
	lr := KVRecord{}
	if len(data) < 12 {
		return lr, fmt.Errorf("log record is too short")
	}

	lr.CheckSum = binary.LittleEndian.Uint32(data[:4])
	lr.KeySize = binary.LittleEndian.Uint32(data[4:8])
	lr.ValueSize = binary.LittleEndian.Uint32(data[8:12])

	// Validate that we have enough data for the payload
	expectedSize := 12 + int(lr.KeySize) + int(lr.ValueSize)
	if len(data) < expectedSize {
		return lr, fmt.Errorf("log record data too short: expected %d bytes, got %d", expectedSize, len(data))
	}

	offset := 12
	lr.Key = make([]byte, lr.KeySize)
	copy(lr.Key, data[offset:offset+int(lr.KeySize)])
	offset += int(lr.KeySize)

	lr.Value = make([]byte, lr.ValueSize)
	copy(lr.Value, data[offset:offset+int(lr.ValueSize)])

	return lr, nil
}

// Serialize converts the LogRecord to a byte array according to the specified format.
// The format is: Header (20 bytes) + Payload (Key + Value)
func (lr *KVRecord) Serialize() []byte {
	// Calculate total size: header (12 bytes) + key size + value size
	totalSize := 12 + len(lr.Key) + len(lr.Value)

	// Create buffer with exact size needed
	buf := make([]byte, totalSize)

	// Write header fields in little-endian format
	offset := 0

	// Checksum (4 bytes)
	binary.LittleEndian.PutUint32(buf[offset:], lr.CheckSum)
	offset += 4

	// KeySize (4 bytes)
	binary.LittleEndian.PutUint32(buf[offset:], lr.KeySize)
	offset += 4

	// ValueSize (4 bytes)
	binary.LittleEndian.PutUint32(buf[offset:], lr.ValueSize)
	offset += 4

	// Write payload: Key
	copy(buf[offset:], lr.Key)
	offset += len(lr.Key)

	// Write payload: Value
	copy(buf[offset:], lr.Value)

	return buf
}

// ComputeChecksumForRecord calculates checksum for the entire log record
// (excluding the checksum field itself)
func (lr *KVRecord) ComputeChecksumForRecord() uint32 {
	// Create a temporary buffer with all fields except checksum
	// We'll serialize without the checksum field
	// The first 4 bytes are the checksum, so we need to subtract 4 from the total size.
	tempSize := 12 + len(lr.Key) + len(lr.Value) // 12 bytes for non-checksum header fields
	tempBuf := make([]byte, tempSize)

	offset := 0

	// KeySize (4 bytes)
	binary.LittleEndian.PutUint32(tempBuf[offset:], lr.KeySize)
	offset += 4

	// ValueSize (4 bytes)
	binary.LittleEndian.PutUint32(tempBuf[offset:], lr.ValueSize)
	offset += 4

	// Key
	copy(tempBuf[offset:], lr.Key)
	offset += len(lr.Key)

	// Value
	copy(tempBuf[offset:], lr.Value)

	return ComputeChecksum(tempBuf)
}
