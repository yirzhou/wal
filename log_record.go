package main

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// Simple abstraction for a log record.
// +----------+---------------+-----------+-------------+-----+-------+
// | Checksum | Sequence Num  | Key Size  | Value Size  | Key | Value |
// | (4 bytes)|   (8 bytes)   | (4 bytes) |  (4 bytes)  | ... |  ...  |
// +----------+---------------+-----------+-------------+-----+-------+
// | <----------- Header (20 bytes) -----------------> | <- Payload -> |
type LogRecord struct {
	CheckSum    uint32
	SequenceNum uint64
	KeySize     uint32
	ValueSize   uint32
	Key         []byte
	Value       []byte
}

// Size returns the size of the log record.
func (lr *LogRecord) Size() int {
	return 20 + int(lr.KeySize) + int(lr.ValueSize)
}

// Deserialize converts a byte array to a LogRecord according to the specified format.
func Deserialize(data []byte) (LogRecord, error) {
	lr := LogRecord{}
	if len(data) < 20 {
		return lr, fmt.Errorf("log record is too short")
	}

	lr.CheckSum = binary.LittleEndian.Uint32(data[:4])
	lr.SequenceNum = binary.LittleEndian.Uint64(data[4:12])
	lr.KeySize = binary.LittleEndian.Uint32(data[12:16])
	lr.ValueSize = binary.LittleEndian.Uint32(data[16:20])

	// Validate that we have enough data for the payload
	expectedSize := 20 + int(lr.KeySize) + int(lr.ValueSize)
	if len(data) < expectedSize {
		return lr, fmt.Errorf("log record data too short: expected %d bytes, got %d", expectedSize, len(data))
	}

	offset := 20
	lr.Key = make([]byte, lr.KeySize)
	copy(lr.Key, data[offset:offset+int(lr.KeySize)])
	offset += int(lr.KeySize)

	lr.Value = make([]byte, lr.ValueSize)
	copy(lr.Value, data[offset:offset+int(lr.ValueSize)])

	return lr, nil
}

// Serialize converts the LogRecord to a byte array according to the specified format.
// The format is: Header (20 bytes) + Payload (Key + Value)
func (lr *LogRecord) Serialize() []byte {
	// Calculate total size: header (20 bytes) + key size + value size
	totalSize := 20 + len(lr.Key) + len(lr.Value)

	// Create buffer with exact size needed
	buf := make([]byte, totalSize)

	// Write header fields in little-endian format
	offset := 0

	// Checksum (4 bytes)
	binary.LittleEndian.PutUint32(buf[offset:], lr.CheckSum)
	offset += 4

	// SequenceNum (8 bytes)
	binary.LittleEndian.PutUint64(buf[offset:], lr.SequenceNum)
	offset += 8

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

// ComputeChecksum calculates a CRC32 checksum of the given byte slice
func ComputeChecksum(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

// ComputeChecksumForRecord calculates checksum for the entire log record
// (excluding the checksum field itself)
func (lr *LogRecord) ComputeChecksumForRecord() uint32 {
	// Create a temporary buffer with all fields except checksum
	// We'll serialize without the checksum field
	// The first 4 bytes are the checksum, so we need to subtract 4 from the total size.
	tempSize := 16 + len(lr.Key) + len(lr.Value) // 16 bytes for non-checksum header fields
	tempBuf := make([]byte, tempSize)

	offset := 0

	// SequenceNum (8 bytes)
	binary.LittleEndian.PutUint64(tempBuf[offset:], lr.SequenceNum)
	offset += 8

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
