package db

import (
	"encoding/binary"
	"log"
	"os"
	"wal/lib"
)

// SegmentMetadata is the metadata for a segment file.
type SegmentMetadata struct {
	id       uint64
	level    uint32
	minKey   []byte
	maxKey   []byte
	filePath string
}

// GetSparseIndexFileName returns the file name of the sparse index file for the segment.
func (s *SegmentMetadata) GetSparseIndexFileName() string {
	return getSparseIndexFileNameFromSegmentId(s.id)
}

// ReadSegmentMetadata reads the segment metadata from the file.
// The bytes are in the following order:
// checksum (4 bytes), id (8 bytes), level (4 bytes), minKeySize (4 bytes), maxKeySize (4 bytes), filePathSize (4 bytes), minKey (variable), maxKey (variable), filePath (variable)
func ReadSegmentMetadata(file *os.File) (*SegmentMetadata, error) {
	bytes := make([]byte, 4+8+4+4+4+4)
	_, err := file.Read(bytes)
	if err != nil {
		log.Println("ReadSegmentMetadata: Error reading segment metadata:", err)
		return nil, err
	}
	// Read the ID
	id := binary.LittleEndian.Uint64(bytes[4:12])
	// Read the level
	level := binary.LittleEndian.Uint32(bytes[12:16])
	// Read the min key size
	minKeySize := binary.LittleEndian.Uint32(bytes[16:20])
	// Read the max key size
	maxKeySize := binary.LittleEndian.Uint32(bytes[20:24])
	// Read the file path size
	filePathSize := binary.LittleEndian.Uint32(bytes[24:28])
	// Read the min key
	minKey := make([]byte, minKeySize)
	_, err = file.Read(minKey)
	if err != nil {
		log.Println("ReadSegmentMetadata: Error reading min key:", err)
		return nil, err
	}
	// Read the max key
	maxKey := make([]byte, maxKeySize)
	_, err = file.Read(maxKey)
	if err != nil {
		log.Println("ReadSegmentMetadata: Error reading max key:", err)
		return nil, err
	}
	// Read the file path
	filePath := make([]byte, filePathSize)
	_, err = file.Read(filePath)
	if err != nil {
		log.Println("ReadSegmentMetadata: Error reading file path:", err)
		return nil, err
	}

	log.Println("ReadSegmentMetadata: Reading file path:", string(filePath))
	// Get the entire data bytes
	dataBytes := append(bytes[4:], minKey...)
	dataBytes = append(dataBytes, maxKey...)
	dataBytes = append(dataBytes, filePath...)
	// Compute the checksum
	checksum := ComputeChecksum(dataBytes)
	// Check if the checksum is correct
	if checksum != binary.LittleEndian.Uint32(bytes[:4]) {
		log.Println("ReadSegmentMetadata: Checksum mismatch:", checksum, binary.LittleEndian.Uint32(bytes[:4]))
		return nil, lib.ErrBadChecksum
	}
	// Return the segment metadata
	return &SegmentMetadata{
		id:       id,
		minKey:   minKey,
		maxKey:   maxKey,
		filePath: string(filePath),
		level:    level,
	}, nil
}

// GetBytes returns the bytes representation of the segment metadata.
// The bytes are in the following order:
// checksum (4 bytes), id (8 bytes), level (4 bytes), minKeySize (4 bytes), maxKeySize (4 bytes), filePathSize (4 bytes), minKey (variable), maxKey (variable), filePath (variable)
func (s *SegmentMetadata) GetBytes() []byte {
	// checksum, id, level, minKeySize, maxKeySize, filePathSize, minKey, maxKey, filePath
	bytes := make([]byte, 4+8+4+4+4+4+len(s.minKey)+len(s.maxKey)+len(s.filePath))
	// Write the ID first
	binary.LittleEndian.PutUint64(bytes[4:12], s.id)
	// Write the level
	binary.LittleEndian.PutUint32(bytes[12:16], s.level)
	// Write the min key size
	binary.LittleEndian.PutUint32(bytes[16:20], uint32(len(s.minKey)))
	// Write the max key size
	binary.LittleEndian.PutUint32(bytes[20:24], uint32(len(s.maxKey)))
	// Write the file path size
	binary.LittleEndian.PutUint32(bytes[24:28], uint32(len(s.filePath)))
	// Write the min key
	copy(bytes[28:28+len(s.minKey)], s.minKey)
	// Write the max key
	copy(bytes[28+len(s.minKey):28+len(s.minKey)+len(s.maxKey)], s.maxKey)
	// Write the file path
	copy(bytes[28+len(s.minKey)+len(s.maxKey):28+len(s.minKey)+len(s.maxKey)+len(s.filePath)], s.filePath)
	// Write the checksum
	checksum := ComputeChecksum(bytes[4:])
	binary.LittleEndian.PutUint32(bytes[:4], checksum)
	// Return the bytes
	return bytes
}
