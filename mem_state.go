package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"slices"
	"strings"
)

// MemState is a simple in-memory key-value store. It doesn't have any concurrency protection.
type MemState struct {
	state map[string][]byte // key -> value
}

// Pair is a key-value pair.
type pair struct {
	Key   string
	Value []byte
}

// GetAllSortedPairs returns all the key-value pairs in the memtable sorted by key.
func (m *MemState) GetAllSortedPairs() []pair {
	entries := make([]pair, 0, len(m.state))
	for key := range m.state {
		entries = append(entries, pair{Key: key, Value: m.state[key]})
	}
	slices.SortFunc(entries, func(a, b pair) int {
		return strings.Compare(a.Key, b.Key)
	})
	return entries
}

func NewMemState() *MemState {
	return &MemState{
		state: make(map[string][]byte),
	}
}

func (m *MemState) Get(key []byte) ([]byte, error) {
	value, ok := m.state[string(key)]
	if !ok {
		return nil, errors.New("key not found")
	}
	return value, nil
}

func (m *MemState) Put(key, value []byte) error {
	m.state[string(key)] = value
	return nil
}

func (m *MemState) Print() {
	fmt.Println("========== MemState starts ==========")
	for k, v := range m.state {
		fmt.Printf("%s: %s\n", k, string(v))
	}
	fmt.Println("========== MemState ends ==========")
}

// createNewSegmentFile creates a new segment file.
func (m *MemState) createNewSegmentFile(filePath string) (*os.File, error) {
	segmentFile, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Println("Error creating segment file:", err)
		return nil, err
	}
	return segmentFile, nil
}

// Flush flushes the memtable to the segment file.
// It writes the entries to the segment file in sorted order.
// TODO: It also updates the offset in the sparse index.
func (m *MemState) Flush(filePath string) error {
	segmentFile, err := m.createNewSegmentFile(filePath)
	if err != nil {
		log.Println("Error creating segment file:", err)
		return err
	}
	defer segmentFile.Close()
	// Seek to the beginning of the file (offset 0)
	_, err = segmentFile.Seek(0, io.SeekStart)
	if err != nil {
		log.Println("Error seeking to start:", err)
		return err
	}
	entries := m.GetAllSortedPairs()
	for _, entry := range entries {
		bytes := getKVRecordBytes([]byte(entry.Key), entry.Value)
		_, err := segmentFile.Write(bytes)
		if err != nil {
			log.Println("Error writing to segment file:", err)
			return err
		}
	}
	// Sync the segment file to ensure all data is written to disk.
	err = segmentFile.Sync()
	if err != nil {
		log.Println("Error flushing to segment file:", err)
		return err
	}
	return nil
}

// getNextKVRecord reads the next KV record from the file.
func getNextKVRecord(file *os.File) (KVRecord, error) {
	// Read checksum first.
	checksumBytes := make([]byte, 4)
	_, err := file.Read(checksumBytes)
	if err != nil {
		return KVRecord{}, err
	}
	checksum := binary.LittleEndian.Uint32(checksumBytes)
	// Read key size next.
	keySizeBytes := make([]byte, 4)
	_, err = file.Read(keySizeBytes)
	if err != nil {
		return KVRecord{}, err
	}
	keySize := binary.LittleEndian.Uint32(keySizeBytes)
	// Read value size next.
	valueSizeBytes := make([]byte, 4)
	_, err = file.Read(valueSizeBytes)
	if err != nil {
		return KVRecord{}, err
	}
	valueSize := binary.LittleEndian.Uint32(valueSizeBytes)
	// Read key + value next.
	dataBytes := make([]byte, keySize+valueSize)
	_, err = file.Read(dataBytes)
	if err != nil {
		return KVRecord{}, err
	}
	// Compute the checksum.
	computedChecksum := ComputeChecksum(slices.Concat(keySizeBytes, valueSizeBytes, dataBytes))
	if computedChecksum != checksum {
		return KVRecord{}, ErrBadChecksum
	}
	// Return the record.
	return KVRecord{
		CheckSum:  checksum,
		KeySize:   keySize,
		ValueSize: valueSize,
		Key:       dataBytes[:keySize],
		Value:     dataBytes[keySize:],
	}, nil
}

// recoverFromCheckpoint recovers the memtable from a checkpoint file.
// Each record is in the format of: checksum + key size + value size + key + value.
func recoverFromCheckpoint(checkpointFilePath string) (*MemState, error) {
	memState := NewMemState()
	file, err := os.Open(checkpointFilePath)
	if err != nil {
		log.Println("Error opening checkpoint file:", err)
		return nil, err
	}
	defer file.Close()
	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		log.Println("Error seeking to start:", err)
		return nil, err
	}
	for {
		_, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			log.Println("Error seeking to current position:", err)
			break
		}
		record, err := getNextKVRecord(file)
		if err != nil {
			if err != io.EOF {
				log.Println("Error getting next KV record:", err)
			}
			break
		}
		memState.Put(record.Key, record.Value)
	}
	return memState, nil
}
