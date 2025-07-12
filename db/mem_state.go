package db

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"slices"
	"strings"
	"wal/lib"
)

// MemState is a simple in-memory key-value store. It doesn't have any concurrency protection.
type MemState struct {
	state          map[string][]byte // key -> value
	sparseIndexMap sparseIndexes     // segmentID -> sparseIndex
}

// Pair is a key-value pair.
type pair struct {
	Key   string
	Value []byte
}

// A single entry in our sparse index.
type sparseIndexEntry struct {
	key    []byte // The key itself
	offset int64  // The byte offset in the segment file where this key's record begins
}

// The sparse index for one segment file.
type sparseIndex []sparseIndexEntry

type sparseIndexes map[uint64]sparseIndex

// GetSparseIndex returns the sparse index for a given segment ID.
func (m *MemState) GetSparseIndex(segmentID uint64) sparseIndex {
	res, ok := m.sparseIndexMap[segmentID]
	if !ok {
		return make(sparseIndex, 0)
	}
	return res
}

func (m *MemState) RemoveSparseIndexEntry(segmentID uint64) {
	delete(m.sparseIndexMap, segmentID)
}

func (m *MemState) AddSparseIndexEntriesBulk(segmentID uint64, sparseIndexEntries []sparseIndexEntry) {
	m.sparseIndexMap[segmentID] = sparseIndexEntries
}

// AddSparseIndexEntry adds a new entry to the sparse index.
func (m *MemState) AddSparseIndexEntry(segmentID uint64, key []byte, offset int64) {
	if m.sparseIndexMap[segmentID] == nil {
		m.sparseIndexMap[segmentID] = make(sparseIndex, 0)
	}
	m.sparseIndexMap[segmentID] = append(m.sparseIndexMap[segmentID], sparseIndexEntry{key: key, offset: offset})
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
		state:          make(map[string][]byte),
		sparseIndexMap: make(sparseIndexes),
	}
}

// FindKeyInSparseIndex finds the offset of a key in the sparse index.
func (m *MemState) FindKeyInSparseIndex(segmentID uint64, key []byte) int64 {
	// We are not checking if the slice is empty. If it's empty, zero will be returned. That means that the segment file is empty.
	// In theory, we can directly return -1 to signal that the segment file is empty as a small optimization.
	cmp := func(a, b sparseIndexEntry) int {
		return bytes.Compare(a.key, b.key)
	}
	idx, _ := slices.BinarySearchFunc(m.sparseIndexMap[segmentID], sparseIndexEntry{key: key}, cmp)

	// If the key is larger than the largest key in the segment file, go back by one index.
	if idx == len(m.sparseIndexMap[segmentID]) {
		idx--
	}
	// Check if the key at the idx is larger than the key we are looking for. If it is larger, we need to go back one index.
	if cmp(sparseIndexEntry{key: key}, m.sparseIndexMap[segmentID][idx]) < 0 {
		if idx-1 >= 0 {
			// Return the offset of the previous key.
			idx--
		} else {
			// The key is smaller than the smallest key in the segment file.
			return -1
		}
	}
	return m.sparseIndexMap[segmentID][idx].offset
}

// Get returns the value for a given key.
// If the key is not found, it returns an error.
func (m *MemState) Get(key []byte) ([]byte, error) {
	// Found in MemState.
	value, ok := m.state[string(key)]
	if !ok {
		return nil, errors.New("key not found in memtable")
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

// GetAllSegmentFilePathsDescendingOrder gets all the segment files in the descending order of their segment IDs.
func (m *MemState) GetAllSegmentIDsDescendingOrder() ([]uint64, error) {
	segmentIDs := make([]uint64, 0)
	for segmentID := range m.sparseIndexMap {
		segmentIDs = append(segmentIDs, segmentID)
	}
	slices.Sort(segmentIDs)
	slices.Reverse(segmentIDs)
	return segmentIDs, nil
}

// Flush flushes the memtable to the segment file.
// It writes the entries to the segment file in sorted order.
// TODO: It also updates the offset in the sparse index.
func (m *MemState) Flush(filePath string) ([]byte, []byte, error) {
	segmentID, err := GetSegmentIDFromSegmentFilePath(filePath)
	if err != nil {
		log.Println("Error getting segment ID from segment file path:", err)
		return nil, nil, err
	}
	segmentFile, err := m.createNewSegmentFile(filePath)
	if err != nil {
		log.Println("Error creating segment file:", err)
		return nil, nil, err
	}
	defer segmentFile.Close()
	// Seek to the beginning of the file (offset 0)
	_, err = segmentFile.Seek(0, io.SeekStart)
	if err != nil {
		log.Println("Error seeking to start:", err)
		return nil, nil, err
	}
	entries := m.GetAllSortedPairs()
	offset := int64(0)
	for idx, entry := range entries {
		bytes := getKVRecordBytes([]byte(entry.Key), entry.Value)
		_, err := segmentFile.Write(bytes)
		if err != nil {
			log.Println("Error writing to segment file:", err)
			return nil, nil, err
		}
		// Update the sparse index every 2 records.
		if idx%2 == 0 {
			log.Println("Creating sparse index for segment:", segmentID, "with offset:", offset, "and key:", entry.Key)
			// Create a new file name -> sparse index map.
			if m.sparseIndexMap[segmentID] == nil {
				m.sparseIndexMap[segmentID] = make(sparseIndex, 0)
			}
			m.sparseIndexMap[segmentID] = append(m.sparseIndexMap[segmentID], sparseIndexEntry{key: []byte(entry.Key), offset: offset})
		}
		offset += int64(len(bytes))
	}
	// Sync the segment file to ensure all data is written to disk.
	err = segmentFile.Sync()
	if err != nil {
		log.Println("Error flushing to segment file:", err)
		return nil, nil, err
	}
	// Return the min and max key
	minKey := []byte(entries[0].Key)
	maxKey := []byte(entries[len(entries)-1].Key)
	return minKey, maxKey, nil
}

// getNextSparseIndexRecord reads the next sparse index record from the file.
func getNextSparseIndexRecord(file *os.File) (*SparseIndexRecord, error) {
	headerBytes := make([]byte, 24)
	_, err := file.Read(headerBytes)
	if err != nil {
		if err == io.EOF {
			// no more records
			return nil, nil
		}
		log.Println("getNextSparseIndexRecord:Error reading sparse index header:", err)
		return nil, err
	}
	record, err := DecodeSparseIndexHeader(headerBytes)
	if err != nil {
		log.Println("getNextSparseIndexRecord:Error decoding sparse index header:", err)
		return nil, err
	}
	record.Key = make([]byte, record.KeySize)
	_, err = file.Read(record.Key)
	if err != nil {
		log.Println("getNextSparseIndexRecord:Error reading sparse index key:", err)
		return nil, err
	}

	// Combine the header and the key to get the full record.
	fullBytes := append(headerBytes[4:], record.Key...)
	// Check the checksum.
	computedChecksum := ComputeChecksum(fullBytes)
	if computedChecksum != record.Checksum {
		log.Printf("getNextSparseIndexRecord:Error computing checksum: %d != %d for key: %s\n", computedChecksum, record.Checksum, string(record.Key))
		return nil, lib.ErrBadChecksum
	}
	// Return the record.
	return record, nil
}

// getNextKVRecord reads the next KV record from the file.
func getNextKVRecord(file *os.File) (KVRecord, error) {
	// Read checksum first.
	checksumBytes := make([]byte, 4)
	_, err := file.Read(checksumBytes)
	if err != nil {
		if err != io.EOF {
			log.Println("getNextKVRecord:Error reading checksum:", err)
		}
		return KVRecord{}, err
	}
	checksum := binary.LittleEndian.Uint32(checksumBytes)
	// Read key size next.
	keySizeBytes := make([]byte, 4)
	_, err = file.Read(keySizeBytes)
	if err != nil {
		log.Println("getNextKVRecord:Error reading key size:", err)
		return KVRecord{}, err
	}
	keySize := binary.LittleEndian.Uint32(keySizeBytes)
	// Read value size next.
	valueSizeBytes := make([]byte, 4)
	_, err = file.Read(valueSizeBytes)
	if err != nil {
		log.Println("getNextKVRecord:Error reading value size:", err)
		return KVRecord{}, err
	}
	valueSize := binary.LittleEndian.Uint32(valueSizeBytes)
	// Read key + value next.
	dataBytes := make([]byte, keySize+valueSize)
	_, err = file.Read(dataBytes)
	if err != nil {
		log.Println("getNextKVRecord:Error reading key + value:", err)
		return KVRecord{}, err
	}
	// Compute the checksum.
	computedChecksum := ComputeChecksum(slices.Concat(keySizeBytes, valueSizeBytes, dataBytes))
	if computedChecksum != checksum {
		log.Println("getNextKVRecord:Error computing checksum:", computedChecksum, "!=", checksum)
		return KVRecord{}, lib.ErrBadChecksum
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

// tryRecoverFromCheckpoint recovers the memtable from a checkpoint file.
// Each record is in the format of: checksum + key size + value size + key + value.
func tryRecoverFromCheckpoint(checkpointFilePath string) (*MemState, error) {
	memState := NewMemState()
	file, err := os.Open(checkpointFilePath)
	if err != nil {
		log.Println("tryRecoverFromCheckpoint:Error opening checkpoint file:", err)
		return nil, err
	}
	defer file.Close()
	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		log.Println("tryRecoverFromCheckpoint:Error seeking to start:", err)
		return nil, err
	}
	offset := int64(0)
	for {
		offset, err = file.Seek(0, io.SeekCurrent)
		if err != nil {
			log.Println("tryRecoverFromCheckpoint:Error seeking to current position:", err)
			break
		}
		record, err := getNextKVRecord(file)
		if err != nil {
			if err != io.EOF {
				log.Println("tryRecoverFromCheckpoint:Error getting next KV record:", err)
				// Truncate the file to the last good offset.
				os.Truncate(checkpointFilePath, offset)
			}
			break
		}
		memState.Put(record.Key, record.Value)
	}
	return memState, nil
}
