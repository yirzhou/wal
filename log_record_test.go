package main

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestLogRecord_Serialize(t *testing.T) {
	tests := []struct {
		name     string
		record   LogRecord
		expected []byte
	}{
		{
			name: "basic record",
			record: LogRecord{
				CheckSum:    12345,
				SequenceNum: 67890,
				Key:         []byte("hello"),
				Value:       []byte("world123"),
			},
		},
		{
			name: "empty key and value",
			record: LogRecord{
				CheckSum:    0,
				SequenceNum: 0,
				Key:         []byte{},
				Value:       []byte{},
			},
		},
		{
			name: "large sequence number",
			record: LogRecord{
				CheckSum:    999999,
				SequenceNum: 18446744073709551615, // max uint64
				Key:         []byte("abc"),
				Value:       []byte("defg"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set KeySize and ValueSize from actual lengths
			tt.record.KeySize = uint32(len(tt.record.Key))
			tt.record.ValueSize = uint32(len(tt.record.Value))

			serialized := tt.record.Serialize()

			// Check total size
			expectedSize := 20 + len(tt.record.Key) + len(tt.record.Value)
			if len(serialized) != expectedSize {
				t.Errorf("Serialized size = %d, want %d", len(serialized), expectedSize)
			}

			// Verify header fields
			checksum := binary.LittleEndian.Uint32(serialized[0:4])
			if checksum != tt.record.CheckSum {
				t.Errorf("Checksum = %d, want %d", checksum, tt.record.CheckSum)
			}

			sequenceNum := binary.LittleEndian.Uint64(serialized[4:12])
			if sequenceNum != tt.record.SequenceNum {
				t.Errorf("SequenceNum = %d, want %d", sequenceNum, tt.record.SequenceNum)
			}

			keySize := binary.LittleEndian.Uint32(serialized[12:16])
			if keySize != tt.record.KeySize {
				t.Errorf("KeySize = %d, want %d", keySize, tt.record.KeySize)
			}

			valueSize := binary.LittleEndian.Uint32(serialized[16:20])
			if valueSize != tt.record.ValueSize {
				t.Errorf("ValueSize = %d, want %d", valueSize, tt.record.ValueSize)
			}

			// Verify payload
			keyStart := 20
			keyEnd := keyStart + int(tt.record.KeySize)
			valueStart := keyEnd
			valueEnd := valueStart + int(tt.record.ValueSize)

			if !bytes.Equal(serialized[keyStart:keyEnd], tt.record.Key) {
				t.Errorf("Key = %v, want %v", serialized[keyStart:keyEnd], tt.record.Key)
			}

			if !bytes.Equal(serialized[valueStart:valueEnd], tt.record.Value) {
				t.Errorf("Value = %v, want %v", serialized[valueStart:valueEnd], tt.record.Value)
			}
		})
	}
}

func TestDeserialize(t *testing.T) {
	tests := []struct {
		name        string
		data        []byte
		expected    LogRecord
		expectError bool
	}{
		{
			name: "valid record",
			data: func() []byte {
				record := LogRecord{
					CheckSum:    12345,
					SequenceNum: 67890,
					Key:         []byte("hello"),
					Value:       []byte("world123"),
				}
				record.KeySize = uint32(len(record.Key))
				record.ValueSize = uint32(len(record.Value))
				return record.Serialize()
			}(),
			expected: LogRecord{
				CheckSum:    12345,
				SequenceNum: 67890,
				KeySize:     5,
				ValueSize:   8,
				Key:         []byte("hello"),
				Value:       []byte("world123"),
			},
			expectError: false,
		},
		{
			name:        "data too short",
			data:        []byte{1, 2, 3, 4, 5}, // less than 20 bytes
			expectError: true,
		},
		{
			name:        "empty data",
			data:        []byte{},
			expectError: true,
		},
		{
			name: "empty key and value",
			data: func() []byte {
				record := LogRecord{
					CheckSum:    0,
					SequenceNum: 0,
					Key:         []byte{},
					Value:       []byte{},
				}
				record.KeySize = uint32(len(record.Key))
				record.ValueSize = uint32(len(record.Value))
				return record.Serialize()
			}(),
			expected: LogRecord{
				CheckSum:    0,
				SequenceNum: 0,
				KeySize:     0,
				ValueSize:   0,
				Key:         []byte{},
				Value:       []byte{},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Deserialize(tt.data)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if result.CheckSum != tt.expected.CheckSum {
				t.Errorf("CheckSum = %d, want %d", result.CheckSum, tt.expected.CheckSum)
			}

			if result.SequenceNum != tt.expected.SequenceNum {
				t.Errorf("SequenceNum = %d, want %d", result.SequenceNum, tt.expected.SequenceNum)
			}

			if result.KeySize != tt.expected.KeySize {
				t.Errorf("KeySize = %d, want %d", result.KeySize, tt.expected.KeySize)
			}

			if result.ValueSize != tt.expected.ValueSize {
				t.Errorf("ValueSize = %d, want %d", result.ValueSize, tt.expected.ValueSize)
			}

			if !bytes.Equal(result.Key, tt.expected.Key) {
				t.Errorf("Key = %v, want %v", result.Key, tt.expected.Key)
			}

			if !bytes.Equal(result.Value, tt.expected.Value) {
				t.Errorf("Value = %v, want %v", result.Value, tt.expected.Value)
			}
		})
	}
}

func TestSerializeDeserializeRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		record LogRecord
	}{
		{
			name: "basic record",
			record: LogRecord{
				CheckSum:    12345,
				SequenceNum: 67890,
				KeySize:     5,
				ValueSize:   8,
				Key:         []byte("hello"),
				Value:       []byte("world123"),
			},
		},
		{
			name: "empty key and value",
			record: LogRecord{
				CheckSum:    0,
				SequenceNum: 0,
				KeySize:     0,
				ValueSize:   0,
				Key:         []byte{},
				Value:       []byte{},
			},
		},
		{
			name: "large values",
			record: LogRecord{
				CheckSum:    999999,
				SequenceNum: 18446744073709551615,
				KeySize:     11,
				ValueSize:   16,
				Key:         []byte("verylongkey"),
				Value:       []byte("verylongvalue123"),
			},
		},
		{
			name: "special characters",
			record: LogRecord{
				CheckSum:    123,
				SequenceNum: 456,
				KeySize:     4,
				ValueSize:   4,
				Key:         []byte{0, 255, 128, 64},
				Value:       []byte{255, 0, 128, 64},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Serialize
			serialized := tt.record.Serialize()

			// Deserialize
			deserialized, err := Deserialize(serialized)
			if err != nil {
				t.Errorf("Deserialize failed: %v", err)
				return
			}

			// Compare original with deserialized
			if deserialized.CheckSum != tt.record.CheckSum {
				t.Errorf("CheckSum mismatch: got %d, want %d", deserialized.CheckSum, tt.record.CheckSum)
			}

			if deserialized.SequenceNum != tt.record.SequenceNum {
				t.Errorf("SequenceNum mismatch: got %d, want %d", deserialized.SequenceNum, tt.record.SequenceNum)
			}

			if deserialized.KeySize != tt.record.KeySize {
				t.Errorf("KeySize mismatch: got %d, want %d", deserialized.KeySize, tt.record.KeySize)
			}

			if deserialized.ValueSize != tt.record.ValueSize {
				t.Errorf("ValueSize mismatch: got %d, want %d", deserialized.ValueSize, tt.record.ValueSize)
			}

			if !bytes.Equal(deserialized.Key, tt.record.Key) {
				t.Errorf("Key mismatch: got %v, want %v", deserialized.Key, tt.record.Key)
			}

			if !bytes.Equal(deserialized.Value, tt.record.Value) {
				t.Errorf("Value mismatch: got %v, want %v", deserialized.Value, tt.record.Value)
			}
		})
	}
}

func TestComputeChecksum(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected uint32
	}{
		{
			name:     "empty data",
			data:     []byte{},
			expected: 0,
		},
		{
			name:     "simple string",
			data:     []byte("hello"),
			expected: 907060870, // pre-computed CRC32
		},
		{
			name:     "numbers",
			data:     []byte{1, 2, 3, 4, 5},
			expected: 1191942644, // actual computed CRC32
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ComputeChecksum(tt.data)
			if result != tt.expected {
				t.Errorf("ComputeChecksum(%v) = %d, want %d", tt.data, result, tt.expected)
			}
		})
	}
}

func TestLogRecord_ComputeChecksumForRecord(t *testing.T) {
	tests := []struct {
		name   string
		record LogRecord
	}{
		{
			name: "basic record",
			record: LogRecord{
				SequenceNum: 12345,
				KeySize:     5,
				ValueSize:   8,
				Key:         []byte("hello"),
				Value:       []byte("world123"),
			},
		},
		{
			name: "empty key and value",
			record: LogRecord{
				SequenceNum: 0,
				KeySize:     0,
				ValueSize:   0,
				Key:         []byte{},
				Value:       []byte{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checksum1 := tt.record.ComputeChecksumForRecord()
			checksum2 := tt.record.ComputeChecksumForRecord()

			// Same record should produce same checksum
			if checksum1 != checksum2 {
				t.Errorf("Checksums don't match: %d vs %d", checksum1, checksum2)
			}

			// Checksum should not be zero for non-empty data
			if len(tt.record.Key) > 0 || len(tt.record.Value) > 0 {
				if checksum1 == 0 {
					t.Errorf("Expected non-zero checksum for non-empty data")
				}
			}
		})
	}
}
