package main

import (
	"io"
	"log"
	"os"
)

// NewLog opens/creates the log file and initializes the Log object.
func NewLog(path string) (*WAL, *MemState, error) {
	// 1. Open the file...
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Println("Error opening file:", err)
		return nil, nil, err
	}

	// Seek to the beginning of the file.
	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		log.Println("Error seeking to the beginning of the file:", err)
		return nil, nil, err
	}

	// 2. Create the initial Log object.
	wal := &WAL{
		file: file,
		path: path,
		// lastSequenceNum starts at 0
	}

	reader := wal.file
	var goodOffset int64 = 0
	memState := NewMemState()
	// var lastEntrySize int64 = 0
	// Scan the file to populate lastSequenceNum.
	for {
		offset, err := reader.Seek(0, io.SeekCurrent)
		if err != nil {
			log.Println("Error seeking:", err)
			return nil, nil, err
		}

		record, err := recoverNextRecord(reader)
		// Check the current offset of the reader.
		if err != nil {
			// If we get an End-Of-File error, it's a clean stop.
			// This is the expected way to finish recovery.
			if err == io.EOF {
				log.Println("Completed recovery of WAL..")
				goodOffset = offset
				break
			}
			// If we get a bad checksum, it means the last write was torn.
			// We stop here and trust the log up to this point.
			if err == ErrBadChecksum || err == io.ErrUnexpectedEOF {
				log.Println("Bad checksum or unexpected EOF", err.Error())
				goodOffset = offset
				break
			}
			// Any other error is unexpected, such as unexpected EOF.
			log.Println("Error recovering next record:", err)
			return nil, nil, err
		}

		wal.lastSequenceNum = record.SequenceNum
		memState.Put(record.Key, record.Value)
	}

	// Truncate the file -- doesn't change the offset and only changes the file size.
	log.Println("Truncating to", goodOffset)
	err = file.Truncate(goodOffset)
	if err != nil {
		log.Println("Error truncating file:", err)
		return nil, nil, err
	}

	// 3. Move to the good offset for future appends.
	_, err = file.Seek(goodOffset, io.SeekStart)
	if err != nil {
		log.Println("Error seeking to good offset::", err)
		return nil, nil, err
	}

	// 4. Return the fully initialized, ready-to-use Log object.
	return wal, memState, nil
}

func main() {
	wal, memState, err := NewLog("wal.log")
	if err != nil {
		log.Println("Error creating WAL:", err)
		return
	}
	log.Println("lastSequenceNum:", wal.lastSequenceNum)
	defer wal.Close()

	memState.Print()
	// wal.Append([]byte(""), []byte("value2"))
	// wal.Append([]byte("color"), []byte(""))
	// wal.Append([]byte("key-1"), []byte("some utf-8 chars ✨ or binary data \x00\x01\x02"))
}
