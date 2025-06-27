package main

import (
	"os"
)

// NewLog opens/creates the log file and initializes the Log object.
func NewLog(path string) (*Log, error) {
	// 1. Open the file...
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	// 2. Create the initial Log object.
	log := &Log{
		file: file,
		path: path,
		// lastSequenceNum starts at 0
	}

	// 3. Scan the file to populate lastSequenceNum.
	// This is a new, private method we would write.
	if err := log.recover(); err != nil {
		// We might want to close the file here before returning
		file.Close()
		return nil, err
	}

	// 4. Return the fully initialized, ready-to-use Log object.
	return log, nil
}
