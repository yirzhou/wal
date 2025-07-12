package db

import (
	"io/ioutil"
	"log"
)

type Configuration struct {
	CheckpointSize             int64
	BaseDir                    string
	SegmentFileSizeThresholdLX int64
	CompactionIntervalMs       uint64
}

// NewDefaultConfiguration returns the default DB configuration.
func NewDefaultConfiguration() *Configuration {
	return &Configuration{
		CheckpointSize:             1024, // 1KiB
		BaseDir:                    "./db",
		SegmentFileSizeThresholdLX: 1024,
		CompactionIntervalMs:       20, // 20ms
	}
}

func (c *Configuration) WithNoLog() *Configuration {
	log.SetOutput(ioutil.Discard) // Suppress log output
	return c
}

// WithBaseDir sets the base directory for the DB.
func (c *Configuration) WithBaseDir(dir string) *Configuration {
	c.BaseDir = dir
	return c
}

// WithCompactionIntervalMs sets the compaction interval for the DB.
func (c *Configuration) WithCompactionIntervalMs(intervalMs uint64) *Configuration {
	c.CompactionIntervalMs = intervalMs
	return c
}

// GetCompactionIntervalMs returns the compaction interval for the DB.
func (c *Configuration) GetCompactionIntervalMs() uint64 {
	return c.CompactionIntervalMs
}

// WithSegmentFileSizeThresholdLX sets the segment file size threshold for the DB.
func (c *Configuration) WithSegmentFileSizeThresholdLX(size int64) *Configuration {
	c.SegmentFileSizeThresholdLX = size
	return c
}

// WithCheckpointSize sets the checkpoint size for the DB.
func (c *Configuration) WithCheckpointSize(size int64) *Configuration {
	c.CheckpointSize = size
	return c
}

// GetCheckpointSize returns the checkpoint size for the DB.
func (c *Configuration) GetCheckpointSize() int64 {
	return c.CheckpointSize
}

// GetBaseDir returns the base directory for the DB.
func (c *Configuration) GetBaseDir() string {
	return c.BaseDir
}

// GetSegmentFileSizeThresholdLX returns the segment file size threshold for the DB.
func (c *Configuration) GetSegmentFileSizeThresholdLX() int64 {
	return c.SegmentFileSizeThresholdLX
}
