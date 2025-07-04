package db

type Configuration struct {
	CheckpointSize int64
	BaseDir        string
}

// NewDefaultConfiguration returns the default DB configuration.
func NewDefaultConfiguration() *Configuration {
	return &Configuration{
		CheckpointSize: 1024, // 1KiB
		BaseDir:        "./db",
	}
}

// WithBaseDir sets the base directory for the DB.
func (c *Configuration) WithBaseDir(dir string) *Configuration {
	c.BaseDir = dir
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
