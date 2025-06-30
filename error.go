package main

import "errors"

var ErrBadChecksum = errors.New("checksum mismatch")

// ErrCheckpointNeeded is a special error returned by Log.Append to signal
// that the log has reached its size threshold and a checkpoint should be run.
var ErrCheckpointNeeded = errors.New("checkpoint needed")

var ErrCheckpointCorrupted = errors.New("checkpoint corrupted")
