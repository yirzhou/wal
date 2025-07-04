package lib

import "errors"

// ErrBadChecksum is returned when the checksum of a record does not match the computed checksum.
var ErrBadChecksum = errors.New("checksum mismatch")

// ErrCheckpointNeeded is a special error returned by Log.Append to signal
// that the log has reached its size threshold and a checkpoint should be run.
var ErrCheckpointNeeded = errors.New("checkpoint needed")

// ErrCheckpointCorrupted is returned when the checkpoint file is corrupted (e.g., edited in the middle, or bit-flipped).
var ErrCheckpointCorrupted = errors.New("checkpoint corrupted")

// ErrSparseIndexCorrupted is returned when the sparse index file is corrupted (e.g., edited in the middle, or bit-flipped).
var ErrSparseIndexCorrupted = errors.New("sparse index corrupted")
