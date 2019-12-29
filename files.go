package skipdisk

import (
	"io"
	"os"
)

var (
	// Make sure that the os.File struct implements the writer and reader at interfaces.
	_ ReaderWriterAt = &os.File{}

	// Make sure that the os.File struct implements the sync interface.
	_ CanSync = &os.File{}

	// Make sure that the os.File struct implements the SyncableIO interface.
	_ SyncableIO = &os.File{}
)

type (
	// fileType is a simple 1-Byte value that prefixes all of the file names to indicate the type of
	// file that is being read/written.
	fileType byte

	// ReaderWriterAt is used as the interface for reading and writing data for the database. It can
	// be used in nearly every IO portion of the database.
	ReaderWriterAt interface {
		io.ReaderAt
		io.WriterAt
	}

	// CanSync is used to check if the current IO interface that a file wrapper is using has a
	// method that allows its changes to be flushed to the disk.
	CanSync interface {
		Sync() error
	}

	SyncableIO interface {
		CanSync
		ReaderWriterAt
	}
)

// getPathExists will return true or false indicating whether or not the path specified (file or
// folder) is valid.
func getPathExists(path string) bool {
	// We can do this by getting the stat for the path specified. If we get a NotExist error then we
	// know that the path is not valid.
	_, err := os.Stat(path)

	// Return the inverted value of IsNotExists.
	return !os.IsNotExist(err)
}

// newDirectory will create a new directory at the path specified, including any missing directories
// in the provided path. The directory will be owned by the current user. If the directory already
// exists then nothing will change.
func newDirectory(path string) error {
	if err := createDirectory(path); err == nil {
		return takeOwnership(path)
	} else {
		return err
	}
}

// createDirectory will create a directory at the path specified. If the path contains multiple
// directories that do not exist, all of them will be created.
func createDirectory(path string) error {
	return os.MkdirAll(path, os.ModeDir)
}

// takeOwnership will change the owner of the path specified to be such that the DB has ownership.
func takeOwnership(path string) error {
	return os.Chown(path, os.Getuid(), os.Getgid())
}
