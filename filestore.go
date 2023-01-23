package filestore

import (
	"errors"
	"io"
)

// A Storer stores the content of the given reader (e.g. a file) and returns a consistent hash for later retrieval.
type Storer interface {
	Store(r io.Reader) (hash string, err error)
}

// A Fetcher fetches the content of the given hash in the form of an io.Reader.
type Fetcher interface {
	Fetch(hash string) (io.ReadCloser, error)
}

// An Iterator iterates over all stored files and returns their hashes in batches.
type Iterator interface {
	// Iterate calls callback with a maxBatch number of asset hashes.
	// If callback returns an error, iteration stops and the error is returned.
	Iterate(maxBatch int, callback func(hashes []string) error) error
}

// A Remover can remove a file with the given hash.
type Remover interface {
	Remove(hash string) error
}

// A Sizer can return the size of a file with the given hash.
type Sizer interface {
	Size(hash string) (int64, error)
}

// An ImgproxyURLSourcer can return the source URL to original file for imgproxy.
type ImgproxyURLSourcer interface {
	// ImgproxyURLSource gets the source URL to original file (e.g. for use with imgproxy).
	ImgproxyURLSource(hash string) (string, error)
}

// ErrNotExist is returned when a stored file does not exist.
var ErrNotExist = errors.New("file does not exist")
