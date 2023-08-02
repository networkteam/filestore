package filestore

import (
	"context"
	"errors"
	"io"
)

// A Storer stores the content of the given reader (e.g. a file) and returns a consistent hash for later retrieval.
type Storer interface {
	Store(ctx context.Context, r io.Reader) (hash string, err error)
}

// A HashedStorer stores the content of the given reader (e.g. a file) with a pre-calculated hash.
// The hash can be chosen freely and is not checked against the reader content.
type HashedStorer interface {
	StoreHashed(ctx context.Context, r io.Reader, hash string) error
}

// A Fetcher fetches the content of the given hash in the form of an io.Reader.
type Fetcher interface {
	Fetch(ctx context.Context, hash string) (io.ReadCloser, error)
}

// An Exister checks if the given hash exists in the store.
type Exister interface {
	Exists(ctx context.Context, hash string) (bool, error)
}

// An Iterator iterates over all stored files and returns their hashes in batches.
type Iterator interface {
	// Iterate calls callback with a maxBatch number of asset hashes.
	// If callback returns an error, iteration stops and the error is returned.
	Iterate(ctx context.Context, maxBatch int, callback func(hashes []string) error) error
}

// A Remover can remove a file with the given hash.
type Remover interface {
	Remove(ctx context.Context, hash string) error
}

// A Sizer can return the size of a file with the given hash.
type Sizer interface {
	Size(ctx context.Context, hash string) (int64, error)
}

// An ImgproxyURLSourcer can return the source URL to original file for imgproxy.
type ImgproxyURLSourcer interface {
	// ImgproxyURLSource gets the source URL to original file (e.g. for use with imgproxy).
	ImgproxyURLSource(hash string) (string, error)
}

// A FileStore bundles all the interfaces above.
type FileStore interface {
	Storer
	HashedStorer
	Exister
	Fetcher
	Iterator
	Remover
	Sizer
	ImgproxyURLSourcer
}

// ErrNotExist is returned when a stored file does not exist.
var ErrNotExist = errors.New("file does not exist")
