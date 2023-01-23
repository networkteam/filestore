package filestore

import (
	"errors"
	"io"
)

type Storer interface {
	Store(r io.Reader) (hash string, err error)
}

type Fetcher interface {
	Fetch(hash string) (io.ReadCloser, error)
}

type Iterator interface {
	// Iterate calls callback with a maxBatch number of asset hashes.
	// If callback returns an error, iteration stops and the error is returned.
	Iterate(maxBatch int, callback func(hashes []string) error) error
}

type Remover interface {
	Remove(hash string) error
}

type Sizer interface {
	Size(hash string) (int64, error)
}

type ImgproxyURLSourcer interface {
	// ImgproxyURLSource gets the source URL to original file (e.g. for use with imgproxy).
	ImgproxyURLSource(hash string) (string, error)
}

var ErrNotExist = errors.New("file does not exist")
