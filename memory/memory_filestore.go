package memory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"sync"

	"github.com/networkteam/filestore"
)

// Filestore is an in-memory file store for testing purposes.
type Filestore struct {
	mx    sync.RWMutex
	files map[string][]byte
}

var _ filestore.FileStore = &Filestore{}

// NewFilestore creates a new in-memory file store.
func NewFilestore() *Filestore {
	return &Filestore{
		files: make(map[string][]byte),
	}
}

// Store implements filestore.Storer.
func (f *Filestore) Store(ctx context.Context, r io.Reader) (hash string, err error) {
	f.mx.Lock()
	defer f.mx.Unlock()

	data, err := io.ReadAll(r)
	if err != nil {
		return "", err
	}

	digest := sha256.New()
	digest.Write(data)
	hashBytes := digest.Sum(nil)
	hash = hex.EncodeToString(hashBytes)

	f.files[hash] = data

	return hash, nil
}

func (f *Filestore) StoreHashed(ctx context.Context, r io.Reader, hash string) error {
	f.mx.Lock()
	defer f.mx.Unlock()

	if _, ok := f.files[hash]; ok {
		return nil
	}

	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	f.files[hash] = data

	return nil
}

func (f *Filestore) Exists(ctx context.Context, hash string) (bool, error) {
	f.mx.RLock()
	defer f.mx.RUnlock()

	_, ok := f.files[hash]
	return ok, nil
}

// Fetch implements filestore.Fetcher.
func (f *Filestore) Fetch(ctx context.Context, hash string) (io.ReadCloser, error) {
	f.mx.RLock()
	defer f.mx.RUnlock()

	data, ok := f.files[hash]
	if !ok {
		return nil, filestore.ErrNotExist
	}

	return io.NopCloser(bytes.NewReader(data)), nil
}

// Iterate implements filestore.Iterator.
func (f *Filestore) Iterate(ctx context.Context, maxBatch int, callback func(hashes []string) error) error {
	f.mx.RLock()
	defer f.mx.RUnlock()

	hashes := make([]string, 0, maxBatch)
	for hash := range f.files {
		hashes = append(hashes, hash)
		if len(hashes) == maxBatch {
			if err := callback(hashes); err != nil {
				return err
			}
			hashes = hashes[:0]
		}
	}
	if len(hashes) > 0 {
		return callback(hashes)
	}
	return nil
}

// Remove implements filestore.Remover.
func (f *Filestore) Remove(ctx context.Context, hash string) error {
	f.mx.Lock()
	defer f.mx.Unlock()

	if _, ok := f.files[hash]; !ok {
		return filestore.ErrNotExist
	}

	delete(f.files, hash)

	return nil
}

// Size implements filestore.Sizer.
func (f *Filestore) Size(ctx context.Context, hash string) (int64, error) {
	f.mx.RLock()
	defer f.mx.RUnlock()

	data, ok := f.files[hash]
	if !ok {
		return 0, filestore.ErrNotExist
	}

	return int64(len(data)), nil
}

// ImgproxyURLSource returns a dummy URL to the hash in memory. It should only be used for testing purposes.
func (f *Filestore) ImgproxyURLSource(hash string) (string, error) {
	return "memory://" + hash, nil
}
