package local

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/hashicorp/go-multierror"

	"github.com/networkteam/filestore"
)

const (
	// DefaultPrefixSize is the default path prefix size.
	DefaultPrefixSize = 2
	// DefaultTargetFileMode is the default file mode when storing assets.
	DefaultTargetFileMode = 0644
)

// Filestore is a file store that stores files on a local filesystem.
type Filestore struct {
	tmpPath    string
	assetsPath string

	TargetFileMode os.FileMode
	PrefixSize     int
}

// NewFilestore creates a new file store operating on a (local) filesystem.
//
// The assetsPath is the path to a directory where the assets will be stored.
// The tmpPath is the path to a directory where temporary files will be stored.
// It should be on the same filesystem as assetsPath to support atomic renames.
func NewFilestore(tmpPath, assetsPath string) (*Filestore, error) {
	// Create tmp folder if it does not exist
	if err := os.MkdirAll(tmpPath, 0755); err != nil {
		return nil, fmt.Errorf("creating tmp folder: %w", err)
	}

	// Create assets folder if it does not exist
	if err := os.MkdirAll(assetsPath, 0755); err != nil {
		return nil, fmt.Errorf("creating assets folder: %w", err)
	}

	return &Filestore{
		tmpPath:        tmpPath,
		assetsPath:     assetsPath,
		TargetFileMode: DefaultTargetFileMode,
		PrefixSize:     DefaultPrefixSize,
	}, nil
}

var (
	_ filestore.Storer             = &Filestore{}
	_ filestore.Fetcher            = &Filestore{}
	_ filestore.Iterator           = &Filestore{}
	_ filestore.Remover            = &Filestore{}
	_ filestore.Sizer              = &Filestore{}
	_ filestore.ImgproxyURLSourcer = &Filestore{}
)

// Store stores the content of the reader in a local file.
// The content is first stored in a temporary file to compute a consistent hash (SHA256)
// and then the file is renamed to the hash in the assets path.
func (f *Filestore) Store(ctx context.Context, r io.Reader) (hash string, err error) {
	var (
		tempFile      *os.File
		tmpWasRenamed bool
		tmpWasClosed  bool
	)

	// Create temporary file to store uploaded file, will be renamed with hash later
	tempFile, err = os.CreateTemp(f.tmpPath, "image-upload-*")
	if err != nil {
		return "", fmt.Errorf("creating temp file: %w", err)
	}

	defer func() {
		if !tmpWasClosed {
			if closeErr := tempFile.Close(); closeErr != nil {
				err = multierror.Append(
					err,
					fmt.Errorf("closing temporary file (with previous error): %w", closeErr),
				)
				return
			}
		}
		if !tmpWasRenamed {
			if removeErr := os.Remove(tempFile.Name()); removeErr != nil {
				err = multierror.Append(
					err,
					fmt.Errorf("removing temporary file (with previous error): %w", removeErr),
				)
				return
			}
		}
	}()

	// Read from given file and write to temp file while simultaneously writing into a SHA256 digest to calculate the hash on the fly
	tmpReader := io.TeeReader(r, tempFile)

	digest := sha256.New()

	if _, err = io.Copy(digest, tmpReader); err != nil {
		return "", fmt.Errorf("copying reader: %w", err)
	}

	var hashBytes []byte
	hashBytes = digest.Sum(hashBytes)
	hashHex := hex.EncodeToString(hashBytes)

	pathPrefix, err := f.prefixPath(hashHex)
	if err != nil {
		return "", err
	}

	if err = tempFile.Close(); err != nil {
		return "", fmt.Errorf("closing temp file: %w", err)
	}
	tmpWasClosed = true

	targetPath := fmt.Sprintf("%s/%s/%s", f.assetsPath, pathPrefix, hashHex)
	// Check if target path exists
	if _, err = os.Stat(targetPath); err == nil {
		return hashHex, nil
	}

	if err = os.MkdirAll(fmt.Sprintf("%s/%s", f.assetsPath, pathPrefix), 0755); err != nil {
		return "", fmt.Errorf("creating asset subdirectory: %w", err)
	}

	if err = os.Rename(tempFile.Name(), targetPath); err != nil {
		return "", fmt.Errorf("renaming temp file: %w", err)
	}

	tmpWasRenamed = true
	err = os.Chmod(targetPath, f.TargetFileMode)
	if err != nil {
		return "", fmt.Errorf("setting file mode: %w", err)
	}

	return hashHex, nil
}

// Fetch returns a reader to the file with the given hash.
// If the file does not exist, ErrNotExist is returned.
func (f *Filestore) Fetch(ctx context.Context, hash string) (io.ReadCloser, error) {
	prefixPath, err := f.prefixPath(hash)
	if err != nil {
		return nil, err
	}

	path := fmt.Sprintf("%s/%s/%s", f.assetsPath, prefixPath, hash)
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, filestore.ErrNotExist
		}
		return nil, fmt.Errorf("opening file: %w", err)
	}
	return file, nil
}

var errInvalidHash = errors.New("invalid hash")

// ImgproxyURLSource gets a source URL to a local file for imgproxy.
func (f *Filestore) ImgproxyURLSource(hash string) (string, error) {
	prefixPath, err := f.prefixPath(hash)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("local:///%s/%s", prefixPath, hash), nil
}

// Iterate over all files in the store with a batch size of maxBatch.
func (f *Filestore) Iterate(ctx context.Context, maxBatch int, callback func(hashes []string) error) error {
	hashes := make([]string, 0, maxBatch)
	err := filepath.Walk(f.assetsPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || info.Name()[0] == '.' {
			return nil
		}

		hashes = append(hashes, info.Name())

		// If we have enough hashes, invoke the callback
		if len(hashes) == maxBatch {
			if err := callback(hashes); err != nil {
				return err
			}
			// Reset slice
			hashes = hashes[:0]
		}

		return nil
	})
	if err != nil {
		return err
	}

	// Invoke callback with remaining hashes
	if len(hashes) > 0 {
		return callback(hashes)
	}
	return nil
}

// Remove a file from the store with the given hash.
func (f *Filestore) Remove(ctx context.Context, hash string) error {
	prefixPath, err := f.prefixPath(hash)
	if err != nil {
		return err
	}

	dirName := fmt.Sprintf("%s/%s", f.assetsPath, prefixPath)
	fileName := fmt.Sprintf("%s/%s", dirName, hash)
	err = os.Remove(fileName)
	if err != nil {
		return fmt.Errorf("removing file %q: %w", fileName, err)
	}

	// Check if directory for prefix is empty
	dir, err := os.Open(dirName)
	if err != nil {
		return fmt.Errorf("opening directory %s: %w", dirName, err)
	}
	defer dir.Close()

	_, err = dir.Readdirnames(1)
	if err != nil {
		// io.EOF means the directory is empty
		if errors.Is(err, io.EOF) {
			err = os.Remove(dirName)
			if err != nil {
				return fmt.Errorf("removing empty directory %s: %w", dirName, err)
			}
			return nil
		}
		return fmt.Errorf("reading directory %s: %w", dirName, err)
	}

	return nil
}

// Size returns the size of the file with the given hash.
func (f *Filestore) Size(ctx context.Context, hash string) (int64, error) {
	prefixPath, err := f.prefixPath(hash)
	if err != nil {
		return 0, err
	}

	path := fmt.Sprintf("%s/%s/%s", f.assetsPath, prefixPath, hash)
	stat, err := os.Stat(path)
	if err != nil {
		return 0, err
	}

	return stat.Size(), nil
}

func (f *Filestore) prefixPath(hash string) (string, error) {
	if len(hash) < f.PrefixSize {
		return "", errInvalidHash
	}
	return hash[0:f.PrefixSize], nil
}
