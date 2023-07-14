package s3

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/gofrs/uuid"
	"github.com/minio/minio-go/v7"

	"github.com/networkteam/filestore"
)

// Filestore is a file store that stores files in a S3 compatible object storage (e.g. AWS S3 or MinIO).
type Filestore struct {
	Client     *minio.Client
	URL        string
	BucketName string
}

var _ filestore.FileStore = &Filestore{}

// NewFilestore creates a new S3 file store.
func NewFilestore(ctx context.Context, endpoint, bucketName string, opts ...Option) (*Filestore, error) {
	s3Options := &options{}
	for _, opt := range opts {
		opt(s3Options)
	}

	client, err := minio.New(endpoint, &minio.Options{
		Creds:           s3Options.credentials,
		Secure:          s3Options.secure,
		Region:          s3Options.region,
		BucketLookup:    s3Options.bucketLookup,
		TrailingHeaders: s3Options.trailingHeaders,

		Transport: s3Options.transport,
	})
	if err != nil {
		return nil, fmt.Errorf("creating MinIO client: %w", err)
	}

	bucketExists, err := client.BucketExists(ctx, bucketName)
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchBucket" {
			return nil, nil
		}
		return nil, fmt.Errorf("checking if bucket %q exists: %w", bucketName, err)
	}

	if !bucketExists && !s3Options.bucketAutoCreate {
		return nil, nil
	}

	if !bucketExists {
		err = client.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
		if err != nil {
			return nil, fmt.Errorf("creating bucket %q: %w", bucketName, err)
		}
	}

	return &Filestore{
		Client:     client,
		URL:        endpoint,
		BucketName: bucketName,
	}, nil
}

// Fetch gets an object from the S3 bucket by hash and returns a reader for the object.
// It will stat the object to check for existence. If the object does not exist, it will return ErrNotExist.
func (s Filestore) Fetch(ctx context.Context, hash string) (io.ReadCloser, error) {
	readCloser, err := s.Client.GetObject(ctx, s.BucketName, hash, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("getting object %q: %w", hash, err)
	}

	// We have to stat the object to check for an error if the hash does not exist
	_, err = readCloser.Stat()
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return nil, filestore.ErrNotExist
		}
		return nil, fmt.Errorf("getting object info %q: %w", hash, err)
	}

	return readCloser, nil
}

// ImgproxyURLSource implements the ImgproxyURLSourcer interface.
// It returns a URL to the object that will be understood by imgproxy in the form of "s3://bucket-name/object-key".
func (s Filestore) ImgproxyURLSource(hash string) (string, error) {
	return fmt.Sprintf("s3://%s/%s", s.BucketName, hash), nil
}

// Iterate iterates over all objects in the S3 bucket and calls the callback with a maxBatch amount of hashes.
// Iteration will stop if the callback returns an error.
func (s Filestore) Iterate(ctx context.Context, maxBatch int, callback func(hashes []string) error) error {
	objInfos := s.Client.ListObjects(ctx, s.BucketName, minio.ListObjectsOptions{})

	hashes := make([]string, 0, maxBatch)

	for objInfo := range objInfos {
		if objInfo.Err != nil {
			return fmt.Errorf("listing objects: %w", objInfo.Err)
		}

		hashes = append(hashes, objInfo.Key)
		if len(hashes) == maxBatch {
			err := callback(hashes)
			if err != nil {
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

// Remove removes an object from the S3 bucket by hash.
// It is not guaranteed to error if the hash does not exist.
func (s Filestore) Remove(ctx context.Context, hash string) error {
	err := s.Client.RemoveObject(ctx, s.BucketName, hash, minio.RemoveObjectOptions{})
	if err != nil {
		return fmt.Errorf("removing object %q: %w", hash, err)
	}
	return nil
}

// Size returns the size of an object in the S3 bucket by hash.
func (s Filestore) Size(ctx context.Context, hash string) (int64, error) {
	object, err := s.Client.GetObject(ctx, s.BucketName, hash, minio.GetObjectOptions{})
	if err != nil {
		return 0, fmt.Errorf("getting object %q: %w", hash, err)
	}

	stat, err := object.Stat()
	if err != nil {
		return 0, fmt.Errorf("statting object %q: %w", hash, err)
	}

	return stat.Size, nil
}

// Store stores an object in the S3 bucket by hash.
// The reader should implement Sized for better performance (the client can optimize the operation given the size and reduce memory usage).
// The reader can implement ContentTyped or ContentDispositioned to set the content type or content disposition of the object.
func (s Filestore) Store(ctx context.Context, r io.Reader) (string, error) {
	var size int64 = -1
	if sizedReader, ok := r.(Sized); ok {
		size = sizedReader.Size()
	}

	var contentType, contentDisposition string
	if typedReader, ok := r.(ContentTyped); ok {
		contentType = typedReader.ContentType()
	}
	if dispoReader, ok := r.(ContentDispositioned); ok {
		contentDisposition = dispoReader.ContentDisposition()
	}

	digest := sha256.New()
	hashedReader := io.TeeReader(r, digest)

	tmpID, err := uuid.NewV4()
	if err != nil {
		return "", fmt.Errorf("generating temp id: %w", err)
	}
	tmpObjectName := fmt.Sprintf("tmp/%s", tmpID)

	_, err = s.Client.PutObject(ctx, s.BucketName, tmpObjectName, hashedReader, size, minio.PutObjectOptions{
		ContentType:        contentType,
		ContentDisposition: contentDisposition,
	})
	if err != nil {
		return "", fmt.Errorf("putting temp object %q: %w", tmpObjectName, err)
	}

	hashBytes := digest.Sum(nil)
	hashHex := hex.EncodeToString(hashBytes)

	_, err = s.Client.CopyObject(ctx, minio.CopyDestOptions{
		Bucket: s.BucketName,
		Object: hashHex,
	}, minio.CopySrcOptions{
		Bucket: s.BucketName,
		Object: tmpObjectName,
	})
	if err != nil {
		return "", fmt.Errorf("copying temp object %q: %w", tmpObjectName, err)
	}

	err = s.Client.RemoveObject(ctx, s.BucketName, tmpObjectName, minio.RemoveObjectOptions{})
	if err != nil {
		return "", fmt.Errorf("removing temp object: %w", err)
	}

	return hashHex, nil
}
