package s3

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"

	"github.com/gofrs/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/networkteam/filestore"
)

// Filestore is a file store that stores files in a S3 compatible object storage (e.g. AWS S3 or MinIO).
type Filestore struct {
	Client     *minio.Client
	URL        string
	BucketName string
}

var (
	_ filestore.Storer             = &Filestore{}
	_ filestore.Fetcher            = &Filestore{}
	_ filestore.Iterator           = &Filestore{}
	_ filestore.Remover            = &Filestore{}
	_ filestore.Sizer              = &Filestore{}
	_ filestore.ImgproxyURLSourcer = &Filestore{}
)

type options struct {
	credentials     *credentials.Credentials
	secure          bool
	region          string
	bucketLookup    minio.BucketLookupType
	trailingHeaders bool
	transport       http.RoundTripper
}

// Option is a functional option for creating a S3 file store.
type Option func(*options)

// WithSecure sets the secure flag for the S3 client (i.e. use HTTPS for the endpoint).
func WithSecure() Option {
	return func(opts *options) {
		opts.secure = true
	}
}

// WithCredentialsV2 sets the credentials for the S3 client using V2 signatures.
// The token can be left empty.
func WithCredentialsV2(accessKey, secretKey, token string) Option {
	return func(opts *options) {
		opts.credentials = credentials.NewStaticV2(accessKey, secretKey, token)
	}
}

// WithCredentialsV4 sets the credentials for the S3 client using V4 signatures.
// The V4 signature should be used with MinIO.
// The token can be left empty.
func WithCredentialsV4(accessKey, secretKey, token string) Option {
	return func(opts *options) {
		opts.credentials = credentials.NewStaticV4(accessKey, secretKey, token)
	}
}

// WithRegion sets the region for the S3 client.
// The region can be left empty for MinIO.
func WithRegion(region string) Option {
	return func(opts *options) {
		opts.region = region
	}
}

// WithBucketLookupPath sets the bucket lookup to path style.
// If not set, the bucket lookup is set to auto.
func WithBucketLookupPath() Option {
	return func(opts *options) {
		opts.bucketLookup = minio.BucketLookupPath
	}
}

// WithBucketLookupDNS sets the bucket lookup to DNS style.
func WithBucketLookupDNS() Option {
	return func(opts *options) {
		opts.bucketLookup = minio.BucketLookupDNS
	}
}

// WithTrailingHeaders sets the trailing headers flag for the S3 client.
func WithTrailingHeaders() Option {
	return func(opts *options) {
		opts.trailingHeaders = true
	}
}

// WithTransport sets a custom HTTP transport for testing or special needs.
func WithTransport(transport http.RoundTripper) Option {
	return func(opts *options) {
		opts.transport = transport
	}
}

// NewFilestore creates a new S3 file store.
func NewFilestore(ctx context.Context, endpoint, bucketName string, autoCreateBucket bool, opts ...Option) (*Filestore, error) {
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

	if !bucketExists && !autoCreateBucket {
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

// Sized is a reader that can return the size of the data.
type Sized interface {
	// Size of the data that can be read.
	Size() int64
}

// ContentTyped is a reader that also returns the content type of the data.
type ContentTyped interface {
	// ContentType (media type) of the data.
	ContentType() string
}

// ContentDispositioned is a reader that also returns the content disposition of the data.
type ContentDispositioned interface {
	// ContentDisposition of the data (e.g. "inline; filename=\"test.png\"").
	ContentDisposition() string
}

// SizedReader wraps a reader and its size of the data to implement Sized.
func SizedReader(r io.Reader, size int64) io.Reader {
	return &sizedReader{r, size}
}

type sizedReader struct {
	io.Reader
	size int64
}

func (s *sizedReader) Size() int64 {
	return s.size
}

var _ Sized = &sizedReader{}

// ContentTypedReader wraps a reader and its content type to implement ContentTyped.
func ContentTypedReader(r io.Reader, contentType string) io.Reader {
	return &contentTypedReader{r, contentType}
}

type contentTypedReader struct {
	io.Reader
	contentType string
}

func (s *contentTypedReader) ContentType() string {
	return s.contentType
}

var _ ContentTyped = &contentTypedReader{}

// ContentDispositionedReader wraps a reader and its content disposition to implement ContentDispositioned.
func ContentDispositionedReader(r io.Reader, contentDisposition string) io.Reader {
	return &contentDispositionedReader{r, contentDisposition}
}

type contentDispositionedReader struct {
	io.Reader
	contentDisposition string
}

func (s *contentDispositionedReader) ContentDisposition() string {
	return s.contentDisposition
}

var _ ContentDispositioned = &contentDispositionedReader{}
