package filestore

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
)

// S3 is a file store that stores files in a S3 compatible object storage (e.g. AWS S3 or MinIO).
type S3 struct {
	Client     *minio.Client
	URL        string
	BucketName string
}

// SizedReader is a reader that also returns the size of the data.
type SizedReader interface {
	io.Reader
	// Size of the data that can be read.
	Size() int64
}

// ContentTypeReader is a reader that also returns the content type of the data.
type ContentTypeReader interface {
	io.Reader
	// ContentType (media type) of the data.
	ContentType() string
}

// ContentDispositionReader is a reader that also returns the content disposition of the data.
type ContentDispositionReader interface {
	io.Reader
	// ContentDisposition of the data (e.g. "inline; filename=\"test.png\"").
	ContentDisposition() string
}

var (
	_ Storer             = &S3{}
	_ Fetcher            = &S3{}
	_ Iterator           = &S3{}
	_ Remover            = &S3{}
	_ Sizer              = &S3{}
	_ ImgproxyURLSourcer = &S3{}
)

type s3Options struct {
	credentials     *credentials.Credentials
	secure          bool
	region          string
	bucketLookup    minio.BucketLookupType
	trailingHeaders bool
	transport       http.RoundTripper
}

// S3Option is a functional option for creating a S3 file store.
type S3Option func(*s3Options)

// WithS3Secure sets the secure flag for the S3 client (i.e. use HTTPS for the endpoint).
func WithS3Secure() S3Option {
	return func(opts *s3Options) {
		opts.secure = true
	}
}

// WithS3CredentialsV2 sets the credentials for the S3 client using V2 signatures.
// The token can be left empty.
func WithS3CredentialsV2(accessKey, secretKey, token string) S3Option {
	return func(opts *s3Options) {
		opts.credentials = credentials.NewStaticV2(accessKey, secretKey, token)
	}
}

// WithS3CredentialsV4 sets the credentials for the S3 client using V4 signatures.
// The V4 signature should be used with MinIO.
// The token can be left empty.
func WithS3CredentialsV4(accessKey, secretKey, token string) S3Option {
	return func(opts *s3Options) {
		opts.credentials = credentials.NewStaticV4(accessKey, secretKey, token)
	}
}

// WithS3Region sets the region for the S3 client.
// The region can be left empty for MinIO.
func WithS3Region(region string) S3Option {
	return func(opts *s3Options) {
		opts.region = region
	}
}

// WithS3BucketLookupPath sets the bucket lookup to path style.
// If not set, the bucket lookup is set to auto.
func WithS3BucketLookupPath() S3Option {
	return func(opts *s3Options) {
		opts.bucketLookup = minio.BucketLookupPath
	}
}

// WithS3BucketLookupDNS sets the bucket lookup to DNS style.
func WithS3BucketLookupDNS() S3Option {
	return func(opts *s3Options) {
		opts.bucketLookup = minio.BucketLookupDNS
	}
}

// WithS3TrailingHeaders sets the trailing headers flag for the S3 client.
func WithS3TrailingHeaders() S3Option {
	return func(opts *s3Options) {
		opts.trailingHeaders = true
	}
}

// WithS3Transport sets a custom HTTP transport for testing or special needs.
func WithS3Transport(transport http.RoundTripper) S3Option {
	return func(opts *s3Options) {
		opts.transport = transport
	}
}

// NewS3 creates a new S3 file store.
func NewS3(ctx context.Context, endpoint, bucketName string, opts ...S3Option) (*S3, error) {
	s3Options := &s3Options{}
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
		return nil, fmt.Errorf("checking if bucket %q exists: %w", bucketName, err)
	}

	if !bucketExists {
		err = client.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
		if err != nil {
			return nil, fmt.Errorf("creating bucket %q: %w", bucketName, err)
		}
	}

	return &S3{
		Client:     client,
		URL:        endpoint,
		BucketName: bucketName,
	}, nil
}

// Fetch gets an object from the S3 bucket by hash and returns a reader for the object.
// It will stat the object to check for existence. If the object does not exist, it will return ErrNotExist.
func (s S3) Fetch(ctx context.Context, hash string) (io.ReadCloser, error) {
	readCloser, err := s.Client.GetObject(ctx, s.BucketName, hash, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("getting object %q: %w", hash, err)
	}

	// We have to stat the object to check for an error if the hash does not exist
	_, err = readCloser.Stat()
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return nil, ErrNotExist
		}
		return nil, fmt.Errorf("getting object info %q: %w", hash, err)
	}

	return readCloser, nil
}

// ImgproxyURLSource implements the ImgproxyURLSourcer interface.
// It returns a URL to the object that will be understood by imgproxy in the form of "s3://bucket-name/object-key".
func (s S3) ImgproxyURLSource(hash string) (string, error) {
	return fmt.Sprintf("s3://%s/%s", s.BucketName, hash), nil
}

// Iterate iterates over all objects in the S3 bucket and calls the callback with a maxBatch amount of hashes.
// Iteration will stop if the callback returns an error.
func (s S3) Iterate(ctx context.Context, maxBatch int, callback func(hashes []string) error) error {
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
func (s S3) Remove(ctx context.Context, hash string) error {
	err := s.Client.RemoveObject(ctx, s.BucketName, hash, minio.RemoveObjectOptions{})
	if err != nil {
		return fmt.Errorf("removing object %q: %w", hash, err)
	}
	return nil
}

// Size returns the size of an object in the S3 bucket by hash.
func (s S3) Size(ctx context.Context, hash string) (int64, error) {
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
// The reader should implement SizedReader for better performance (the client can optimize the operation given the size and reduce memory usage).
// The reader can implement ContentTypeReader or ContentDispositionReader to set the content type or content disposition of the object.
func (s S3) Store(ctx context.Context, r io.Reader) (string, error) {
	var size int64 = -1
	if sizedReader, ok := r.(SizedReader); ok {
		size = sizedReader.Size()
	}

	var contentType, contentDisposition string
	if typedReader, ok := r.(ContentTypeReader); ok {
		contentType = typedReader.ContentType()
	}
	if dispoReader, ok := r.(ContentDispositionReader); ok {
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
		return "", fmt.Errorf("putting temp object: %w", err)
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
		return "", fmt.Errorf("copying temp object: %w", err)
	}

	err = s.Client.RemoveObject(ctx, s.BucketName, tmpObjectName, minio.RemoveObjectOptions{})
	if err != nil {
		return "", fmt.Errorf("removing temp object: %w", err)
	}

	return hashHex, nil
}
