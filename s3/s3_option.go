package s3

import (
	"net/http"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type options struct {
	credentials      *credentials.Credentials
	secure           bool
	region           string
	bucketLookup     minio.BucketLookupType
	trailingHeaders  bool
	transport        http.RoundTripper
	bucketAutoCreate bool
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

// WithBucketAutoCreate sets the automatic creation of the bucket if it doesn't exist yet.
func WithBucketAutoCreate() Option {
	return func(opts *options) {
		opts.bucketAutoCreate = true
	}
}
