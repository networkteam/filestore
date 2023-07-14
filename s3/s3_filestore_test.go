package s3_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"net/url"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/networkteam/filestore"
	"github.com/networkteam/filestore/s3"
)

func TestS3_Roundtrip(t *testing.T) {
	ctx := context.Background()

	store := createS3Filestore(t, ctx)

	reader := strings.NewReader("Hello World")
	expectedHash := "a591a6d40bf420404a011733cfb7b190d62c65bf0bcda32b57b277d9ad9f146e"

	hash, err := store.Store(ctx, reader)
	require.NoError(t, err)

	assert.Equal(t, expectedHash, hash)

	size, err := store.Size(ctx, hash)
	require.NoError(t, err)

	assert.Equal(t, int64(11), size)

	imgproxyURLSource, err := store.ImgproxyURLSource(hash)
	require.NoError(t, err)

	bucketName := "assets"
	if v := os.Getenv("S3_BUCKET"); v != "" {
		bucketName = v
	}

	assert.Equal(t, "s3://"+bucketName+"/"+expectedHash, imgproxyURLSource)

	fetch, err := store.Fetch(ctx, hash)
	require.NoError(t, err)

	data, err := io.ReadAll(fetch)
	require.NoError(t, err)

	assert.Equal(t, "Hello World", string(data))

	err = fetch.Close()
	require.NoError(t, err)

	// Check iterating a single object

	var hashes []string
	err = store.Iterate(ctx, 5, func(hshs []string) error {
		hashes = append(hashes, hshs...)
		return nil
	})
	require.NoError(t, err)

	assert.Equal(t, []string{hash}, hashes)
}

func TestS3_Store(t *testing.T) {
	ctx := context.Background()

	store := createS3Filestore(t, ctx)

	reader := strings.NewReader("Hello World")

	sizedReader := s3.SizedReader(reader, 11)

	hash, err := store.Store(ctx, sizedReader)
	require.NoError(t, err)

	expectedHash := "a591a6d40bf420404a011733cfb7b190d62c65bf0bcda32b57b277d9ad9f146e"
	assert.Equal(t, expectedHash, hash)

	size, err := store.Size(ctx, hash)
	require.NoError(t, err)

	assert.Equal(t, int64(11), size)
}

func TestS3_Remove(t *testing.T) {
	ctx := context.Background()
	store := createS3Filestore(t, ctx)

	reader := strings.NewReader("Hello World")

	hash, err := store.Store(ctx, reader)
	require.NoError(t, err)

	err = store.Remove(ctx, hash)
	require.NoError(t, err)

	_, err = store.Fetch(ctx, hash)
	require.ErrorIs(t, err, filestore.ErrNotExist)
}

func TestS3_Iterate(t *testing.T) {
	ctx := context.Background()

	store := createS3Filestore(t, ctx)

	for i := 0; i < 21; i++ {
		reader := strings.NewReader(fmt.Sprintf("Hello World %d", i))
		_, err := store.Store(ctx, reader)
		require.NoError(t, err)
	}

	var hashes []string
	calls := 0
	err := store.Iterate(ctx, 5, func(hshs []string) error {
		calls++
		hashes = append(hashes, hshs...)
		return nil
	})
	require.NoError(t, err)

	assert.Equal(t, 5, calls)

	sort.Strings(hashes)

	assert.Equal(t, []string{
		"028c8e5c1302492d6d2454f6195d17c28b35828643dbf253519fbc560ac2935d",
		"059c488c1f9ab56f9313e5753f1118739d6a97123cb94401b027b9f1abe34696",
		"171a257fb7510d42a538e5cda085654536137b5f658441e1f12a2d4b77be7987",
		"1aed4d8555515c961bffea900d5e7f1c1e4abf0f6da250d8bf15843106e0533b",
		"1ca885c938e75fe6dd8d842ca6e4caf6f28b428f40d414705acfdea5e31bfd64",
		"3192311c9a22e747f75d86f567e91795a4eaa080ba05ba6b8dc07a6a36eff5bc",
		"3df75539dda4c512db688b3f1d86184c0d7b99cbea1eb87dec8385a2651ac1f3",
		"62591d74d3dfcb938447622f08c9624f8358eba5715561742d2389ab7ec36623",
		"74509cd16a43bbfb54018b39a40a53bd7f6fe96013e49426cbc0cba75997b648",
		"7c6f7f8f62f3aa8e2bcad4cc29fd54d79c4e1801b2a9e2c993afd7a3858e48ec",
		"8204ebcb308d2240e21acb5dfb8d48131ad61119b244aac854070abdbf4c7ca9",
		"8ab19be0b400f0dfbc42ba114419c7e65f8fc86948bb08c53efcef5c322a8a9f",
		"9cb4ebb9b037d06a52d9bdd33758f94b4a2d73efbed3d286aeb860ec28b35e32",
		"b4c2c83f9945fcaf3b6db1d4cdd0db85d8e944f8bcdf52569d49796fe2095c18",
		"d6aa7522fd294d89a657e4099ff936801beb135c77f7f40a53b06205c38511c8",
		"d8ae5831d3d79695bb388370a89a5e76408443c0cefb262802cfa36cbddd96b9",
		"e4a20b8392401b3176db5d49b803e119741e66c63d4f872eb86b8241685e9ff9",
		"e7f70e33727c665ecaeaa498a499b649900f1c8107bbaed7dc5404e8dfcaf70d",
		"e927cb7ab5f9e72979e3608fec1e4b40862f6d5e4df3bc2e339e16de0f37bb68",
		"f9a6b044d1024184a0b3b88e27fe90d7b69b72cb0aedcc6502c22f88f87b4fa4",
		"fba23e8484d943ba031d255cfb9300926a4c2af5c64e2b8d221b0cb4d37be458",
	}, hashes)

	// Check that iterate stops when callback returns error
	myErr := errors.New("my error")
	err = store.Iterate(ctx, 5, func(hashes []string) error {
		return myErr
	})
	require.ErrorIs(t, err, myErr)
}

func createS3Filestore(t *testing.T, ctx context.Context) *s3.Filestore {
	t.Helper()

	if endpoint := os.Getenv("S3_ENDPOINT"); endpoint != "" {
		t.Logf("Using S3 endpoint %s for test", endpoint)

		bucketName := os.Getenv("S3_BUCKET")
		if bucketName == "" {
			t.Fatal("S3_BUCKET is not set")
		}

		var opts []s3.Option

		accessKey := os.Getenv("S3_ACCESS_KEY")
		if accessKey == "" {
			t.Fatal("S3_ACCESS_KEY is not set")
		}
		secretKey := os.Getenv("S3_SECRET_KEY")
		if secretKey == "" {
			t.Fatal("S3_SECRET_KEY is not set")
		}
		opts = append(opts, s3.WithCredentialsV4(accessKey, secretKey, ""))

		if region := os.Getenv("S3_REGION"); region != "" {
			opts = append(opts, s3.WithRegion(region))
		}

		if v := os.Getenv("S3_BUCKET_LOOKUP_DNS"); v == "1" || v == "true" {
			opts = append(opts, s3.WithBucketLookupDNS())
		}

		if v := os.Getenv("S3_BUCKET_LOOKUP_PATH"); v == "1" || v == "true" {
			opts = append(opts, s3.WithBucketLookupPath())
		}

		if secure := os.Getenv("S3_SECURE"); secure == "1" || secure == "true" {
			opts = append(opts, s3.WithSecure())
		}

		if _, ok := os.LookupEnv("S3_BUCKET_AUTO_CREATE"); ok {
			opts = append(opts, s3.WithBucketAutoCreate())
		}

		store, err := s3.NewFilestore(
			ctx,
			endpoint,
			bucketName,
			opts...,
		)
		require.NoError(t, err)

		t.Cleanup(func() {
			// Remove all objects from the bucket
			err := store.Iterate(ctx, 50, func(hashes []string) error {
				for _, hash := range hashes {
					err := store.Remove(ctx, hash)
					require.NoError(t, err)
				}
				return nil
			})
			require.NoError(t, err)
		})

		return store
	}

	t.Log("Using in-memory fake S3 server for test (no S3_ENDPOINT env var set)")

	backend := s3mem.New()
	faker := gofakes3.New(backend)
	ts := httptest.NewServer(faker.Server())

	t.Cleanup(func() {
		ts.Close()
	})

	parsedURL, err := url.Parse(ts.URL)
	require.NoError(t, err)

	store, err := s3.NewFilestore(
		ctx,
		parsedURL.Host,
		"assets",
		s3.WithCredentialsV4("YOUR-ACCESSKEYID", "YOUR-SECRETACCESSKEY", ""),
		s3.WithBucketAutoCreate(),
	)
	require.NoError(t, err)

	return store
}
