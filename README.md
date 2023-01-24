# filestore: Go file storage implementations

[![GoDoc](https://godoc.org/github.com/networkteam/filestore?status.svg)](https://godoc.org/github.com/networkteam/filestore)
[![Build Status](https://github.com/networkteam/filestore/workflows/run%20tests/badge.svg)](https://github.com/networkteam/filestore/actions?workflow=run%20tests)
[![Go Report Card](https://goreportcard.com/badge/github.com/networkteam/filestore)](https://goreportcard.com/report/github.com/networkteam/filestore)

## Features

* Identify uploaded files as hashes (e.g. for reference in a database)
* Implementations
  * Local file storage (in a directory with subdirectories derived from hash prefix)
  * S3 based file storage (tested with MinIO and AWS S3)

## Scope

An abstracted file storage in webapps is the main use case.
It is well suited to handle file uploads, retrieval and deletion.
With the integration of image processing like [imgproxy](https://imgproxy.net/) it provides a simple and efficient way to handle files and images in webapps.
When storing files in S3 a webapp can be run stateless which simplifies container deployments. 

## Install

```sh
go get github.com/networkteam/filestore
```

## Usage

### Local filestore

```go
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/networkteam/filestore/local"
)

func main() {
	ctx := context.Background()

	fStore, err := local.NewFilestore("./tmp", "./assets")
	if err != nil {
		log.Fatal(err)
	}

	// Storing
	hash, err := fStore.Store(ctx, strings.NewReader("Hello World"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(hash)

	// Fetching
	r, err := fStore.Fetch(ctx, hash)
	if err != nil {
		log.Fatal(err)
	}
	defer r.Close()
	content, err := io.ReadAll(r)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(content))
}
```

### S3 filestore

```go
package main

import (
  "context"
  "fmt"
  "io"
  "log"
  "strings"

  "github.com/networkteam/filestore/s3"
)

func main() {
  ctx := context.Background()

  fStore, err := s3.NewFilestore(
    ctx,
    "s3.eu-central-1.amazonaws.com",
    "my-bucket",
    s3.WithCredentialsV4("my-access-key", "********", ""),
  )
  if err != nil {
    log.Fatal(err)
  }

  // Storing
  text := "Hello World"
  sr := strings.NewReader(text)
  // Wrap reader with s3.SizedReader to set content length in advance
  hash, err := fStore.Store(ctx, s3.SizedReader(sr, int64(len(text))))
  if err != nil {
    log.Fatal(err)
  }
  fmt.Println(hash)

  // Fetching
  r, err := fStore.Fetch(ctx, hash)
  if err != nil {
    log.Fatal(err)
  }
  defer r.Close()
  content, err := io.ReadAll(r)
  if err != nil {
    log.Fatal(err)
  }
  fmt.Println(string(content))
}
```

## Dependencies

The filestore module provides each implementation in its own package to reduce the amount of transitive dependencies (e.g. you don't need a S3 client if not using `s3.Filestore`).

## Development

### Tests

#### S3 filestore

* S3 filestore will use a fake S3 server by default
* An external S3 server and bucket can be used by setting the environment variables `S3_ENDPOINT`, `S3_BUCKET`, `S3_ACCESS_KEY` and `S3_SECRET_KEY`
* Tests can be run against AWS S3 like this (make sure to allow sufficient access to bucket):
    ```sh
    S3_ENDPOINT=s3.eu-central-1.amazonaws.com S3_BUCKET=my-bucket-name S3_ACCESS_KEY=my-access-key S3_SECRET_KEY=******** go test
    ```
* Tests can be run against a MinIO server like this:
    ```sh
    S3_ENDPOINT=localhost:9000 S3_BUCKET=my-bucket-name S3_ACCESS_KEY=my-access-key S3_SECRET_KEY=******** go test
    ```

## License

[MIT](./LICENSE)
