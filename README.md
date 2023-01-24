# filestore: Go file storage implementations

[![GoDoc](https://godoc.org/github.com/networkteam/filestore?status.svg)](https://godoc.org/github.com/networkteam/filestore)
[![Build Status](https://github.com/networkteam/filestore/workflows/run%20tests/badge.svg)](https://github.com/networkteam/filestore/actions?workflow=run%20tests)
[![Go Report Card](https://goreportcard.com/badge/github.com/networkteam/filestore)](https://goreportcard.com/report/github.com/networkteam/filestore)

## Features

* Local file storage (in a directory with subdirectories derived from hash prefix)
* Identify uploaded files as hashes (e.g. in a database)

## Scope

Store uploaded files locally or in an object store (S3) with a consistent interface.
Add capabilities for image processing with e.g. imgproxy.

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

	"github.com/networkteam/filestore"
)

func main() {
	ctx := context.Background()

	fStore, err := filestore.NewLocal("./tmp", "./assets")
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

	"github.com/networkteam/filestore"
)

func main() {
	ctx := context.Background()

	fStore, err := filestore.NewS3(
		ctx,
		"s3.eu-central-1.amazonaws.com",
		"my-bucket",
		filestore.WithS3CredentialsV4("my-access-key", "********", ""),
	)
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
