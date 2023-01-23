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
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/networkteam/filestore"
)

func main() {
	fStore, err := filestore.NewLocal("./tmp", "./assets")
	if err != nil {
		log.Fatal(err)
	}

	// Storing
	hash, err := fStore.Store(strings.NewReader("Hello World"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(hash)

	// Fetching
	r, err := fStore.Fetch(hash)
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