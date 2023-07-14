package s3

import "io"

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
