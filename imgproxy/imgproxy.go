package imgproxy

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"
)

type Service struct {
	baseURL string
	key     []byte
	salt    []byte
}

type ResizingType string

const (
	ResizingTypeFill ResizingType = "fill"
	ResizingTypeFit  ResizingType = "fit"
	ResizingTypeAuto ResizingType = "auto"
)

type Parameters struct {
	Resize  ResizingType
	Width   int
	Height  int
	Gravity string
	Enlarge bool
	Format  string
}

type Option func(*options) error

type options struct {
	key, salt []byte
}

func WithKeyAndSalt(key, salt []byte) Option {
	return func(opts *options) error {
		opts.key = key
		opts.salt = salt

		return nil
	}
}

func WithHexKeyAndSalt(keyHex, saltHex string) Option {
	return func(opts *options) error {
		var key, salt []byte
		var err error

		if key, err = hex.DecodeString(keyHex); err != nil {
			return fmt.Errorf("hex decoding key: %w", err)
		}

		if salt, err = hex.DecodeString(saltHex); err != nil {
			return fmt.Errorf("hex decoding salt: %w", err)
		}

		opts.key = key
		opts.salt = salt

		return nil
	}
}

func NewService(baseURL string, opts ...Option) (*Service, error) {
	// Make sure base URL contains no trailing slash
	baseURL = strings.TrimRight(baseURL, "/")

	var options options
	for _, opt := range opts {
		if err := opt(&options); err != nil {
			return nil, err
		}
	}

	return &Service{
		baseURL: baseURL,
		key:     options.key,
		salt:    options.salt,
	}, nil
}

func (s *Service) ImageURL(imgproxySourceURL string, params Parameters) (string, error) {
	var parts []string

	if params.Width > 0 || params.Height > 0 {
		enlarge := 0
		if params.Enlarge {
			enlarge = 1
		}
		resize := params.Resize
		if resize == "" {
			resize = ResizingTypeAuto
		}
		parts = append(parts, fmt.Sprintf("resize:%s:%d:%d:%d", resize, params.Width, params.Height, enlarge))
	}
	gravity := params.Gravity
	if gravity != "" {
		parts = append(parts, fmt.Sprintf("gravity:%s", gravity))
	}

	extension := params.Format
	if extension != "" {
		extension = "." + extension
	}

	encodedURL := base64.RawURLEncoding.EncodeToString([]byte(imgproxySourceURL))

	path := fmt.Sprintf("/%s/%s%s", strings.Join(parts, "/"), encodedURL, extension)

	// TODO Add support for unsigned URLs
	mac := hmac.New(sha256.New, s.key)
	mac.Write(s.salt)
	mac.Write([]byte(path))
	signature := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))

	return fmt.Sprintf("%s/%s%s", s.baseURL, signature, path), nil
}
