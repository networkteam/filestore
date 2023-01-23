package filestore_test

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/networkteam/filestore"
)

func TestLocal_Store(t *testing.T) {
	testDir, err := os.MkdirTemp("", "test-store")
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = os.RemoveAll(testDir)
	})

	fStore, err := filestore.NewLocal(path.Join(testDir, "tmp"), path.Join(testDir, "assets"))
	require.NoError(t, err)

	r := strings.NewReader("Test content")
	hash, err := fStore.Store(r)
	require.NoError(t, err)

	assert.Equal(t, "9d9595c5d94fb65b824f56e9999527dba9542481580d69feb89056aabaa0aa87", hash)

	// Can be stored again
	_, _ = r.Seek(0, io.SeekStart)
	hash, err = fStore.Store(r)
	require.NoError(t, err)

	assert.Equal(t, "9d9595c5d94fb65b824f56e9999527dba9542481580d69feb89056aabaa0aa87", hash)

	// Check that tmp test dir is empty after store
	files, err := os.ReadDir(path.Join(testDir, "tmp"))
	require.NoError(t, err)
	assert.Equal(t, 0, len(files), "tmp dir should be empty")
}

func TestLocal_ImgproxyURLSource(t *testing.T) {
	testDir, err := os.MkdirTemp("", "test-store")
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = os.RemoveAll(testDir)
	})

	fStore, err := filestore.NewLocal(path.Join(testDir, "tmp"), path.Join(testDir, "assets"))
	require.NoError(t, err)

	// Check existing file
	r := strings.NewReader("Test content")
	hash, err := fStore.Store(r)
	require.NoError(t, err)

	require.Equal(t, "9d9595c5d94fb65b824f56e9999527dba9542481580d69feb89056aabaa0aa87", hash)

	url, err := fStore.ImgproxyURLSource("9d9595c5d94fb65b824f56e9999527dba9542481580d69feb89056aabaa0aa87")
	require.NoError(t, err)

	assert.Equal(t, "local:///9d/9d9595c5d94fb65b824f56e9999527dba9542481580d69feb89056aabaa0aa87", url)
}

func TestLocal_Fetch(t *testing.T) {
	testDir, err := os.MkdirTemp("", "test-store")
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = os.RemoveAll(testDir)
	})

	fStore, err := filestore.NewLocal(path.Join(testDir, "tmp"), path.Join(testDir, "assets"))
	require.NoError(t, err)

	// Check non-existing file
	_, err = fStore.Fetch("a09595c5d94fb65b824f56e9999527dba9542481580d69feb89056aabaa0aa87")
	require.Error(t, err)

	// Check existing file
	r := strings.NewReader("Test content")
	hash, err := fStore.Store(r)
	require.NoError(t, err)

	entry, err := fStore.Fetch(hash)
	require.NoError(t, err)

	defer entry.Close()

	content, err := ioutil.ReadAll(entry)
	require.NoError(t, err)

	assert.Equal(t, "Test content", string(content))
}

func TestLocal_Iterate(t *testing.T) {
	testDir, err := os.MkdirTemp("", "test-store")
	require.NoError(t, err)

	t.Cleanup(func() { _ = os.RemoveAll(testDir) })

	fStore, err := filestore.NewLocal(path.Join(testDir, "tmp"), path.Join(testDir, "assets"))
	require.NoError(t, err)

	r := strings.NewReader("Test content")
	hash, err := fStore.Store(r)
	require.NoError(t, err)

	assert.Equal(t, "9d9595c5d94fb65b824f56e9999527dba9542481580d69feb89056aabaa0aa87", hash)

	var files []string
	err = fStore.Iterate(10, func(hashes []string) error {
		files = append(files, hashes...)
		return nil
	})
	require.NoError(t, err)

	assert.Equal(t, []string{"9d9595c5d94fb65b824f56e9999527dba9542481580d69feb89056aabaa0aa87"}, files)

	// Store some more files
	for i := 0; i < 15; i++ {
		r := strings.NewReader(fmt.Sprintf("Test content %d", i))
		_, err = fStore.Store(r)
		require.NoError(t, err)
	}

	files = files[:0]

	err = fStore.Iterate(5, func(hashes []string) error {
		files = append(files, hashes...)
		return nil
	})
	require.NoError(t, err)

	assert.Len(t, files, 16)
}

func TestLocal_Remove(t *testing.T) {
	testDir, err := os.MkdirTemp("", "test-store")
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = os.RemoveAll(testDir)
	})

	fStore, err := filestore.NewLocal(path.Join(testDir, "tmp"), path.Join(testDir, "assets"))
	require.NoError(t, err)

	r := strings.NewReader("Test content")
	hash, err := fStore.Store(r)
	require.NoError(t, err)

	assert.Equal(t, "9d9595c5d94fb65b824f56e9999527dba9542481580d69feb89056aabaa0aa87", hash)

	err = fStore.Remove(hash)
	require.NoError(t, err)

	// Check that assets test dir is empty after remove
	files, err := os.ReadDir(path.Join(testDir, "assets"))
	require.NoError(t, err)
	assert.Empty(t, files, "assets dir should be empty")
}
