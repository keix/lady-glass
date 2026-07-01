package object

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type FileStore struct {
	Root string
}

func NewFileStore(root string) *FileStore {
	return &FileStore{Root: root}
}

func (s *FileStore) Get(ctx context.Context, uri string) ([]byte, error) {
	path := s.pathFromURI(uri)
	return os.ReadFile(path)
}

func (s *FileStore) PutJSON(ctx context.Context, key string, v any) (string, error) {
	body, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return "", err
	}

	return s.PutBytes(ctx, key, body, "application/json")
}

func (s *FileStore) PutText(ctx context.Context, key string, text string) (string, error) {
	return s.PutBytes(ctx, key, []byte(text), "text/plain")
}

func (s *FileStore) PutBytes(ctx context.Context, key string, body []byte, contentType string) (string, error) {
	path := filepath.Join(s.Root, filepath.Clean(key))

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", err
	}

	if err := os.WriteFile(path, body, 0o644); err != nil {
		return "", err
	}

	return fmt.Sprintf("file://%s", path), nil
}

// URIFor is the file:// URI at which key would land on disk after a
// subsequent Put. It mirrors the return of PutBytes exactly so callers
// can pre-compute the URI (e.g. for an Exists probe) without touching
// the filesystem.
func (s *FileStore) URIFor(key string) string {
	return fmt.Sprintf("file://%s", filepath.Join(s.Root, filepath.Clean(key)))
}

// Exists reports whether the file backing uri is present on disk. A
// missing file is (false, nil); any other stat error surfaces as an
// error so callers can distinguish "safe to write" from "cannot tell".
func (s *FileStore) Exists(ctx context.Context, uri string) (bool, error) {
	if _, err := os.Stat(s.pathFromURI(uri)); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *FileStore) pathFromURI(uri string) string {
	return strings.TrimPrefix(uri, "file://")
}
