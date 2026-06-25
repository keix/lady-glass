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

func (s *FileStore) pathFromURI(uri string) string {
	return strings.TrimPrefix(uri, "file://")
}
