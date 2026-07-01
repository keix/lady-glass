package object_test

import (
	"context"
	"testing"

	"github.com/keix/lady-glass/internal/object"
)

func TestFileStore_Exists_ReturnsFalseWhenObjectAbsent(t *testing.T) {
	// A key that was never written must be reported (false, nil) so
	// idempotent-write stages (archive-result) know it is safe to
	// proceed with the initial PutBytes.
	store := object.NewFileStore(t.TempDir())

	ok, err := store.Exists(context.Background(), "file:///nonexistent/path.json")
	if err != nil {
		t.Fatalf("exists: %v", err)
	}
	if ok {
		t.Fatal("exists = true, want false")
	}
}

func TestFileStore_Exists_ReturnsTrueAfterPut(t *testing.T) {
	store := object.NewFileStore(t.TempDir())
	uri, err := store.PutBytes(context.Background(), "manifests/j.json", []byte("{}"), "application/json")
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	ok, err := store.Exists(context.Background(), uri)
	if err != nil {
		t.Fatalf("exists: %v", err)
	}
	if !ok {
		t.Fatal("exists = false after successful put")
	}
}
