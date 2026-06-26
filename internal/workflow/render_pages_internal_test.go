package workflow

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSortedPDFCPUPageFilesSortsByNumericSuffix(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"input_1.pdf", "input_10.pdf", "input_2.pdf"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("%PDF\n"), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	files, err := sortedPDFCPUPageFiles(entries)
	if err != nil {
		t.Fatalf("sort files: %v", err)
	}

	got := make([]string, len(files))
	for i, file := range files {
		got[i] = file.name
	}
	want := []string{"input_1.pdf", "input_2.pdf", "input_10.pdf"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("sorted files = %v, want %v", got, want)
		}
	}
}

func TestPDFCPUPageNumberRejectsInvalidSuffix(t *testing.T) {
	for _, name := range []string{"input.pdf", "input_x.pdf", "input_0.pdf"} {
		if _, err := pdfcpuPageNumber(name); err == nil {
			t.Fatalf("expected error for %q", name)
		}
	}
}
