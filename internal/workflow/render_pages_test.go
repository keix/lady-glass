package workflow_test

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/keix/lady-glass/internal/object"
	"github.com/keix/lady-glass/internal/workflow"
)

func TestRenderPages_SplitsRealPDFIntoPerPagePDFs(t *testing.T) {
	pdfBytes := loadFixturePDF(t)

	obj := object.NewFileStore(t.TempDir())
	ctx := context.Background()

	inputURI, err := obj.PutBytes(ctx, "jobs/j1/input.pdf", pdfBytes, "application/pdf")
	if err != nil {
		t.Fatalf("seed source: %v", err)
	}

	out, err := workflow.RenderPages(ctx, workflow.RenderPagesInput{
		JobID:    "j1",
		InputURI: inputURI,
	}, obj)
	if err != nil {
		t.Fatalf("render: %v", err)
	}

	if len(out.Pages) < 1 {
		t.Fatalf("got 0 pages, want ≥ 1")
	}

	// Every output URI must point at a real PDF object.
	for i, uri := range out.Pages {
		body, err := obj.Get(ctx, uri)
		if err != nil {
			t.Fatalf("get page %d (%s): %v", i+1, uri, err)
		}
		if len(body) < 32 {
			t.Fatalf("page %d body too small: %d bytes", i+1, len(body))
		}
		if !strings.HasPrefix(string(body), "%PDF") {
			t.Fatalf("page %d is not a PDF (prefix=%q)", i+1, string(body[:8]))
		}
		// URI shape: jobs/<job>/pages/<NNNNNN>/input.pdf
		if !strings.Contains(uri, "/jobs/j1/pages/") || !strings.HasSuffix(uri, "/input.pdf") {
			t.Fatalf("page %d uri %q does not follow the per-page key shape", i+1, uri)
		}
	}
}

func TestRenderPages_RejectsEmptyInput(t *testing.T) {
	obj := object.NewFileStore(t.TempDir())

	cases := []workflow.RenderPagesInput{
		{},
		{JobID: "j1"},
		{InputURI: "file:///foo.pdf"},
	}
	for i, in := range cases {
		if _, err := workflow.RenderPages(context.Background(), in, obj); err == nil {
			t.Fatalf("case %d: expected error for %+v", i, in)
		}
	}
}

func TestRenderPages_RejectsNonPDFBytes(t *testing.T) {
	obj := object.NewFileStore(t.TempDir())
	ctx := context.Background()

	inputURI, err := obj.PutBytes(ctx, "jobs/j2/input.pdf", []byte("this is not a PDF at all"), "application/pdf")
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	_, err = workflow.RenderPages(ctx, workflow.RenderPagesInput{
		JobID:    "j2",
		InputURI: inputURI,
	}, obj)
	if err == nil {
		t.Fatal("expected error when input is not a valid PDF")
	}
	if !strings.Contains(err.Error(), "split") {
		t.Fatalf("error %q does not point at the split step", err)
	}
}

// loadFixturePDF returns a small PDF for split testing. The
// repository's only checked-in PDF lives at testdata/private/smbc.pdf
// (gitignored, present on contributor machines); when absent the test
// is skipped so CI without the fixture stays green.
func loadFixturePDF(t *testing.T) []byte {
	t.Helper()

	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Join(filepath.Dir(thisFile), "..", "..")
	candidate := filepath.Join(repoRoot, "testdata", "private", "smbc.pdf")

	body, err := os.ReadFile(candidate)
	if err != nil {
		t.Skipf("fixture %s not present; skipping (this is fine in CI without private testdata)", candidate)
	}
	return body
}
