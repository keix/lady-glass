package workflow

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/pdfcpu/pdfcpu/pkg/api"

	"github.com/keix/lady-glass/internal/object"
)

// RenderPagesInput is the SFN task payload for the RenderPages step.
// It is consumed only on the "rendered" workflow branch — the
// "passthrough" branch skips this task and feeds the source PDF
// straight to SubmitPages.
type RenderPagesInput struct {
	JobID    string `json:"job_id"`
	InputURI string `json:"input_uri"`
}

// RenderPagesOutput hands SubmitPages a typed Pages list. The SFN ASL
// projects this into the SubmitPages task's "pages.$" parameter.
type RenderPagesOutput struct {
	JobID string   `json:"job_id"`
	Pages []string `json:"pages"`
}

// RenderPages downloads the source PDF, splits it into one-page PDFs
// via pdfcpu (pure Go, no system library), uploads each page back to
// the object store at the canonical pages/<n>/input.pdf key, and
// returns the list of URIs for SubmitPages to fan out from. The
// downstream Gemini stage already accepts application/pdf so no
// rasterisation is needed — we just split.
//
// pdfcpu.SplitFile operates on the local filesystem, so the work
// happens in /tmp. Lambda's 512 MB ephemeral storage gives plenty of
// room for typical statements; multi-hundred-page PDFs would need a
// different approach (streamed split, ECS, or a higher-tier Lambda).
//
// RenderPages is idempotent: re-running it overwrites the same
// per-page object keys with identical bytes (pdfcpu output is
// deterministic for a given input), so SubmitPages can safely
// re-enqueue from the same Pages list.
func RenderPages(ctx context.Context, in RenderPagesInput, obj object.Store) (RenderPagesOutput, error) {
	if in.JobID == "" {
		return RenderPagesOutput{}, fmt.Errorf("render_pages: empty job_id")
	}
	if in.InputURI == "" {
		return RenderPagesOutput{}, fmt.Errorf("render_pages: empty input_uri")
	}

	body, err := obj.Get(ctx, in.InputURI)
	if err != nil {
		return RenderPagesOutput{}, fmt.Errorf("render_pages: fetch input %q: %w", in.InputURI, err)
	}

	tmpDir, err := os.MkdirTemp("", "render-")
	if err != nil {
		return RenderPagesOutput{}, fmt.Errorf("render_pages: tmpdir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	srcPath := filepath.Join(tmpDir, "input.pdf")
	if err := os.WriteFile(srcPath, body, 0o600); err != nil {
		return RenderPagesOutput{}, fmt.Errorf("render_pages: write source: %w", err)
	}

	outDir := filepath.Join(tmpDir, "pages")
	if err := os.Mkdir(outDir, 0o700); err != nil {
		return RenderPagesOutput{}, fmt.Errorf("render_pages: mkdir pages: %w", err)
	}

	// span=1 → one page per output file. pdfcpu names them
	// input_1.pdf, input_2.pdf, … in source order.
	if err := api.SplitFile(srcPath, outDir, 1, nil); err != nil {
		return RenderPagesOutput{}, fmt.Errorf("render_pages: split: %w", err)
	}

	entries, err := os.ReadDir(outDir)
	if err != nil {
		return RenderPagesOutput{}, fmt.Errorf("render_pages: read output dir: %w", err)
	}
	pageFiles, err := sortedPDFCPUPageFiles(entries)
	if err != nil {
		return RenderPagesOutput{}, err
	}

	pages := make([]string, 0, len(pageFiles))
	for i, pageFile := range pageFiles {
		page := i + 1
		fullPath := filepath.Join(outDir, pageFile.name)
		pageBody, err := os.ReadFile(fullPath)
		if err != nil {
			return RenderPagesOutput{}, fmt.Errorf("render_pages: read page %d: %w", page, err)
		}
		key := fmt.Sprintf("jobs/%s/pages/%06d/input.pdf", in.JobID, page)
		uri, err := obj.PutBytes(ctx, key, pageBody, "application/pdf")
		if err != nil {
			return RenderPagesOutput{}, fmt.Errorf("render_pages: upload page %d: %w", page, err)
		}
		pages = append(pages, uri)
	}

	return RenderPagesOutput{JobID: in.JobID, Pages: pages}, nil
}

type pdfcpuPageFile struct {
	name string
	page int
}

func sortedPDFCPUPageFiles(entries []os.DirEntry) ([]pdfcpuPageFile, error) {
	files := make([]pdfcpuPageFile, 0, len(entries))
	for _, ent := range entries {
		if ent.IsDir() {
			return nil, fmt.Errorf("render_pages: unexpected directory in split output: %s", ent.Name())
		}
		page, err := pdfcpuPageNumber(ent.Name())
		if err != nil {
			return nil, err
		}
		files = append(files, pdfcpuPageFile{name: ent.Name(), page: page})
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].page < files[j].page
	})
	return files, nil
}

func pdfcpuPageNumber(name string) (int, error) {
	base := strings.TrimSuffix(filepath.Base(name), filepath.Ext(name))
	idx := strings.LastIndex(base, "_")
	if idx < 0 || idx == len(base)-1 {
		return 0, fmt.Errorf("render_pages: split output %q has no page suffix", name)
	}
	page, err := strconv.Atoi(base[idx+1:])
	if err != nil || page < 1 {
		return 0, fmt.Errorf("render_pages: split output %q has invalid page suffix", name)
	}
	return page, nil
}
