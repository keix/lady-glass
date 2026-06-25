package store_test

import (
	"context"
	"testing"

	"github.com/keix/lady-glass/internal/pipeline"
	"github.com/keix/lady-glass/internal/store"
)

func TestMemoryStore_GetJob_ReturnsNilWhenAbsent(t *testing.T) {
	st := store.NewMemoryStore()

	rec, err := st.GetJob(context.Background(), "missing")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if rec != nil {
		t.Fatalf("expected nil record, got %+v", rec)
	}
}

func TestMemoryStore_PutAndGetJob_RoundTrips(t *testing.T) {
	st := store.NewMemoryStore()
	ctx := context.Background()

	in := store.JobRecord{
		JobID:     "job_x",
		Status:    store.JobStatusRunning,
		InputURI:  "file://input.pdf",
		PageCount: 3,
	}
	if err := st.PutJob(ctx, in); err != nil {
		t.Fatalf("put: %v", err)
	}

	rec, err := st.GetJob(ctx, "job_x")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if rec == nil {
		t.Fatal("expected record, got nil")
	}
	if rec.Status != store.JobStatusRunning || rec.PageCount != 3 || rec.InputURI != "file://input.pdf" {
		t.Fatalf("round-trip mismatch: %+v", rec)
	}
}

func TestMemoryStore_ListStagesByJob_FiltersAndSortsByPage(t *testing.T) {
	st := store.NewMemoryStore()
	ctx := context.Background()

	// Seed pages out of order to test the sort. Also seed a different stage
	// and a different version that should be filtered out.
	for _, page := range []int{3, 1, 2} {
		if err := st.MarkSucceeded(ctx, pipeline.StepOutput{
			JobID: "job_y", Page: page, Stage: "gemini", Version: "v1",
			ResultURI: "file://r",
		}, ""); err != nil {
			t.Fatalf("seed page %d: %v", page, err)
		}
	}
	if err := st.MarkSucceeded(ctx, pipeline.StepOutput{
		JobID: "job_y", Page: 1, Stage: "line_ocr", Version: "v1",
		ResultURI: "file://r",
	}, "gemini"); err != nil {
		t.Fatalf("seed line_ocr: %v", err)
	}
	if err := st.MarkSucceeded(ctx, pipeline.StepOutput{
		JobID: "job_y", Page: 1, Stage: "gemini", Version: "v2",
		ResultURI: "file://r",
	}, ""); err != nil {
		t.Fatalf("seed v2: %v", err)
	}

	recs, err := st.ListStagesByJob(ctx, "job_y", "gemini", "v1")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(recs) != 3 {
		t.Fatalf("matched %d records, want 3", len(recs))
	}
	for i, r := range recs {
		if r.Page != i+1 {
			t.Fatalf("page at index %d = %d, want %d (sort failed)", i, r.Page, i+1)
		}
		if r.Stage != "gemini" || r.Version != "v1" {
			t.Fatalf("record %+v leaked through filter", r)
		}
	}
}

func TestMemoryStore_ListStagesByJob_OtherJobIsExcluded(t *testing.T) {
	st := store.NewMemoryStore()
	ctx := context.Background()

	if err := st.MarkSucceeded(ctx, pipeline.StepOutput{
		JobID: "job_a", Page: 1, Stage: "gemini", Version: "v1",
		ResultURI: "file://r",
	}, ""); err != nil {
		t.Fatalf("seed a: %v", err)
	}
	if err := st.MarkSucceeded(ctx, pipeline.StepOutput{
		JobID: "job_b", Page: 1, Stage: "gemini", Version: "v1",
		ResultURI: "file://r",
	}, ""); err != nil {
		t.Fatalf("seed b: %v", err)
	}

	recs, err := st.ListStagesByJob(ctx, "job_a", "gemini", "v1")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(recs) != 1 || recs[0].JobID != "job_a" {
		t.Fatalf("expected 1 record for job_a, got %+v", recs)
	}
}
