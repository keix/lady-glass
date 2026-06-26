package workflow_test

import (
	"context"
	"testing"

	"github.com/keix/lady-glass/internal/store"
	"github.com/keix/lady-glass/internal/workflow"
)

func TestMarkJobFailed_FlipsStatusAndPreservesContext(t *testing.T) {
	st := store.NewMemoryStore()
	ctx := context.Background()

	if err := st.PutJob(ctx, store.JobRecord{
		JobID:     "j_fail",
		Status:    store.JobStatusRunning,
		InputURI:  "s3://bkt/jobs/j_fail/input.pdf",
		PageCount: 3,
		Mode:      "rendered",
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	out, err := workflow.MarkJobFailed(ctx, workflow.MarkJobFailedInput{
		JobID: "j_fail",
		Error: "one or more pages failed",
	}, st)
	if err != nil {
		t.Fatalf("mark: %v", err)
	}
	if out.JobID != "j_fail" {
		t.Fatalf("out.JobID = %q", out.JobID)
	}

	rec, err := st.GetJob(ctx, "j_fail")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if rec.Status != store.JobStatusFailed {
		t.Fatalf("status = %q, want failed", rec.Status)
	}
	if rec.Error != "one or more pages failed" {
		t.Fatalf("error = %q", rec.Error)
	}
	if rec.InputURI != "s3://bkt/jobs/j_fail/input.pdf" {
		t.Fatalf("InputURI lost: %q", rec.InputURI)
	}
	if rec.PageCount != 3 {
		t.Fatalf("PageCount lost: %d", rec.PageCount)
	}
	if rec.Mode != "rendered" {
		t.Fatalf("Mode lost: %q", rec.Mode)
	}
}

func TestMarkJobFailed_WithoutPreexistingRecord(t *testing.T) {
	st := store.NewMemoryStore()

	if _, err := workflow.MarkJobFailed(context.Background(), workflow.MarkJobFailedInput{
		JobID: "j_new",
		Error: "submitted with no prior record",
	}, st); err != nil {
		t.Fatalf("mark: %v", err)
	}

	rec, err := st.GetJob(context.Background(), "j_new")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if rec == nil || rec.Status != store.JobStatusFailed {
		t.Fatalf("record after mark = %+v", rec)
	}
	if rec.InputURI != "" || rec.PageCount != 0 {
		t.Fatalf("expected zero-valued InputURI / PageCount when no prior record; got %+v", rec)
	}
}

func TestMarkJobFailed_RejectsEmptyJobID(t *testing.T) {
	st := store.NewMemoryStore()

	if _, err := workflow.MarkJobFailed(context.Background(), workflow.MarkJobFailedInput{}, st); err == nil {
		t.Fatal("expected error for empty job_id, got nil")
	}
}
