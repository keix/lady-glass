package workflow_test

import (
	"context"
	"errors"
	"testing"

	"github.com/keix/lady-glass/internal/pipeline"
	"github.com/keix/lady-glass/internal/store"
	"github.com/keix/lady-glass/internal/workflow"
)

func seedStage(t *testing.T, st store.Store, jobID string, page int, status store.StageStatus) {
	t.Helper()
	ctx := context.Background()
	switch status {
	case store.StageStatusSucceeded:
		if err := st.MarkSucceeded(ctx, pipeline.StepOutput{
			JobID:     jobID,
			Page:      page,
			Stage:     "gemini",
			Version:   "v1",
			ResultURI: "file://r",
		}, ""); err != nil {
			t.Fatalf("seed page %d succeeded: %v", page, err)
		}
	case store.StageStatusFailed:
		if err := st.MarkFailed(ctx, pipeline.StepInput{
			JobID:   jobID,
			Page:    page,
			Stage:   "gemini",
			Version: "v1",
		}, errors.New("boom")); err != nil {
			t.Fatalf("seed page %d failed: %v", page, err)
		}
	case store.StageStatusRunning:
		if err := st.MarkRunning(context.Background(), pipeline.StepInput{
			JobID:   jobID,
			Page:    page,
			Stage:   "gemini",
			Version: "v1",
		}); err != nil {
			t.Fatalf("seed page %d running: %v", page, err)
		}
	default:
		t.Fatalf("unexpected seed status %q", status)
	}
}

func TestCheckPages_AllSucceeded_ReturnsSucceeded(t *testing.T) {
	st := store.NewMemoryStore()
	for _, p := range []int{1, 2, 3} {
		seedStage(t, st, "j", p, store.StageStatusSucceeded)
	}

	out, err := workflow.CheckPages(context.Background(), workflow.CheckPagesInput{
		JobID: "j", PageCount: 3, FinalStage: "gemini", FinalVersion: "v1",
	}, st)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if out.Status != workflow.CheckPagesStatusSucceeded {
		t.Fatalf("status = %q, want succeeded", out.Status)
	}
	if out.Succeeded != 3 || out.Failed != 0 || out.Pending != 0 {
		t.Fatalf("counts = %+v", out)
	}
}

func TestCheckPages_AnyFailed_ReturnsFailedFast(t *testing.T) {
	st := store.NewMemoryStore()
	seedStage(t, st, "j", 1, store.StageStatusSucceeded)
	seedStage(t, st, "j", 2, store.StageStatusFailed)
	// page 3 has no record yet

	out, err := workflow.CheckPages(context.Background(), workflow.CheckPagesInput{
		JobID: "j", PageCount: 3, FinalStage: "gemini", FinalVersion: "v1",
	}, st)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if out.Status != workflow.CheckPagesStatusFailed {
		t.Fatalf("status = %q, want failed", out.Status)
	}
	if out.Failed != 1 {
		t.Fatalf("failed count = %d, want 1", out.Failed)
	}
}

func TestCheckPages_SomeRunning_ReturnsPending(t *testing.T) {
	st := store.NewMemoryStore()
	seedStage(t, st, "j", 1, store.StageStatusSucceeded)
	seedStage(t, st, "j", 2, store.StageStatusRunning)
	// page 3 has no record yet

	out, err := workflow.CheckPages(context.Background(), workflow.CheckPagesInput{
		JobID: "j", PageCount: 3, FinalStage: "gemini", FinalVersion: "v1",
	}, st)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if out.Status != workflow.CheckPagesStatusPending {
		t.Fatalf("status = %q, want pending", out.Status)
	}
	if out.Succeeded != 1 || out.Failed != 0 {
		t.Fatalf("counts = %+v", out)
	}
	// 3 expected, 1 succeeded, 0 failed -> 2 pending
	if out.Pending != 2 {
		t.Fatalf("pending = %d, want 2", out.Pending)
	}
}

func TestCheckPages_NoRecords_ReturnsPending(t *testing.T) {
	st := store.NewMemoryStore()

	out, err := workflow.CheckPages(context.Background(), workflow.CheckPagesInput{
		JobID: "fresh", PageCount: 5, FinalStage: "gemini", FinalVersion: "v1",
	}, st)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if out.Status != workflow.CheckPagesStatusPending {
		t.Fatalf("status = %q, want pending", out.Status)
	}
	if out.Pending != 5 {
		t.Fatalf("pending = %d, want 5", out.Pending)
	}
}

func TestCheckPages_DifferentStageDoesNotCount(t *testing.T) {
	st := store.NewMemoryStore()
	// One page succeeded under mock v1 — should not count toward
	// the final gemini v1 stage's progress.
	if err := st.MarkSucceeded(context.Background(), pipeline.StepOutput{
		JobID: "j", Page: 1, Stage: "mock", Version: "v1",
		ResultURI: "file://r",
	}, "gemini"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	out, err := workflow.CheckPages(context.Background(), workflow.CheckPagesInput{
		JobID: "j", PageCount: 1, FinalStage: "gemini", FinalVersion: "v1",
	}, st)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if out.Status != workflow.CheckPagesStatusPending {
		t.Fatalf("status = %q, want pending (only earlier-stage record present)", out.Status)
	}
}

func TestCheckPages_RejectsBadInput(t *testing.T) {
	st := store.NewMemoryStore()

	cases := []workflow.CheckPagesInput{
		{},
		{JobID: "j"},
		{JobID: "j", FinalStage: "gemini"},
		{JobID: "j", FinalVersion: "v1"},
		{JobID: "j", FinalStage: "gemini", FinalVersion: "v1", PageCount: -1},
	}
	for i, in := range cases {
		if _, err := workflow.CheckPages(context.Background(), in, st); err == nil {
			t.Fatalf("case %d: expected error for %+v, got nil", i, in)
		}
	}
}
