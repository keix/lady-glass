package workflow_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/keix/lady-glass/internal/object"
	"github.com/keix/lady-glass/internal/pipeline"
	"github.com/keix/lady-glass/internal/store"
	"github.com/keix/lady-glass/internal/workflow"
)

// seedSucceededPage writes a per-page result.json to the file store and
// records the resulting URI on a succeeded stage record.
func seedSucceededPage(t *testing.T, st store.Store, obj object.Store, jobID string, page int, body map[string]any) {
	t.Helper()
	ctx := context.Background()
	resultURI, err := obj.PutJSON(ctx, jobIDKey(jobID, page), body)
	if err != nil {
		t.Fatalf("seed page %d body: %v", page, err)
	}
	if err := st.MarkSucceeded(ctx, pipeline.StepOutput{
		JobID:     jobID,
		Page:      page,
		Stage:     "gemini",
		Version:   "v1",
		ResultURI: resultURI,
	}, ""); err != nil {
		t.Fatalf("seed page %d record: %v", page, err)
	}
}

func jobIDKey(jobID string, page int) string {
	return fmt.Sprintf("jobs/%s/pages/%06d/gemini/v1/result.json", jobID, page)
}

func TestMerge_WritesMergedDocumentAndUpdatesJob(t *testing.T) {
	st := store.NewMemoryStore()
	obj := object.NewFileStore(t.TempDir())
	ctx := context.Background()

	// Pre-existing JobRecord from SubmitPages.
	if err := st.PutJob(ctx, store.JobRecord{
		JobID:     "j_merge",
		Status:    store.JobStatusRunning,
		InputURI:  "s3://bkt/jobs/j_merge/input.pdf",
		PageCount: 3,
		Mode:      "rendered",
		ChainID:   "test-chain",
		Chain: []pipeline.StageSpec{
			{Name: "gemini", Version: "v1", QueueName: "gemini-q"},
		},
	}); err != nil {
		t.Fatalf("seed job: %v", err)
	}

	seedSucceededPage(t, st, obj, "j_merge", 1, map[string]any{"text": "page one", "page_num": 1})
	seedSucceededPage(t, st, obj, "j_merge", 2, map[string]any{"text": "page two", "page_num": 2})
	seedSucceededPage(t, st, obj, "j_merge", 3, map[string]any{"text": "page three", "page_num": 3})

	out, err := workflow.Merge(ctx, workflow.MergeInput{
		JobID: "j_merge", PageCount: 3, FinalStage: "gemini", FinalVersion: "v1",
	}, st, obj)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}

	if out.MergedResultURI == "" {
		t.Fatal("merged_result_uri is empty")
	}

	body, err := obj.Get(ctx, out.MergedResultURI)
	if err != nil {
		t.Fatalf("read merged: %v", err)
	}
	var merged workflow.MergedDocument
	if err := json.Unmarshal(body, &merged); err != nil {
		t.Fatalf("decode merged: %v", err)
	}
	if merged.JobID != "j_merge" {
		t.Fatalf("merged.JobID = %q", merged.JobID)
	}
	if merged.PageCount != 3 {
		t.Fatalf("merged.PageCount = %d, want 3", merged.PageCount)
	}
	if len(merged.Pages) != 3 {
		t.Fatalf("merged.Pages = %d entries, want 3", len(merged.Pages))
	}
	for i, p := range merged.Pages {
		wantPage := i + 1
		if p.Page != wantPage {
			t.Fatalf("merged.Pages[%d].Page = %d, want %d (order)", i, p.Page, wantPage)
		}
		var inner map[string]any
		if err := json.Unmarshal(p.Result, &inner); err != nil {
			t.Fatalf("decode embedded page %d: %v", wantPage, err)
		}
		if got, _ := inner["page_num"].(float64); int(got) != wantPage {
			t.Fatalf("embedded page %d page_num = %v", wantPage, inner["page_num"])
		}
	}

	// Job is finalised.
	rec, err := st.GetJob(ctx, "j_merge")
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if rec.Status != store.JobStatusSucceeded {
		t.Fatalf("job status = %q, want succeeded", rec.Status)
	}
	if rec.ResultURI != out.MergedResultURI {
		t.Fatalf("job ResultURI = %q, want %q", rec.ResultURI, out.MergedResultURI)
	}
	if rec.InputURI != "s3://bkt/jobs/j_merge/input.pdf" {
		t.Fatalf("InputURI was lost from JobRecord: %q", rec.InputURI)
	}
	if rec.Mode != "rendered" {
		t.Fatalf("Mode was lost from JobRecord: %q", rec.Mode)
	}
	if rec.ChainID != "test-chain" || len(rec.Chain) != 1 || rec.Chain[0].Name != "gemini" {
		t.Fatalf("Chain binding was lost from JobRecord: id=%q chain=%+v", rec.ChainID, rec.Chain)
	}
}

func TestMerge_MissingPageStageRecordIsAnError(t *testing.T) {
	st := store.NewMemoryStore()
	obj := object.NewFileStore(t.TempDir())
	seedSucceededPage(t, st, obj, "j", 1, map[string]any{"text": "ok"})

	_, err := workflow.Merge(context.Background(), workflow.MergeInput{
		JobID: "j", PageCount: 3, FinalStage: "gemini", FinalVersion: "v1",
	}, st, obj)
	if err == nil {
		t.Fatal("expected error when stage records are missing, got nil")
	}
	if !strings.Contains(err.Error(), "of 3 pages") {
		t.Fatalf("error %q does not mention page-count gap", err)
	}
}

func TestMerge_NonSucceededPageIsAnError(t *testing.T) {
	st := store.NewMemoryStore()
	obj := object.NewFileStore(t.TempDir())

	// Page 1 succeeded properly.
	seedSucceededPage(t, st, obj, "j", 1, map[string]any{"text": "ok"})
	// Page 2 ended up running (e.g. CheckPages incorrectly let us through).
	if err := st.MarkRunning(context.Background(), pipeline.StepInput{
		JobID: "j", Page: 2, Stage: "gemini", Version: "v1",
	}); err != nil {
		t.Fatalf("seed running: %v", err)
	}

	_, err := workflow.Merge(context.Background(), workflow.MergeInput{
		JobID: "j", PageCount: 2, FinalStage: "gemini", FinalVersion: "v1",
	}, st, obj)
	if err == nil {
		t.Fatal("expected error when a page is not succeeded, got nil")
	}
	if !strings.Contains(err.Error(), "page 2") {
		t.Fatalf("error %q does not name the offending page", err)
	}
}

func TestMerge_ObjectReadErrorIsWrapped(t *testing.T) {
	st := store.NewMemoryStore()
	obj := object.NewFileStore(t.TempDir())

	// Record claims success with a pointer that does not resolve in the
	// object store.
	if err := st.MarkSucceeded(context.Background(), pipeline.StepOutput{
		JobID: "j", Page: 1, Stage: "gemini", Version: "v1",
		ResultURI: "file:///definitely/does/not/exist.json",
	}, ""); err != nil {
		t.Fatalf("seed: %v", err)
	}

	_, err := workflow.Merge(context.Background(), workflow.MergeInput{
		JobID: "j", PageCount: 1, FinalStage: "gemini", FinalVersion: "v1",
	}, st, obj)
	if err == nil {
		t.Fatal("expected wrapped object read error, got nil")
	}
	if !strings.Contains(err.Error(), "read page 1 result") {
		t.Fatalf("error %q does not name the failing read", err)
	}
}

func TestMerge_PreservesInputURIFromExistingJob(t *testing.T) {
	st := store.NewMemoryStore()
	obj := object.NewFileStore(t.TempDir())
	ctx := context.Background()

	if err := st.PutJob(ctx, store.JobRecord{
		JobID:    "j_keep",
		Status:   store.JobStatusRunning,
		InputURI: "s3://bkt/jobs/j_keep/source.pdf",
	}); err != nil {
		t.Fatalf("seed job: %v", err)
	}
	seedSucceededPage(t, st, obj, "j_keep", 1, map[string]any{"text": "x"})

	if _, err := workflow.Merge(ctx, workflow.MergeInput{
		JobID: "j_keep", PageCount: 1, FinalStage: "gemini", FinalVersion: "v1",
	}, st, obj); err != nil {
		t.Fatalf("merge: %v", err)
	}

	rec, err := st.GetJob(ctx, "j_keep")
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if rec.InputURI != "s3://bkt/jobs/j_keep/source.pdf" {
		t.Fatalf("input_uri was not preserved through Merge: %q", rec.InputURI)
	}
}

func TestMerge_PreservesModeFromExistingJob(t *testing.T) {
	st := store.NewMemoryStore()
	obj := object.NewFileStore(t.TempDir())
	ctx := context.Background()

	if err := st.PutJob(ctx, store.JobRecord{
		JobID:  "j_mode",
		Status: store.JobStatusRunning,
		Mode:   "rendered",
	}); err != nil {
		t.Fatalf("seed job: %v", err)
	}
	seedSucceededPage(t, st, obj, "j_mode", 1, map[string]any{"text": "x"})

	if _, err := workflow.Merge(ctx, workflow.MergeInput{
		JobID: "j_mode", PageCount: 1, FinalStage: "gemini", FinalVersion: "v1",
	}, st, obj); err != nil {
		t.Fatalf("merge: %v", err)
	}

	rec, err := st.GetJob(ctx, "j_mode")
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if rec.Mode != "rendered" {
		t.Fatalf("mode was not preserved through Merge: %q", rec.Mode)
	}
}
