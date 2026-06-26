package workflow_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/keix/lady-glass/internal/pipeline"
	"github.com/keix/lady-glass/internal/queue"
	"github.com/keix/lady-glass/internal/store"
	"github.com/keix/lady-glass/internal/workflow"
)

func TestSubmitPages_FansOutOneMessagePerPage(t *testing.T) {
	st := store.NewMemoryStore()
	q := queue.NewMemoryQueue()

	in := workflow.SubmitPagesInput{
		JobID:      "j1",
		InputURI:   "s3://bkt/jobs/j1/input.pdf",
		Chain: []pipeline.StageSpec{{Name: "gemini", Version: "v1", QueueName: "gemini"}},
		Pages: []string{
			"s3://bkt/jobs/j1/pages/000001/input.png",
			"s3://bkt/jobs/j1/pages/000002/input.png",
			"s3://bkt/jobs/j1/pages/000003/input.png",
		},
	}

	out, err := workflow.SubmitPages(context.Background(), in, st, q)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if out.JobID != "j1" || out.PageCount != 3 {
		t.Fatalf("output = %+v, want {JobID: j1, PageCount: 3}", out)
	}

	msgs := q.Messages["gemini"]
	if len(msgs) != 3 {
		t.Fatalf("queue messages = %d, want 3", len(msgs))
	}
	for i, msg := range msgs {
		wantPage := i + 1
		if msg.JobID != "j1" {
			t.Fatalf("message %d job_id = %q, want j1", i, msg.JobID)
		}
		if msg.Page != wantPage {
			t.Fatalf("message %d page = %d, want %d", i, msg.Page, wantPage)
		}
		if msg.InputURI != in.Pages[i] {
			t.Fatalf("message %d input_uri = %q, want %q", i, msg.InputURI, in.Pages[i])
		}
	}
}

func TestSubmitPages_RecordsJobRunningWithPageCount(t *testing.T) {
	st := store.NewMemoryStore()
	q := queue.NewMemoryQueue()

	in := workflow.SubmitPagesInput{
		JobID:      "j_job",
		InputURI:   "s3://bkt/jobs/j_job/input.pdf",
		Chain: []pipeline.StageSpec{{Name: "gemini", Version: "v1", QueueName: "gemini"}},
		Pages:      []string{"a.png", "b.png"},
	}

	if _, err := workflow.SubmitPages(context.Background(), in, st, q); err != nil {
		t.Fatalf("submit: %v", err)
	}

	rec, err := st.GetJob(context.Background(), "j_job")
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if rec == nil {
		t.Fatal("expected job record, got nil")
	}
	if rec.Status != store.JobStatusRunning {
		t.Fatalf("status = %q, want running", rec.Status)
	}
	if rec.PageCount != 2 {
		t.Fatalf("page count = %d, want 2", rec.PageCount)
	}
	if rec.InputURI != in.InputURI {
		t.Fatalf("input_uri = %q", rec.InputURI)
	}
}

func TestSubmitPages_PreservesModeFromExistingJob(t *testing.T) {
	st := store.NewMemoryStore()
	q := queue.NewMemoryQueue()
	ctx := context.Background()

	if err := st.PutJob(ctx, store.JobRecord{
		JobID:    "j_rendered",
		Status:   store.JobStatusCreated,
		Mode:     "rendered",
		InputURI: "s3://bkt/jobs/j_rendered/input.pdf",
	}); err != nil {
		t.Fatalf("seed job: %v", err)
	}

	if _, err := workflow.SubmitPages(ctx, workflow.SubmitPagesInput{
		JobID:      "j_rendered",
		InputURI:   "s3://bkt/jobs/j_rendered/input.pdf",
		Chain: []pipeline.StageSpec{{Name: "gemini", Version: "v1", QueueName: "gemini"}},
		Pages:      []string{"page-1.pdf"},
	}, st, q); err != nil {
		t.Fatalf("submit: %v", err)
	}

	rec, err := st.GetJob(ctx, "j_rendered")
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if rec.Mode != "rendered" {
		t.Fatalf("mode = %q, want rendered", rec.Mode)
	}
}

func TestSubmitPages_QueueErrorIsReturned(t *testing.T) {
	st := store.NewMemoryStore()

	in := workflow.SubmitPagesInput{
		JobID:      "j2",
		Chain: []pipeline.StageSpec{{Name: "gemini", Version: "v1", QueueName: "gemini"}},
		Pages:      []string{"a.png", "b.png"},
	}

	failing := &queue.Failing{Err: errors.New("simulated queue failure")}
	_, err := workflow.SubmitPages(context.Background(), in, st, failing)
	if err == nil {
		t.Fatal("expected error from failing queue, got nil")
	}
	if !strings.Contains(err.Error(), "page 1") {
		t.Fatalf("error %q does not name the failing page", err)
	}

	// JobRecord should still be written even if fanout fails — SFn can
	// re-invoke SubmitPages, the JobRecord stays consistent.
	rec, err := st.GetJob(context.Background(), "j2")
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if rec == nil || rec.Status != store.JobStatusRunning {
		t.Fatalf("job record after error = %+v", rec)
	}
}

func TestSubmitPages_EmptyPagesProducesEmptyJob(t *testing.T) {
	st := store.NewMemoryStore()
	q := queue.NewMemoryQueue()

	in := workflow.SubmitPagesInput{
		JobID:      "empty",
		Chain: []pipeline.StageSpec{{Name: "gemini", Version: "v1", QueueName: "gemini"}},
		Pages:      nil,
	}

	out, err := workflow.SubmitPages(context.Background(), in, st, q)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if out.PageCount != 0 {
		t.Fatalf("page count = %d, want 0", out.PageCount)
	}
	if got := len(q.Messages["gemini"]); got != 0 {
		t.Fatalf("queue messages = %d, want 0", got)
	}

	rec, err := st.GetJob(context.Background(), "empty")
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if rec == nil || rec.PageCount != 0 || rec.Status != store.JobStatusRunning {
		t.Fatalf("job record = %+v", rec)
	}
}

func TestSubmitPages_MissingJobIDIsAnError(t *testing.T) {
	st := store.NewMemoryStore()
	q := queue.NewMemoryQueue()

	_, err := workflow.SubmitPages(context.Background(), workflow.SubmitPagesInput{
		Chain: []pipeline.StageSpec{{Name: "gemini", Version: "v1", QueueName: "gemini"}},
		Pages:      []string{"a.png"},
	}, st, q)
	if err == nil {
		t.Fatal("expected error for empty job_id, got nil")
	}
}

// Make sure the unused import in the test file resolves to something real
// so refactors do not silently drop the StepInput shape contract on
// fan-out — this is a compile-only test.
var _ = pipeline.StepInput{}
