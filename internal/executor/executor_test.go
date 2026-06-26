package executor_test

import (
	"context"
	"errors"
	"testing"

	"github.com/keix/lady-glass/internal/stage/ai/gemini"
	"github.com/keix/lady-glass/internal/stage/ai/lineocr"
	"github.com/keix/lady-glass/internal/executor"
	"github.com/keix/lady-glass/internal/object"
	"github.com/keix/lady-glass/internal/pipeline"
	"github.com/keix/lady-glass/internal/queue"
	"github.com/keix/lady-glass/internal/store"
)

func TestExecutor_SucceededStageIsNotReRun(t *testing.T) {
	ctx := context.Background()
	objects := object.NewFileStore(t.TempDir())
	st := store.NewMemoryStore()
	q := queue.NewMemoryQueue()

	calls := 0
	step := &lineocr.Mock{Objects: objects, Calls: &calls}

	ex := &executor.Executor{
		Step:      step,
		NextStage: &pipeline.StageSpec{Name: "gemini", Version: "v1", QueueName: "gemini"},
		Store:     st,
		Queue:     q,
	}

	in := pipeline.StepInput{JobID: "j_skip", Page: 1}

	if err := ex.Execute(ctx, in); err != nil {
		t.Fatalf("first execute: %v", err)
	}
	if err := ex.Execute(ctx, in); err != nil {
		t.Fatalf("second execute: %v", err)
	}

	if calls != 1 {
		t.Fatalf("LineOCR.Run was called %d times, want 1", calls)
	}
	if got := len(q.Messages["gemini"]); got != 2 {
		t.Fatalf("gemini queue size = %d, want 2 (re-enqueue on succeeded skip)", got)
	}
}

func TestExecutor_EnqueueFailureRetriesWithoutReRunningStep(t *testing.T) {
	ctx := context.Background()
	objects := object.NewFileStore(t.TempDir())
	st := store.NewMemoryStore()

	calls := 0
	step := &lineocr.Mock{Objects: objects, Calls: &calls}
	next := &pipeline.StageSpec{Name: "gemini", Version: "v1", QueueName: "gemini"}

	failing := &queue.Failing{Err: errors.New("simulated send failure")}
	ex := &executor.Executor{Step: step, NextStage: next, Store: st, Queue: failing}

	in := pipeline.StepInput{JobID: "j_retry", Page: 1}

	if err := ex.Execute(ctx, in); err == nil {
		t.Fatal("expected error from failing queue, got nil")
	}
	if calls != 1 {
		t.Fatalf("LineOCR.Run was called %d times after first attempt, want 1", calls)
	}

	good := queue.NewMemoryQueue()
	ex.Queue = good

	if err := ex.Execute(ctx, in); err != nil {
		t.Fatalf("retry execute: %v", err)
	}
	if calls != 1 {
		t.Fatalf("LineOCR.Run was called %d times after retry, want 1", calls)
	}
	if got := len(good.Messages["gemini"]); got != 1 {
		t.Fatalf("gemini queue size after retry = %d, want 1", got)
	}
}

func TestExecutor_NextStageMessageCarriesOriginalInputURI(t *testing.T) {
	ctx := context.Background()
	objects := object.NewFileStore(t.TempDir())
	st := store.NewMemoryStore()
	q := queue.NewMemoryQueue()

	calls := 0
	step := &lineocr.Mock{Objects: objects, Calls: &calls}

	ex := &executor.Executor{
		Step:      step,
		NextStage: &pipeline.StageSpec{Name: "gemini", Version: "v1", QueueName: "gemini"},
		Store:     st,
		Queue:     q,
	}

	in := pipeline.StepInput{
		JobID:    "j_propagate",
		Page:     1,
		InputURI: "s3://bkt/jobs/j_propagate/pages/000001/input.png",
	}
	if err := ex.Execute(ctx, in); err != nil {
		t.Fatalf("execute: %v", err)
	}

	msgs := q.Messages["gemini"]
	if len(msgs) != 1 {
		t.Fatalf("gemini messages = %d, want 1", len(msgs))
	}
	if got := msgs[0].InputURI; got != in.InputURI {
		t.Fatalf("next InputURI = %q, want %q", got, in.InputURI)
	}
	if msgs[0].PreviousURI == "" {
		t.Fatal("next PreviousURI should be set to the line_ocr ResultURI")
	}
}

// flakyStep fails on the first Run, then succeeds on subsequent calls.
// Used to verify the failed → retry → succeeded state transition.
type flakyStep struct {
	calls int
}

func (s *flakyStep) Name() string    { return "line_ocr" }
func (s *flakyStep) Version() string { return "v1" }
func (s *flakyStep) Run(_ context.Context, in pipeline.StepInput) (pipeline.StepOutput, error) {
	s.calls++
	if s.calls == 1 {
		return pipeline.StepOutput{}, errors.New("simulated step failure")
	}
	return pipeline.StepOutput{
		JobID:     in.JobID,
		Page:      in.Page,
		Stage:     s.Name(),
		Version:   s.Version(),
		ResultURI: "file://stub-result.json",
	}, nil
}

func TestExecutor_FailedStageIsRerunOnNextDelivery(t *testing.T) {
	ctx := context.Background()
	st := store.NewMemoryStore()
	q := queue.NewMemoryQueue()

	step := &flakyStep{}

	ex := &executor.Executor{
		Step:  step,
		Store: st,
		Queue: q,
	}

	in := pipeline.StepInput{JobID: "j_flaky", Page: 1}

	if err := ex.Execute(ctx, in); err == nil {
		t.Fatal("expected error on first execute, got nil")
	}
	if step.calls != 1 {
		t.Fatalf("step calls after 1st delivery = %d, want 1", step.calls)
	}

	rec, err := st.GetStage(ctx, "j_flaky", 1, "line_ocr", "v1")
	if err != nil {
		t.Fatalf("get stage after 1st: %v", err)
	}
	if rec == nil {
		t.Fatal("stage record missing after MarkFailed")
	}
	if rec.Status != store.StageStatusFailed {
		t.Fatalf("status after 1st = %q, want failed", rec.Status)
	}
	if rec.Error == "" {
		t.Fatal("MarkFailed did not record an error message")
	}

	if err := ex.Execute(ctx, in); err != nil {
		t.Fatalf("expected success on second execute, got %v", err)
	}
	if step.calls != 2 {
		t.Fatalf("step calls after 2nd delivery = %d, want 2", step.calls)
	}

	rec, err = st.GetStage(ctx, "j_flaky", 1, "line_ocr", "v1")
	if err != nil {
		t.Fatalf("get stage after 2nd: %v", err)
	}
	if rec.Status != store.StageStatusSucceeded {
		t.Fatalf("status after 2nd = %q, want succeeded", rec.Status)
	}
	if rec.ResultURI != "file://stub-result.json" {
		t.Fatalf("ResultURI = %q, want file://stub-result.json", rec.ResultURI)
	}
}

func TestExecutor_GeminiSucceededIsNotReRun(t *testing.T) {
	ctx := context.Background()
	objects := object.NewFileStore(t.TempDir())
	st := store.NewMemoryStore()
	q := queue.NewMemoryQueue()

	calls := 0
	step := &gemini.Mock{Objects: objects, Calls: &calls}

	ex := &executor.Executor{Step: step, Store: st, Queue: q}

	in := pipeline.StepInput{JobID: "j_gem_skip", Page: 1, PreviousURI: "file://stub"}

	if err := ex.Execute(ctx, in); err != nil {
		t.Fatalf("first execute: %v", err)
	}
	if err := ex.Execute(ctx, in); err != nil {
		t.Fatalf("second execute: %v", err)
	}

	if calls != 1 {
		t.Fatalf("Gemini.Run was called %d times, want 1", calls)
	}
}
