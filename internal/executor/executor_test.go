package executor_test

import (
	"context"
	"errors"
	"testing"

	"github.com/keix/lady-glass/internal/ai/gemini"
	"github.com/keix/lady-glass/internal/ai/lineocr"
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
