package executor_test

import (
	"context"
	"errors"
	"testing"

	"github.com/keix/lady-glass/internal/stage/ai/gemini"
	"github.com/keix/lady-glass/internal/stage/mockstep"
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
	step := &mockstep.Step{Objects: objects, Calls: &calls}

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
		t.Fatalf("Step.Run was called %d times, want 1", calls)
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
	step := &mockstep.Step{Objects: objects, Calls: &calls}
	next := &pipeline.StageSpec{Name: "gemini", Version: "v1", QueueName: "gemini"}

	failing := &queue.Failing{Err: errors.New("simulated send failure")}
	ex := &executor.Executor{Step: step, NextStage: next, Store: st, Queue: failing}

	in := pipeline.StepInput{JobID: "j_retry", Page: 1}

	if err := ex.Execute(ctx, in); err == nil {
		t.Fatal("expected error from failing queue, got nil")
	}
	if calls != 1 {
		t.Fatalf("Step.Run was called %d times after first attempt, want 1", calls)
	}

	good := queue.NewMemoryQueue()
	ex.Queue = good

	if err := ex.Execute(ctx, in); err != nil {
		t.Fatalf("retry execute: %v", err)
	}
	if calls != 1 {
		t.Fatalf("Step.Run was called %d times after retry, want 1", calls)
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
	step := &mockstep.Step{Objects: objects, Calls: &calls}

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
		t.Fatal("next PreviousURI should be set to the mock ResultURI")
	}
}

func TestExecutor_PrefersInboundChainOverEnvNextStage(t *testing.T) {
	// When the inbound message carries a Chain, Executor MUST route
	// to Chain[ChainIdx+1] instead of e.NextStage. This is the §S10
	// compute-binding contract: the job's frozen chain rides on the
	// SQS message and the env-driven fallback only kicks in for
	// legacy messages that predate this feature.
	ctx := context.Background()
	objects := object.NewFileStore(t.TempDir())
	st := store.NewMemoryStore()
	q := queue.NewMemoryQueue()

	calls := 0
	step := &mockstep.Step{Objects: objects, Calls: &calls}

	ex := &executor.Executor{
		Step: step,
		// Env-driven fallback points at a queue that the test will
		// fail if the Executor mistakenly uses it.
		NextStage: &pipeline.StageSpec{Name: "env_wrong", Version: "v1", QueueName: "env_wrong"},
		Store:     st,
		Queue:     q,
	}

	in := pipeline.StepInput{
		JobID:    "j_chain",
		Page:     1,
		InputURI: "s3://bkt/in.png",
		Chain: []pipeline.StageSpec{
			{Name: "mock", Version: "v1", QueueName: "mock"},
			{Name: "normalize_paypay_statement", Version: "v1", QueueName: "normalize_paypay_statement"},
		},
		ChainIdx: 0,
	}
	if err := ex.Execute(ctx, in); err != nil {
		t.Fatalf("execute: %v", err)
	}

	if _, leaked := q.Messages["env_wrong"]; leaked {
		t.Fatalf("Executor routed to env fallback when message had a Chain: %+v", q.Messages)
	}
	msgs := q.Messages["normalize_paypay_statement"]
	if len(msgs) != 1 {
		t.Fatalf("normalize_paypay_statement messages = %d, want 1", len(msgs))
	}
	if msgs[0].ChainIdx != 1 {
		t.Fatalf("forwarded ChainIdx = %d, want 1 (advanced)", msgs[0].ChainIdx)
	}
	if len(msgs[0].Chain) != 2 {
		t.Fatalf("forwarded Chain length = %d, want 2 (preserved)", len(msgs[0].Chain))
	}
}

func TestExecutor_TerminalChainStageDoesNotEnqueue(t *testing.T) {
	// At the last position in the chain, the Executor MUST NOT
	// enqueue anywhere, even if e.NextStage is configured. This is
	// the read-side mirror of the routing rule: terminal stages on
	// the chain are terminal regardless of the Lambda's env.
	ctx := context.Background()
	objects := object.NewFileStore(t.TempDir())
	st := store.NewMemoryStore()
	q := queue.NewMemoryQueue()

	calls := 0
	step := &mockstep.Step{Objects: objects, Calls: &calls}

	ex := &executor.Executor{
		Step:      step,
		NextStage: &pipeline.StageSpec{Name: "env_wrong", Version: "v1", QueueName: "env_wrong"},
		Store:     st,
		Queue:     q,
	}

	in := pipeline.StepInput{
		JobID: "j_terminal",
		Page:  1,
		Chain: []pipeline.StageSpec{
			{Name: "mock", Version: "v1", QueueName: "mock"},
		},
		ChainIdx: 0, // single-stage chain, this is the terminal index
	}
	if err := ex.Execute(ctx, in); err != nil {
		t.Fatalf("execute: %v", err)
	}

	if len(q.Messages) != 0 {
		t.Fatalf("terminal chain stage enqueued anyway: %+v", q.Messages)
	}
}

// flakyStep fails on the first Run, then succeeds on subsequent calls.
// Used to verify the failed → retry → succeeded state transition.
type flakyStep struct {
	calls int
}

func (s *flakyStep) Name() string    { return "mock" }
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

	rec, err := st.GetStage(ctx, "j_flaky", 1, "mock", "v1")
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

	rec, err = st.GetStage(ctx, "j_flaky", 1, "mock", "v1")
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
