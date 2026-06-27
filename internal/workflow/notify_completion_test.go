package workflow_test

import (
	"context"
	"errors"
	"testing"

	"github.com/keix/lady-glass/internal/notify"
	"github.com/keix/lady-glass/internal/store"
	"github.com/keix/lady-glass/internal/workflow"
)

// recordingNotifier captures whichever endpoint was invoked. The test
// asserts both the choice (which terminal state dispatched) and the
// payload (what fields rode from JobRecord onto the Notifier call).
type recordingNotifier struct {
	succeeded   *notify.JobSucceeded
	failed      *notify.JobFailed
	succeededErr error
	failedErr    error
}

func (r *recordingNotifier) NotifySucceeded(_ context.Context, job notify.JobSucceeded) error {
	r.succeeded = &job
	return r.succeededErr
}
func (r *recordingNotifier) NotifyFailed(_ context.Context, job notify.JobFailed) error {
	r.failed = &job
	return r.failedErr
}

func TestNotifyCompletion_SucceededDispatchesToNotifySucceeded(t *testing.T) {
	st := store.NewMemoryStore()
	ctx := context.Background()
	if err := st.PutJob(ctx, store.JobRecord{
		JobID:     "j_ok",
		Status:    store.JobStatusSucceeded,
		ChainID:   "card-statement-v1",
		ResultURI: "s3://bkt/jobs/j_ok/merged/v1/result.json",
		PageCount: 3,
		UpdatedAt: "2026-06-27T12:00:00Z",
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	n := &recordingNotifier{}
	if _, err := workflow.NotifyCompletion(ctx, workflow.NotifyCompletionInput{
		JobID: "j_ok",
	}, st, n); err != nil {
		t.Fatalf("notify_completion: %v", err)
	}
	if n.succeeded == nil {
		t.Fatal("NotifySucceeded was not invoked on a succeeded JobRecord")
	}
	if n.failed != nil {
		t.Fatal("NotifyFailed was invoked on a succeeded JobRecord")
	}
	if n.succeeded.JobID != "j_ok" || n.succeeded.ChainID != "card-statement-v1" {
		t.Fatalf("payload identity wrong: %+v", n.succeeded)
	}
	if n.succeeded.ResultURI != "s3://bkt/jobs/j_ok/merged/v1/result.json" {
		t.Fatalf("ResultURI not forwarded: %q", n.succeeded.ResultURI)
	}
	if n.succeeded.PageCount != 3 {
		t.Fatalf("PageCount = %d, want 3", n.succeeded.PageCount)
	}
	if n.succeeded.SucceededAt.IsZero() {
		t.Fatal("SucceededAt should parse from JobRecord.UpdatedAt")
	}
}

func TestNotifyCompletion_FailedDispatchesToNotifyFailed(t *testing.T) {
	st := store.NewMemoryStore()
	ctx := context.Background()
	if err := st.PutJob(ctx, store.JobRecord{
		JobID:     "j_fail",
		Status:    store.JobStatusFailed,
		ChainID:   "card-statement-v1",
		PageCount: 3,
		Error:     "one or more page stages reported status=failed",
		UpdatedAt: "2026-06-27T12:34:56Z",
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	n := &recordingNotifier{}
	if _, err := workflow.NotifyCompletion(ctx, workflow.NotifyCompletionInput{
		JobID: "j_fail",
	}, st, n); err != nil {
		t.Fatalf("notify_completion: %v", err)
	}
	if n.failed == nil {
		t.Fatal("NotifyFailed was not invoked on a failed JobRecord")
	}
	if n.succeeded != nil {
		t.Fatal("NotifySucceeded was invoked on a failed JobRecord")
	}
	if n.failed.Error != "one or more page stages reported status=failed" {
		t.Fatalf("error not forwarded: %q", n.failed.Error)
	}
	if n.failed.ChainID != "card-statement-v1" {
		t.Fatalf("ChainID not forwarded: %q", n.failed.ChainID)
	}
}

func TestNotifyCompletion_RejectsNonTerminalStatus(t *testing.T) {
	st := store.NewMemoryStore()
	ctx := context.Background()
	if err := st.PutJob(ctx, store.JobRecord{
		JobID:  "j_running",
		Status: store.JobStatusRunning,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	_, err := workflow.NotifyCompletion(ctx, workflow.NotifyCompletionInput{
		JobID: "j_running",
	}, st, notify.NoOp{})
	if err == nil {
		t.Fatal("expected error for non-terminal status; got nil")
	}
}

func TestNotifyCompletion_RejectsMissingJob(t *testing.T) {
	st := store.NewMemoryStore()
	_, err := workflow.NotifyCompletion(context.Background(), workflow.NotifyCompletionInput{
		JobID: "j_missing",
	}, st, notify.NoOp{})
	if err == nil {
		t.Fatal("expected error for missing JobRecord; got nil")
	}
}

func TestNotifyCompletion_RejectsEmptyJobID(t *testing.T) {
	st := store.NewMemoryStore()
	_, err := workflow.NotifyCompletion(context.Background(), workflow.NotifyCompletionInput{}, st, notify.NoOp{})
	if err == nil {
		t.Fatal("expected error for empty job_id; got nil")
	}
}

func TestNotifyCompletion_NotifierErrorPropagates(t *testing.T) {
	st := store.NewMemoryStore()
	ctx := context.Background()
	if err := st.PutJob(ctx, store.JobRecord{
		JobID:  "j_ok",
		Status: store.JobStatusSucceeded,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	n := &recordingNotifier{succeededErr: errors.New("webhook timeout")}
	_, err := workflow.NotifyCompletion(ctx, workflow.NotifyCompletionInput{
		JobID: "j_ok",
	}, st, n)
	if err == nil {
		t.Fatal("expected notifier error to propagate so SFN can retry; got nil")
	}
}
