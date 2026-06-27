package workflow

import (
	"context"
	"fmt"
	"time"

	"github.com/keix/lady-glass/internal/notify"
	"github.com/keix/lady-glass/internal/store"
)

// NotifyCompletionInput is the SFN task payload. Only the job_id is
// passed in; the workflow reads the JobRecord to discover which
// terminal state was committed and what payload to hand the Notifier.
// Keeping the input minimal makes the same Lambda usable from both
// the Merge → NotifyCompletion edge and the MarkJobFailed →
// NotifyCompletion edge without any ASL branching.
type NotifyCompletionInput struct {
	JobID string `json:"job_id"`
}

// NotifyCompletionOutput is the SFN task result. SFn records it on the
// execution; operators read it to confirm which job was notified.
type NotifyCompletionOutput struct {
	JobID string `json:"job_id"`
}

// NotifyCompletion is the post-commit observer step (SPEC §S11). The
// JobRecord's Status is the source of truth for which terminal state
// was committed and therefore which Notifier endpoint to invoke:
//
//	succeeded → Notifier.NotifySucceeded
//	failed    → Notifier.NotifyFailed
//
// Anything else is a contract violation (the function is meant to run
// strictly after Merge or MarkJobFailed) and surfaces as an error.
//
// NotifyCompletion is the only workflow step that intentionally
// observes — and never mutates — the JobRecord. A Notifier failure is
// propagated to SFN so the orchestrator can retry the step. SFN's
// Catch handler on the ASL state ends the execution if retries are
// exhausted, and because this function never writes anything, the
// committed terminal state is preserved unchanged.
func NotifyCompletion(ctx context.Context, in NotifyCompletionInput, st store.Store, n notify.Notifier) (NotifyCompletionOutput, error) {
	if in.JobID == "" {
		return NotifyCompletionOutput{}, fmt.Errorf("notify_completion: empty job_id")
	}
	rec, err := st.GetJob(ctx, in.JobID)
	if err != nil {
		return NotifyCompletionOutput{}, fmt.Errorf("notify_completion: get job: %w", err)
	}
	if rec == nil {
		return NotifyCompletionOutput{}, fmt.Errorf("notify_completion: job %q does not exist", in.JobID)
	}

	updated := parseRFC3339OrZero(rec.UpdatedAt)

	switch rec.Status {
	case store.JobStatusSucceeded:
		if err := n.NotifySucceeded(ctx, notify.JobSucceeded{
			JobID:       rec.JobID,
			ChainID:     rec.ChainID,
			ResultURI:   rec.ResultURI,
			PageCount:   rec.PageCount,
			SucceededAt: updated,
		}); err != nil {
			return NotifyCompletionOutput{}, fmt.Errorf("notify_completion: notify succeeded: %w", err)
		}
	case store.JobStatusFailed:
		if err := n.NotifyFailed(ctx, notify.JobFailed{
			JobID:     rec.JobID,
			ChainID:   rec.ChainID,
			PageCount: rec.PageCount,
			Error:     rec.Error,
			FailedAt:  updated,
		}); err != nil {
			return NotifyCompletionOutput{}, fmt.Errorf("notify_completion: notify failed: %w", err)
		}
	default:
		return NotifyCompletionOutput{}, fmt.Errorf("notify_completion: job %q is %s, want succeeded or failed", in.JobID, rec.Status)
	}

	return NotifyCompletionOutput{JobID: in.JobID}, nil
}

func parseRFC3339OrZero(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}
	}
	return t
}
