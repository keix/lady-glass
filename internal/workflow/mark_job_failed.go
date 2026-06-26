package workflow

import (
	"context"
	"fmt"

	"github.com/keix/lady-glass/internal/store"
)

// MarkJobFailedInput is the SFN task payload for the failure terminal.
type MarkJobFailedInput struct {
	JobID string `json:"job_id"`
	Error string `json:"error,omitempty"`
}

// MarkJobFailedOutput is intentionally tiny — Step Functions records the
// state machine's final output and operators just need the job id.
type MarkJobFailedOutput struct {
	JobID string `json:"job_id"`
}

// MarkJobFailed is the workflow's failure terminal. It overwrites the
// JobRecord with status=failed and (when provided) an error message,
// preserving InputURI and PageCount from any existing row so the
// finalised record stays useful for operators inspecting the job
// post-mortem.
//
// Idempotent: re-running it on the same JobID is a no-op beyond the
// updated_at refresh, and it tolerates a missing pre-existing record
// (in which case InputURI / PageCount stay zero-valued).
func MarkJobFailed(ctx context.Context, in MarkJobFailedInput, st store.Store) (MarkJobFailedOutput, error) {
	if in.JobID == "" {
		return MarkJobFailedOutput{}, fmt.Errorf("mark_job_failed: empty job_id")
	}

	existing, err := st.GetJob(ctx, in.JobID)
	if err != nil {
		return MarkJobFailedOutput{}, fmt.Errorf("mark_job_failed: get job: %w", err)
	}

	job := store.JobRecord{
		JobID:  in.JobID,
		Status: store.JobStatusFailed,
		Error:  in.Error,
	}
	if existing != nil {
		job.InputURI = existing.InputURI
		job.PageCount = existing.PageCount
		job.Mode = existing.Mode
	}
	if err := st.PutJob(ctx, job); err != nil {
		return MarkJobFailedOutput{}, fmt.Errorf("mark_job_failed: put job: %w", err)
	}

	return MarkJobFailedOutput{JobID: in.JobID}, nil
}
