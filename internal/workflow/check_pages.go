package workflow

import (
	"context"
	"fmt"

	"github.com/keix/lady-glass/internal/store"
)

// CheckPagesStatus is the high-level job state CheckPages returns to the
// Step Functions Choice state. The state machine routes on this string.
type CheckPagesStatus string

const (
	CheckPagesStatusPending   CheckPagesStatus = "pending"
	CheckPagesStatusSucceeded CheckPagesStatus = "succeeded"
	CheckPagesStatusFailed    CheckPagesStatus = "failed"
)

// CheckPagesInput is the SFN task payload for CheckPages.
type CheckPagesInput struct {
	// JobID and PageCount come through the workflow from SubmitPages.
	JobID     string `json:"job_id"`
	PageCount int    `json:"page_count"`
	// FinalStage / FinalVersion identify the last stage in the chain —
	// the one whose StageRecord status determines per-page completion
	// (e.g. {gemini, v1}).
	FinalStage   string `json:"final_stage"`
	FinalVersion string `json:"final_version"`
}

// CheckPagesOutput aggregates per-page stage statuses into the
// document-level decision.
type CheckPagesOutput struct {
	JobID     string           `json:"job_id"`
	Status    CheckPagesStatus `json:"status"`
	Succeeded int              `json:"succeeded"`
	Failed    int              `json:"failed"`
	Pending   int              `json:"pending"`
}

// CheckPages reads every StageRecord for (JobID, FinalStage, FinalVersion)
// and aggregates them into a single document-level decision:
//
//   - any record with status=failed → CheckPagesStatusFailed (fail-fast)
//   - every page accounted for and succeeded → CheckPagesStatusSucceeded
//   - otherwise → CheckPagesStatusPending (some pages still running, or
//     no record yet)
//
// CheckPages is read-only: it does NOT mutate the JobRecord or any stage
// rows. Status transitions on the job row are owned by Merge (on success)
// and the workflow's failure terminal (on failed).
func CheckPages(ctx context.Context, in CheckPagesInput, st store.Store) (CheckPagesOutput, error) {
	if in.JobID == "" {
		return CheckPagesOutput{}, fmt.Errorf("check_pages: empty job_id")
	}
	if in.PageCount < 0 {
		return CheckPagesOutput{}, fmt.Errorf("check_pages: negative page_count %d", in.PageCount)
	}
	if in.FinalStage == "" || in.FinalVersion == "" {
		return CheckPagesOutput{}, fmt.Errorf("check_pages: final_stage / final_version are required")
	}

	recs, err := st.ListStagesByJob(ctx, in.JobID, in.FinalStage, in.FinalVersion)
	if err != nil {
		return CheckPagesOutput{}, fmt.Errorf("check_pages: list stages: %w", err)
	}

	out := CheckPagesOutput{JobID: in.JobID}
	for _, r := range recs {
		switch r.Status {
		case store.StageStatusSucceeded:
			out.Succeeded++
		case store.StageStatusFailed:
			out.Failed++
		}
	}
	out.Pending = in.PageCount - out.Succeeded - out.Failed
	if out.Pending < 0 {
		out.Pending = 0
	}

	switch {
	case out.Failed > 0:
		out.Status = CheckPagesStatusFailed
	case out.Succeeded == in.PageCount && in.PageCount > 0:
		out.Status = CheckPagesStatusSucceeded
	default:
		out.Status = CheckPagesStatusPending
	}

	return out, nil
}
