// Package workflow contains the document-level operations invoked by the
// Step Functions state machine (SubmitPages, CheckPages, Merge). Each one
// is a small pure function over Store / Queue / ObjectStore — the Lambda
// binaries are thin SFN-task adapters that decode the input, call here,
// and encode the output.
package workflow

import (
	"context"
	"fmt"

	"github.com/keix/lady-glass/internal/pipeline"
	"github.com/keix/lady-glass/internal/queue"
	"github.com/keix/lady-glass/internal/store"
)

// SubmitPagesInput is the SFN task payload for the SubmitPages step.
type SubmitPagesInput struct {
	// JobID identifies the document being processed.
	JobID string `json:"job_id"`
	// InputURI points at the original document (typically the source PDF
	// uploaded to S3); persisted on the JobRecord for traceability.
	InputURI string `json:"input_uri"`
	// Pages holds one InputURI per page in document order. The page
	// number is derived from the slice index (1-based); page N is at
	// Pages[N-1].
	Pages []string `json:"pages"`
	// Chain is the job's frozen ChainSpec (SPEC §S10), projected here
	// by the API at startJob and forwarded by the SFN ASL. SubmitPages
	// enqueues each page into Chain[0].QueueName with the chain
	// embedded in the StepInput, so every downstream Lambda routes
	// based on this list rather than its own env. Empty Chain is
	// rejected — the API populates it from the JobRecord on every
	// job.
	Chain []pipeline.StageSpec `json:"chain"`
}

// SubmitPagesOutput is the SFN task result. Step Functions uses
// PageCount in the subsequent Wait / CheckPages tasks.
type SubmitPagesOutput struct {
	JobID     string `json:"job_id"`
	PageCount int    `json:"page_count"`
}

// SubmitPages records the JobRecord and fans one StepInput per page out
// to the first stage queue. It is safe to re-invoke: PutJob overwrites
// the job row, and any already-succeeded stage record short-circuits on
// the next-stage Executor side. The cost of re-invocation is therefore
// the SQS Send calls themselves — no external API will be re-billed.
func SubmitPages(ctx context.Context, in SubmitPagesInput, st store.Store, q queue.Queue) (SubmitPagesOutput, error) {
	if in.JobID == "" {
		return SubmitPagesOutput{}, fmt.Errorf("submit_pages: empty job_id")
	}
	if len(in.Chain) == 0 {
		return SubmitPagesOutput{}, fmt.Errorf("submit_pages: empty chain (SPEC §S10 requires the job's ChainSpec on every submit)")
	}

	pageCount := len(in.Pages)
	first := in.Chain[0]

	existing, err := st.GetJob(ctx, in.JobID)
	if err != nil {
		return SubmitPagesOutput{}, fmt.Errorf("submit_pages: get job: %w", err)
	}

	job := store.JobRecord{
		JobID:     in.JobID,
		Status:    store.JobStatusRunning,
		InputURI:  in.InputURI,
		PageCount: pageCount,
	}
	if existing != nil {
		job.Mode = existing.Mode
		job.ChainID = existing.ChainID
		job.Chain = existing.Chain
	}
	if err := st.PutJob(ctx, job); err != nil {
		return SubmitPagesOutput{}, fmt.Errorf("submit_pages: put job: %w", err)
	}

	for i, pageURI := range in.Pages {
		page := i + 1
		msg := pipeline.StepInput{
			JobID:    in.JobID,
			Page:     page,
			Stage:    first.Name,
			Version:  first.Version,
			InputURI: pageURI,
			Chain:    in.Chain,
			ChainIdx: 0,
		}
		if err := q.Send(ctx, first.QueueName, msg); err != nil {
			return SubmitPagesOutput{}, fmt.Errorf("submit_pages: enqueue page %d: %w", page, err)
		}
	}

	return SubmitPagesOutput{
		JobID:     in.JobID,
		PageCount: pageCount,
	}, nil
}
