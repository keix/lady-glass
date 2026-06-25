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
	// FirstQueue is the logical queue name of the first stage in the
	// chain (matches a key in SQSQueue.URLs). The page messages are
	// enqueued here.
	FirstQueue string `json:"first_queue"`
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
	if in.FirstQueue == "" {
		return SubmitPagesOutput{}, fmt.Errorf("submit_pages: empty first_queue")
	}

	pageCount := len(in.Pages)

	if err := st.PutJob(ctx, store.JobRecord{
		JobID:     in.JobID,
		Status:    store.JobStatusRunning,
		InputURI:  in.InputURI,
		PageCount: pageCount,
	}); err != nil {
		return SubmitPagesOutput{}, fmt.Errorf("submit_pages: put job: %w", err)
	}

	for i, pageURI := range in.Pages {
		page := i + 1
		msg := pipeline.StepInput{
			JobID:    in.JobID,
			Page:     page,
			InputURI: pageURI,
		}
		if err := q.Send(ctx, in.FirstQueue, msg); err != nil {
			return SubmitPagesOutput{}, fmt.Errorf("submit_pages: enqueue page %d: %w", page, err)
		}
	}

	return SubmitPagesOutput{
		JobID:     in.JobID,
		PageCount: pageCount,
	}, nil
}
