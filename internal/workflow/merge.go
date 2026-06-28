package workflow

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/keix/lady-glass/internal/object"
	"github.com/keix/lady-glass/internal/store"
)

// MergeInput is the SFN task payload for Merge. It mirrors CheckPagesInput
// because Merge runs after CheckPages has confirmed every page reached the
// final stage with status=succeeded.
type MergeInput struct {
	JobID        string `json:"job_id"`
	PageCount    int    `json:"page_count"`
	FinalStage   string `json:"final_stage"`
	FinalVersion string `json:"final_version"`
}

// MergeOutput carries the URI of the merged document written to the
// object store. Step Functions persists this on its execution result
// and the surrounding workflow can copy it onto the JobRecord.
type MergeOutput struct {
	JobID           string `json:"job_id"`
	MergedResultURI string `json:"merged_result_uri"`
}

// MergedDocument is the shape Merge writes to the object store. The per-
// page result body is embedded as json.RawMessage so the merged document
// is lossless — the structure of each page's JSON is preserved verbatim,
// no fields are flattened, deduplicated, or schema-coerced. Downstream
// consumers (or a follow-up dedicated post-processing stage) can shape
// the merged content for their use case.
type MergedDocument struct {
	JobID     string       `json:"job_id"`
	PageCount int          `json:"page_count"`
	Pages     []MergedPage `json:"pages"`
}

type MergedPage struct {
	Page   int             `json:"page"`
	Result json.RawMessage `json:"result"`
}

// Merge reads every per-page result body from the object store, wraps
// them into a MergedDocument in page order, writes the merged blob back
// to the object store, and updates the JobRecord with status=succeeded
// and the merged URI as ResultURI.
//
// Merge is the workflow's success terminal: it commits success.
//
//	Success is committed by Merge.
//	Failure is committed by MarkJobFailed.
//
// The shape is intentionally asymmetric. MarkJobFailed only has a
// JobRecord to write — there is no artifact, so it stands as its own
// Lambda. Merge already has to write the JobRecord (to record the
// merged ResultURI), so folding `status=succeeded` into the same
// PutJob keeps the success state atomic: the merged blob in S3 and
// the JobRecord's terminal status update land in one logical
// transaction. Splitting Merge into "produce artifact" + a separate
// "mark succeeded" Lambda would create a partial-state window — the
// API would briefly answer GET /jobs/{id}/result with a real result
// while GET /jobs/{id} still reports `running`.
//
// By the time Merge runs, CheckPages has already returned
// CheckPagesStatusSucceeded for the same input, so every page is
// expected to have a non-empty stage record with status=succeeded
// and a populated ResultURI. Missing rows or empty ResultURIs are
// surfaced as errors so SFN can retry or transition to the failure
// terminal.
//
// Merge is idempotent: re-running it overwrites the merged object
// (same key) and re-writes the JobRecord with the same fields, both
// last-writer-wins.
func Merge(ctx context.Context, in MergeInput, st store.Store, obj object.Store) (MergeOutput, error) {
	if in.JobID == "" {
		return MergeOutput{}, fmt.Errorf("merge: empty job_id")
	}
	if in.FinalStage == "" || in.FinalVersion == "" {
		return MergeOutput{}, fmt.Errorf("merge: final_stage / final_version are required")
	}

	recs, err := st.ListStagesByJob(ctx, in.JobID, in.FinalStage, in.FinalVersion)
	if err != nil {
		return MergeOutput{}, fmt.Errorf("merge: list stages: %w", err)
	}
	if in.PageCount > 0 && len(recs) < in.PageCount {
		return MergeOutput{}, fmt.Errorf("merge: only %d of %d pages have a stage record", len(recs), in.PageCount)
	}

	pages := make([]MergedPage, 0, len(recs))
	for _, r := range recs {
		if r.Status != store.StageStatusSucceeded {
			return MergeOutput{}, fmt.Errorf("merge: page %d has status %q, want succeeded", r.Page, r.Status)
		}
		if r.ResultURI == "" {
			return MergeOutput{}, fmt.Errorf("merge: page %d has empty result_uri", r.Page)
		}
		body, err := obj.Get(ctx, r.ResultURI)
		if err != nil {
			return MergeOutput{}, fmt.Errorf("merge: read page %d result: %w", r.Page, err)
		}
		pages = append(pages, MergedPage{
			Page:   r.Page,
			Result: json.RawMessage(body),
		})
	}

	merged := MergedDocument{
		JobID:     in.JobID,
		PageCount: len(pages),
		Pages:     pages,
	}
	mergedBody, err := json.Marshal(merged)
	if err != nil {
		return MergeOutput{}, fmt.Errorf("merge: marshal merged document: %w", err)
	}

	key := fmt.Sprintf("jobs/%s/merged/v1/result.json", in.JobID)
	mergedURI, err := obj.PutBytes(ctx, key, mergedBody, "application/json")
	if err != nil {
		return MergeOutput{}, fmt.Errorf("merge: write merged document: %w", err)
	}

	existing, err := st.GetJob(ctx, in.JobID)
	if err != nil {
		return MergeOutput{}, fmt.Errorf("merge: get job: %w", err)
	}
	job := store.JobRecord{
		JobID:     in.JobID,
		Status:    store.JobStatusSucceeded,
		ResultURI: mergedURI,
		PageCount: len(pages),
	}
	if existing != nil {
		job.InputURI = existing.InputURI
		job.Mode = existing.Mode
		job.ChainID = existing.ChainID
		job.Chain = existing.Chain
	}
	if err := st.PutJob(ctx, job); err != nil {
		return MergeOutput{}, fmt.Errorf("merge: put job: %w", err)
	}

	return MergeOutput{JobID: in.JobID, MergedResultURI: mergedURI}, nil
}
