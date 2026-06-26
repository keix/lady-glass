package store

import (
	"context"

	"github.com/keix/lady-glass/internal/pipeline"
)

type StageStatus string

const (
	StageStatusQueued    StageStatus = "queued"
	StageStatusRunning   StageStatus = "running"
	StageStatusSucceeded StageStatus = "succeeded"
	StageStatusFailed    StageStatus = "failed"
)

type StageRecord struct {
	JobID          string
	Page           int
	Stage          string
	Version        string
	Status         StageStatus
	IdempotencyKey string
	InputURI       string
	ResultURI      string
	NextStage      string
	Error          string
	// ExpiresAt is the unix-epoch second at which this record becomes
	// expired. DynamoStore writes it on every Put when configured with
	// a non-zero RetentionDays; DDB's per-item TTL eventually deletes
	// the row (with up to 48h lag), and GetStage / ListStagesByJob
	// filter expired rows out at read time so the lag is invisible to
	// callers. Zero means "no expiry was attached" (in-memory paths,
	// legacy rows written before retention shipped).
	ExpiresAt int64
}

type JobStatus string

const (
	JobStatusCreated   JobStatus = "created"
	JobStatusRunning   JobStatus = "running"
	JobStatusSucceeded JobStatus = "succeeded"
	JobStatusFailed    JobStatus = "failed"
)

// JobRecord is the document-level row. SubmitPages writes it on entry,
// Merge finalises it on success, and the document workflow flips it to
// failed if any stage reports failure. Per-page progress is derived from
// StageRecord entries via ListStagesByJob — there is no separate per-page
// row in v0.
type JobRecord struct {
	JobID     string
	Status    JobStatus
	InputURI  string
	ResultURI string
	PageCount int
	Error     string
	// Mode is the workflow mode the job was created with — currently
	// "passthrough" (Gemini reads the whole PDF) or "rendered"
	// (RenderPages splits the PDF into per-page PDFs first). Empty
	// means the row predates the field and is treated as
	// "passthrough" by callers.
	Mode string
	// ExpiresAt is the unix-epoch second at which this job row
	// becomes expired. See StageRecord.ExpiresAt for the full
	// contract (TTL + read-time filter, zero = no expiry).
	ExpiresAt int64
	UpdatedAt string
}

// Store persists per-stage state for the Executor.
//
// Contract: every write is an unconditional overwrite. The "succeeded →
// skip" guarantee is enforced one layer up by Executor.Execute, which
// checks status via GetStage and short-circuits before ever calling
// MarkRunning. Stores therefore do NOT enforce "no downgrade" or
// "no overwrite" at the row level.
//
// Deferred to a later phase:
//   - conditional-update lease (detect stuck "running" via TTL or
//     heartbeat).
//   - attempt counter and failure history; only the latest error is
//     retained on the row.
//
// SQS MaxReceiveCount and the DLQ are the upstream retry / escalation
// mechanism that compensates for the missing pieces.
type Store interface {
	GetStage(ctx context.Context, jobID string, page int, stage string, version string) (*StageRecord, error)
	MarkRunning(ctx context.Context, in pipeline.StepInput) error
	MarkSucceeded(ctx context.Context, out pipeline.StepOutput, nextStage string) error
	MarkFailed(ctx context.Context, in pipeline.StepInput, err error) error

	// GetJob returns (nil, nil) when no record exists.
	GetJob(ctx context.Context, jobID string) (*JobRecord, error)
	// PutJob unconditionally overwrites the job row. Same overwrite
	// contract as the stage writers.
	PutJob(ctx context.Context, rec JobRecord) error
	// ListStagesByJob enumerates every StageRecord for the given (jobID,
	// stage, version). CheckPages and Merge use this to count
	// succeeded/failed pages and to gather per-page result URIs.
	ListStagesByJob(ctx context.Context, jobID string, stage string, version string) ([]StageRecord, error)
}
