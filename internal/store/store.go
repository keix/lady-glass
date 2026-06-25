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
}
