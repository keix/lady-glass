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

type Store interface {
	GetStage(ctx context.Context, jobID string, page int, stage string, version string) (*StageRecord, error)
	MarkRunning(ctx context.Context, in pipeline.StepInput) error
	MarkSucceeded(ctx context.Context, out pipeline.StepOutput, nextStage string) error
	MarkFailed(ctx context.Context, in pipeline.StepInput, err error) error
}
