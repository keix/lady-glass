package store

import (
	"context"
	"sync"

	"github.com/keix/lady-glass/internal/pipeline"
)

type MemoryStore struct {
	mu     sync.Mutex
	stages map[string]*StageRecord
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		stages: make(map[string]*StageRecord),
	}
}

func (s *MemoryStore) GetStage(ctx context.Context, jobID string, page int, stage string, version string) (*StageRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := pipeline.StageKey(jobID, page, stage, version)
	rec, ok := s.stages[key]
	if !ok {
		return nil, nil
	}

	cp := *rec
	return &cp, nil
}

// MarkRunning unconditionally overwrites the record with status=running.
// The "do not downgrade from succeeded" invariant is enforced one layer
// up by Executor.Execute via GetStage; this mirrors DynamoStore's
// last-writer-wins PutItem semantics so the contract stays consistent
// across backends. See the Store interface doc for the broader contract.
func (s *MemoryStore) MarkRunning(ctx context.Context, in pipeline.StepInput) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := pipeline.StageKey(in.JobID, in.Page, in.Stage, in.Version)
	s.stages[key] = &StageRecord{
		JobID:          in.JobID,
		Page:           in.Page,
		Stage:          in.Stage,
		Version:        in.Version,
		Status:         StageStatusRunning,
		IdempotencyKey: key,
		InputURI:       in.InputURI,
	}

	return nil
}

func (s *MemoryStore) MarkSucceeded(ctx context.Context, out pipeline.StepOutput, nextStage string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := pipeline.StageKey(out.JobID, out.Page, out.Stage, out.Version)

	s.stages[key] = &StageRecord{
		JobID:          out.JobID,
		Page:           out.Page,
		Stage:          out.Stage,
		Version:        out.Version,
		Status:         StageStatusSucceeded,
		IdempotencyKey: key,
		ResultURI:      out.ResultURI,
		NextStage:      nextStage,
	}

	return nil
}

func (s *MemoryStore) MarkFailed(ctx context.Context, in pipeline.StepInput, err error) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := pipeline.StageKey(in.JobID, in.Page, in.Stage, in.Version)

	s.stages[key] = &StageRecord{
		JobID:          in.JobID,
		Page:           in.Page,
		Stage:          in.Stage,
		Version:        in.Version,
		Status:         StageStatusFailed,
		IdempotencyKey: key,
		InputURI:       in.InputURI,
		Error:          err.Error(),
	}

	return nil
}
