// Package mockstep is a test-only stage.Step implementation. It writes
// a synthetic text artifact + a small JSON record to the object store
// and returns a StepOutput at "mock/v1". The executor / sqs_handler /
// workflow tests use it to exercise the multi-stage chain plumbing
// without needing a real provider call.
//
// The package lives under internal/stage so the (Name, Version)
// contract from SPEC §S2 still applies — Step is just another stage
// from the Executor's perspective.
package mockstep

import (
	"context"
	"fmt"

	"github.com/keix/lady-glass/internal/object"
	"github.com/keix/lady-glass/internal/pipeline"
)

// Step is the mock Step. Calls (when non-nil) is incremented on every
// Run so tests can assert how many times the Step was invoked, used
// when verifying the succeeded-skip path (§S5) and retry behaviour.
type Step struct {
	Objects object.Store
	Calls   *int
}

func (s *Step) Name() string    { return "mock" }
func (s *Step) Version() string { return "v1" }

func (s *Step) Run(ctx context.Context, in pipeline.StepInput) (pipeline.StepOutput, error) {
	if s.Calls != nil {
		*s.Calls++
	}

	text := fmt.Sprintf("mock text for job=%s page=%d", in.JobID, in.Page)

	textURI, err := s.Objects.PutText(ctx, fmt.Sprintf("jobs/%s/pages/%06d/mock/v1/text.txt", in.JobID, in.Page), text)
	if err != nil {
		return pipeline.StepOutput{}, err
	}

	resultURI, err := s.Objects.PutJSON(ctx, fmt.Sprintf("jobs/%s/pages/%06d/mock/v1/result.json", in.JobID, in.Page), map[string]any{
		"text_uri": textURI,
		"text":     text,
	})
	if err != nil {
		return pipeline.StepOutput{}, err
	}

	return pipeline.StepOutput{
		JobID:     in.JobID,
		Page:      in.Page,
		Stage:     s.Name(),
		Version:   s.Version(),
		ResultURI: resultURI,
		TextURI:   textURI,
		Usage: &pipeline.Usage{
			Provider: "mock",
			Model:    "mock-step",
		},
	}, nil
}
