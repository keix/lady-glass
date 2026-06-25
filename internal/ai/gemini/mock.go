package gemini

import (
	"context"
	"fmt"

	"github.com/keix/lady-glass/internal/object"
	"github.com/keix/lady-glass/internal/pipeline"
)

type Mock struct {
	Objects object.Store
	Calls   *int
}

func (s *Mock) Name() string    { return "gemini" }
func (s *Mock) Version() string { return "v1" }

func (s *Mock) Run(ctx context.Context, in pipeline.StepInput) (pipeline.StepOutput, error) {
	if s.Calls != nil {
		*s.Calls++
	}

	body := map[string]any{
		"summary":      "mock structured result",
		"previous_uri": in.PreviousURI,
		"page":         in.Page,
	}

	jsonURI, err := s.Objects.PutJSON(ctx, fmt.Sprintf("jobs/%s/pages/%06d/gemini/v1/result.json", in.JobID, in.Page), body)
	if err != nil {
		return pipeline.StepOutput{}, err
	}

	return pipeline.StepOutput{
		JobID:     in.JobID,
		Page:      in.Page,
		Stage:     s.Name(),
		Version:   s.Version(),
		ResultURI: jsonURI,
		JSONURI:   jsonURI,
		Usage: pipeline.Usage{
			Provider:     "mock",
			Model:        "mock-gemini",
			InputTokens:  10,
			OutputTokens: 20,
		},
	}, nil
}
