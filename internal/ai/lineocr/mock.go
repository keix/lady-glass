package lineocr

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

func (s *Mock) Name() string    { return "line_ocr" }
func (s *Mock) Version() string { return "v1" }

func (s *Mock) Run(ctx context.Context, in pipeline.StepInput) (pipeline.StepOutput, error) {
	if s.Calls != nil {
		*s.Calls++
	}

	text := fmt.Sprintf("mock OCR text for job=%s page=%d", in.JobID, in.Page)

	textURI, err := s.Objects.PutText(ctx, fmt.Sprintf("jobs/%s/pages/%06d/line_ocr/v1/text.txt", in.JobID, in.Page), text)
	if err != nil {
		return pipeline.StepOutput{}, err
	}

	resultURI, err := s.Objects.PutJSON(ctx, fmt.Sprintf("jobs/%s/pages/%06d/line_ocr/v1/result.json", in.JobID, in.Page), map[string]any{
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
		Usage: pipeline.Usage{
			Provider: "mock",
			Model:    "mock-line-ocr",
		},
	}, nil
}
