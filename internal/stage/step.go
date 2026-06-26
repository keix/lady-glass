package stage

import (
	"context"

	"github.com/keix/lady-glass/internal/pipeline"
)

type Step interface {
	Name() string
	Version() string
	Run(ctx context.Context, in pipeline.StepInput) (pipeline.StepOutput, error)
}
