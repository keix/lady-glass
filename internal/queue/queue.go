package queue

import (
	"context"

	"github.com/keix/lady-glass/internal/pipeline"
)

type Queue interface {
	Send(ctx context.Context, queueName string, in pipeline.StepInput) error
}
