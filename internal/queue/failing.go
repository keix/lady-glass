package queue

import (
	"context"

	"github.com/keix/lady-glass/internal/pipeline"
)

// Failing is a Queue whose Send always returns Err.
// Used to exercise next-stage enqueue failure recovery in tests.
type Failing struct {
	Err error
}

func (q *Failing) Send(ctx context.Context, queueName string, in pipeline.StepInput) error {
	return q.Err
}
