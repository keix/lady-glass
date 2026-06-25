package lambda

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-lambda-go/events"

	"github.com/keix/lady-glass/internal/executor"
	"github.com/keix/lady-glass/internal/pipeline"
)

// Handler is the function shape an SQS-triggered Lambda exposes.
type Handler func(ctx context.Context, ev events.SQSEvent) error

// NewSQSHandler returns a Handler that decodes each SQS record body as a
// pipeline.StepInput and dispatches it to ex. A decode failure or executor
// error is returned to the Lambda runtime, which then leaves the message on
// the queue for SQS to redeliver after the visibility timeout.
//
// Batch size 1 is assumed (see design §10.2). For batch size > 1, a single
// failing record fails the whole batch — partial-batch responses are out of
// scope for v0.
func NewSQSHandler(ex *executor.Executor) Handler {
	return func(ctx context.Context, ev events.SQSEvent) error {
		for _, rec := range ev.Records {
			var in pipeline.StepInput
			if err := json.Unmarshal([]byte(rec.Body), &in); err != nil {
				return fmt.Errorf("decode message %s: %w", rec.MessageId, err)
			}
			if err := ex.Execute(ctx, in); err != nil {
				return fmt.Errorf("execute message %s: %w", rec.MessageId, err)
			}
		}
		return nil
	}
}
