package executor

import (
	"context"
	"fmt"

	"github.com/keix/lady-glass/internal/stage"
	"github.com/keix/lady-glass/internal/pipeline"
	"github.com/keix/lady-glass/internal/queue"
	"github.com/keix/lady-glass/internal/store"
)

type Executor struct {
	Step      stage.Step
	NextStage *pipeline.StageSpec
	Store     store.Store
	Queue     queue.Queue
}

// Execute runs the wrapped Step for in. status=succeeded short-circuits
// past Step.Run and only re-enqueues the next stage — this is the central
// idempotency guarantee that survives SQS redelivery, Lambda
// re-invocation, and Step Functions retry. All other statuses (nil,
// running, failed) fall through to the standard path and Step.Run is
// invoked again.
//
// What v0 does NOT detect:
//   - stuck "running" (no lease, no TTL, no heartbeat). A Lambda that
//     dies between MarkRunning and MarkSucceeded leaves the record in
//     running; the next delivery falls through and re-runs the Step.
//     SQS visibility timeout + MaxReceiveCount + DLQ are the upstream
//     backstop.
//   - retry exhaustion at the stage level (no attempt counter on the
//     record; only the latest error message is kept).
//   - concurrent in-flight execution. Under reserved concurrency > 1 or
//     a visibility-timeout race, two workers can both call Step.Run and
//     the external API is hit twice. The downstream stage absorbs the
//     duplicate via its own succeeded-skip on next-stage delivery.
func (e *Executor) Execute(ctx context.Context, in pipeline.StepInput) error {
	stage := e.Step.Name()
	version := e.Step.Version()

	rec, err := e.Store.GetStage(ctx, in.JobID, in.Page, stage, version)
	if err != nil {
		return err
	}

	if rec != nil && rec.Status == store.StageStatusSucceeded {
		return e.enqueueNext(ctx, in, rec.ResultURI)
	}

	in.Stage = stage
	in.Version = version

	if err := e.Store.MarkRunning(ctx, in); err != nil {
		return err
	}

	out, err := e.Step.Run(ctx, in)
	if err != nil {
		_ = e.Store.MarkFailed(ctx, in, err)
		return fmt.Errorf("%s: %w", stage, err)
	}

	nextStageName := ""
	if next := resolveNextStage(in, e.NextStage); next != nil {
		nextStageName = next.Name
	}

	if err := e.Store.MarkSucceeded(ctx, out, nextStageName); err != nil {
		return err
	}

	return e.enqueueNext(ctx, in, out.ResultURI)
}

// resolveNextStage prefers the chain projected into the message (SPEC
// §S10): the job's frozen ChainSpec rides on every SQS hop, and the
// next stage is just Chain[ChainIdx+1]. Messages without Chain fall
// back to the Executor's env-driven NextStage so messages enqueued by
// an older binary keep flowing.
func resolveNextStage(in pipeline.StepInput, fallback *pipeline.StageSpec) *pipeline.StageSpec {
	if len(in.Chain) > 0 {
		idx := in.ChainIdx + 1
		if idx >= len(in.Chain) {
			return nil
		}
		next := in.Chain[idx]
		return &next
	}
	return fallback
}

func (e *Executor) enqueueNext(ctx context.Context, in pipeline.StepInput, previousURI string) error {
	next := resolveNextStage(in, e.NextStage)
	if next == nil {
		return nil
	}

	outgoing := pipeline.StepInput{
		JobID:           in.JobID,
		DocumentID:      in.DocumentID,
		Page:            in.Page,
		Stage:           next.Name,
		Version:         next.Version,
		InputURI:        in.InputURI,
		PreviousURI:     previousURI,
		PromptProfileID: in.PromptProfileID,
		Metadata:        in.Metadata,
		// Forward the chain so the consuming stage can dispatch its
		// own next hop without re-reading JobRecord. The fallback path
		// (no chain on the inbound message) carries no chain forward
		// either — the receiving stage will rely on its own env.
		Chain:    in.Chain,
		ChainIdx: in.ChainIdx + 1,
	}

	return e.Queue.Send(ctx, next.QueueName, outgoing)
}
