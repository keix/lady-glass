package executor

import (
	"context"
	"fmt"

	"github.com/keix/lady-glass/internal/ai"
	"github.com/keix/lady-glass/internal/pipeline"
	"github.com/keix/lady-glass/internal/queue"
	"github.com/keix/lady-glass/internal/store"
)

type Executor struct {
	Step      ai.Step
	NextStage *pipeline.StageSpec
	Store     store.Store
	Queue     queue.Queue
}

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
	if e.NextStage != nil {
		nextStageName = e.NextStage.Name
	}

	if err := e.Store.MarkSucceeded(ctx, out, nextStageName); err != nil {
		return err
	}

	return e.enqueueNext(ctx, in, out.ResultURI)
}

func (e *Executor) enqueueNext(ctx context.Context, in pipeline.StepInput, previousURI string) error {
	if e.NextStage == nil {
		return nil
	}

	next := pipeline.StepInput{
		JobID:           in.JobID,
		DocumentID:      in.DocumentID,
		Page:            in.Page,
		Stage:           e.NextStage.Name,
		Version:         e.NextStage.Version,
		PreviousURI:     previousURI,
		PromptProfileID: in.PromptProfileID,
		Metadata:        in.Metadata,
	}

	return e.Queue.Send(ctx, e.NextStage.QueueName, next)
}
