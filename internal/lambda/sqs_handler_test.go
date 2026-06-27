package lambda_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-lambda-go/events"

	"github.com/keix/lady-glass/internal/stage/mockstep"
	"github.com/keix/lady-glass/internal/executor"
	"github.com/keix/lady-glass/internal/lambda"
	"github.com/keix/lady-glass/internal/object"
	"github.com/keix/lady-glass/internal/pipeline"
	"github.com/keix/lady-glass/internal/queue"
	"github.com/keix/lady-glass/internal/store"
)

func makeEvent(t *testing.T, inputs ...pipeline.StepInput) events.SQSEvent {
	t.Helper()
	var records []events.SQSMessage
	for i, in := range inputs {
		body, err := json.Marshal(in)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		records = append(records, events.SQSMessage{
			MessageId: fmt.Sprintf("m%d", i),
			Body:      string(body),
		})
	}
	return events.SQSEvent{Records: records}
}

func TestSQSHandler_DecodesAndDispatches(t *testing.T) {
	calls := 0
	q := queue.NewMemoryQueue()
	ex := &executor.Executor{
		Step:      &mockstep.Step{Objects: object.NewFileStore(t.TempDir()), Calls: &calls},
		NextStage: &pipeline.StageSpec{Name: "gemini", Version: "v1", QueueName: "gemini"},
		Store:     store.NewMemoryStore(),
		Queue:     q,
	}
	handler := lambda.NewSQSHandler(ex)

	in := pipeline.StepInput{JobID: "j_happy", Page: 1}
	if err := handler(context.Background(), makeEvent(t, in)); err != nil {
		t.Fatalf("handler: %v", err)
	}

	if calls != 1 {
		t.Fatalf("step calls = %d, want 1", calls)
	}
	if got := len(q.Messages["gemini"]); got != 1 {
		t.Fatalf("gemini queue size = %d, want 1", got)
	}
}

func TestSQSHandler_ExecutorErrorReturnsError(t *testing.T) {
	calls := 0
	ex := &executor.Executor{
		Step:      &mockstep.Step{Objects: object.NewFileStore(t.TempDir()), Calls: &calls},
		NextStage: &pipeline.StageSpec{Name: "gemini", Version: "v1", QueueName: "gemini"},
		Store:     store.NewMemoryStore(),
		Queue:     &queue.Failing{Err: errors.New("simulated send failure")},
	}
	handler := lambda.NewSQSHandler(ex)

	in := pipeline.StepInput{JobID: "j_exec_err", Page: 1}
	err := handler(context.Background(), makeEvent(t, in))
	if err == nil {
		t.Fatal("expected error from failing queue, got nil")
	}
	if !strings.Contains(err.Error(), "execute message") {
		t.Fatalf("error %q does not mention 'execute message'", err)
	}
}

func TestSQSHandler_DecodeFailureReturnsError(t *testing.T) {
	calls := 0
	ex := &executor.Executor{
		Step:  &mockstep.Step{Objects: object.NewFileStore(t.TempDir()), Calls: &calls},
		Store: store.NewMemoryStore(),
		Queue: queue.NewMemoryQueue(),
	}
	handler := lambda.NewSQSHandler(ex)

	ev := events.SQSEvent{
		Records: []events.SQSMessage{
			{MessageId: "bad", Body: "this is not json"},
		},
	}
	err := handler(context.Background(), ev)
	if err == nil {
		t.Fatal("expected decode error, got nil")
	}
	if !strings.Contains(err.Error(), "decode message") {
		t.Fatalf("error %q does not mention 'decode message'", err)
	}
	if calls != 0 {
		t.Fatalf("step called %d times on decode failure, want 0", calls)
	}
}
