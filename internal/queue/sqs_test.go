package queue_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/keix/lady-glass/internal/pipeline"
	"github.com/keix/lady-glass/internal/queue"
)

type fakeSQSClient struct {
	calls   []sentMessage
	sendErr error
}

type sentMessage struct {
	queueURL string
	body     string
}

func (f *fakeSQSClient) SendMessage(ctx context.Context, in *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	if f.sendErr != nil {
		return nil, f.sendErr
	}
	f.calls = append(f.calls, sentMessage{
		queueURL: aws.ToString(in.QueueUrl),
		body:     aws.ToString(in.MessageBody),
	})
	return &sqs.SendMessageOutput{MessageId: aws.String("m1")}, nil
}

func TestSQSQueue_SendMarshalsAndDispatchesToURL(t *testing.T) {
	const url = "https://sqs.us-east-1.amazonaws.com/123456789012/gemini-queue"
	fake := &fakeSQSClient{}
	q := queue.NewSQSQueue(fake, map[string]string{"gemini": url})

	in := pipeline.StepInput{JobID: "j1", Page: 1, Stage: "gemini", Version: "v1"}
	if err := q.Send(context.Background(), "gemini", in); err != nil {
		t.Fatalf("send: %v", err)
	}

	if got := len(fake.calls); got != 1 {
		t.Fatalf("client calls = %d, want 1", got)
	}
	if got := fake.calls[0].queueURL; got != url {
		t.Fatalf("queue URL = %q, want %q", got, url)
	}

	var decoded pipeline.StepInput
	if err := json.Unmarshal([]byte(fake.calls[0].body), &decoded); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if decoded.JobID != in.JobID || decoded.Page != in.Page || decoded.Stage != in.Stage {
		t.Fatalf("decoded body = %+v, want %+v", decoded, in)
	}
}

func TestSQSQueue_UnknownQueueIsAnError(t *testing.T) {
	fake := &fakeSQSClient{}
	q := queue.NewSQSQueue(fake, map[string]string{"gemini": "https://example/gemini"})

	err := q.Send(context.Background(), "missing", pipeline.StepInput{JobID: "j2"})
	if err == nil {
		t.Fatal("expected error for unknown queue, got nil")
	}
	if !strings.Contains(err.Error(), `"missing"`) {
		t.Fatalf("error %q does not name the queue", err)
	}
	if len(fake.calls) != 0 {
		t.Fatalf("client was called %d times on unknown queue, want 0", len(fake.calls))
	}
}

func TestSQSQueue_SDKErrorIsWrapped(t *testing.T) {
	fake := &fakeSQSClient{sendErr: errors.New("simulated SDK failure")}
	q := queue.NewSQSQueue(fake, map[string]string{"gemini": "https://example/gemini"})

	err := q.Send(context.Background(), "gemini", pipeline.StepInput{JobID: "j3"})
	if err == nil {
		t.Fatal("expected wrapped SDK error, got nil")
	}
	if !strings.Contains(err.Error(), "simulated SDK failure") {
		t.Fatalf("error %q does not include underlying SDK error", err)
	}
}
