package queue

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/keix/lady-glass/internal/pipeline"
)

// SQSClient is the subset of the AWS SQS client used by SQSQueue. Declaring
// it as an interface lets tests substitute a fake without pulling in the
// SDK's networking machinery.
type SQSClient interface {
	SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

// SQSQueue sends pipeline.StepInput messages to one of several SQS queues,
// addressed by the logical name used in a ChainSpec (e.g. "gemini").
type SQSQueue struct {
	Client SQSClient
	URLs   map[string]string
}

func NewSQSQueue(client SQSClient, urls map[string]string) *SQSQueue {
	return &SQSQueue{Client: client, URLs: urls}
}

func (q *SQSQueue) Send(ctx context.Context, queueName string, in pipeline.StepInput) error {
	url, ok := q.URLs[queueName]
	if !ok {
		return fmt.Errorf("queue: no URL configured for %q", queueName)
	}

	body, err := json.Marshal(in)
	if err != nil {
		return fmt.Errorf("queue: marshal %q: %w", queueName, err)
	}

	if _, err := q.Client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(url),
		MessageBody: aws.String(string(body)),
	}); err != nil {
		return fmt.Errorf("queue: send to %q: %w", queueName, err)
	}

	return nil
}
