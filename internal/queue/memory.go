package queue

import (
	"context"
	"sync"

	"github.com/keix/lady-glass/internal/pipeline"
)

type MemoryQueue struct {
	mu       sync.Mutex
	Messages map[string][]pipeline.StepInput
}

func NewMemoryQueue() *MemoryQueue {
	return &MemoryQueue{
		Messages: make(map[string][]pipeline.StepInput),
	}
}

func (q *MemoryQueue) Send(ctx context.Context, queueName string, in pipeline.StepInput) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.Messages[queueName] = append(q.Messages[queueName], in)
	return nil
}

func (q *MemoryQueue) Pop(queueName string) (pipeline.StepInput, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	msgs := q.Messages[queueName]
	if len(msgs) == 0 {
		return pipeline.StepInput{}, false
	}

	msg := msgs[0]
	q.Messages[queueName] = msgs[1:]
	return msg, true
}
