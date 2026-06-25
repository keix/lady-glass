package main

import (
	"context"
	"log"
	"os"

	awslambda "github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/keix/lady-glass/internal/ai/gemini"
	"github.com/keix/lady-glass/internal/executor"
	lglambda "github.com/keix/lady-glass/internal/lambda"
	"github.com/keix/lady-glass/internal/object"
	"github.com/keix/lady-glass/internal/queue"
	"github.com/keix/lady-glass/internal/store"
)

// gemini-lambda consumes from gemini-queue, runs the Gemini step, and
// writes its result to S3 / DynamoDB. There is no next stage; the chain
// terminates here for v0.
//
// Phase 6 plan: swap gemini.Mock for a real ai.Step backed by the
// Google AI Studio Gemini client, configured via LADY_GLASS_GEMINI_API_KEY.
// Step is the only seam that changes — store, queue, handler, and
// executor wiring stay as-is.
func main() {
	ctx := context.Background()

	table := mustEnv("LADY_GLASS_TABLE")
	bucket := mustEnv("LADY_GLASS_BUCKET")

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("load aws config: %v", err)
	}

	objects := object.NewS3Store(s3.NewFromConfig(cfg), bucket)
	st := store.NewDynamoStore(dynamodb.NewFromConfig(cfg), table)

	// Producing queue is unused (no NextStage) but is initialized to a
	// working SQS client so chain extensions only require an env entry.
	q := queue.NewSQSQueue(sqs.NewFromConfig(cfg), map[string]string{})

	step := &gemini.Mock{Objects: objects}

	ex := &executor.Executor{
		Step:  step,
		Store: st,
		Queue: q,
	}

	awslambda.Start(lglambda.NewSQSHandler(ex))
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("env %s is required", key)
	}
	return v
}
