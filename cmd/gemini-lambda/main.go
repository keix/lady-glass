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

	"github.com/keix/lady-glass/internal/stage/ai/gemini"
	"github.com/keix/lady-glass/internal/executor"
	lglambda "github.com/keix/lady-glass/internal/lambda"
	"github.com/keix/lady-glass/internal/object"
	"github.com/keix/lady-glass/internal/queue"
	"github.com/keix/lady-glass/internal/store"
)

// gemini-lambda consumes from gemini-queue, runs the Gemini multimodal
// extraction step against the page image, and writes the JSON result to
// S3 / DynamoDB. There is no next stage; the chain terminates here for v0.
//
// Required env:
//
//	LADY_GLASS_TABLE              DynamoDB table name
//	LADY_GLASS_BUCKET             S3 bucket for artifacts
//	LADY_GLASS_GEMINI_API_KEY     Google AI Studio API key
//
// Optional env:
//
//	LADY_GLASS_GEMINI_MODEL       Gemini model (default: gemini-2.5-flash)
func main() {
	ctx := context.Background()

	table := mustEnv("LADY_GLASS_TABLE")
	bucket := mustEnv("LADY_GLASS_BUCKET")
	apiKey := mustEnv("LADY_GLASS_GEMINI_API_KEY")
	model := os.Getenv("LADY_GLASS_GEMINI_MODEL")
	if model == "" {
		model = "gemini-2.5-flash"
	}

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("load aws config: %v", err)
	}

	objects := object.NewS3Store(s3.NewFromConfig(cfg), bucket)
	st := store.NewDynamoStore(dynamodb.NewFromConfig(cfg), table)

	// Producing queue is unused (no NextStage) but stays wired so chain
	// extensions only require an env entry.
	q := queue.NewSQSQueue(sqs.NewFromConfig(cfg), map[string]string{})

	sdkClient, err := gemini.NewSDKClient(ctx, apiKey, model)
	if err != nil {
		log.Fatalf("init gemini client: %v", err)
	}

	step := &gemini.Step{Client: sdkClient, Objects: objects}

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
