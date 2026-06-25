package main

import (
	"context"
	"log"
	"os"

	awslambda "github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/keix/lady-glass/internal/queue"
	"github.com/keix/lady-glass/internal/store"
	"github.com/keix/lady-glass/internal/workflow"
)

// submit-pages-lambda is the Step Functions task that opens a job: it
// writes the JobRecord with status=running and fans one StepInput per
// page out to the first stage queue.
//
// Required env:
//
//	LADY_GLASS_TABLE              DynamoDB table name
//	LADY_GLASS_QUEUE_GEMINI       URL of the first stage queue
func main() {
	ctx := context.Background()

	table := mustEnv("LADY_GLASS_TABLE")
	geminiURL := mustEnv("LADY_GLASS_QUEUE_GEMINI")

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("load aws config: %v", err)
	}

	st := store.NewDynamoStore(dynamodb.NewFromConfig(cfg), table)
	q := queue.NewSQSQueue(sqs.NewFromConfig(cfg), map[string]string{
		"gemini": geminiURL,
	})

	awslambda.Start(func(ctx context.Context, in workflow.SubmitPagesInput) (workflow.SubmitPagesOutput, error) {
		return workflow.SubmitPages(ctx, in, st, q)
	})
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("env %s is required", key)
	}
	return v
}
