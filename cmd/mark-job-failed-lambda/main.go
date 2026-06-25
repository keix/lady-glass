package main

import (
	"context"
	"log"
	"os"

	awslambda "github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/keix/lady-glass/internal/store"
	"github.com/keix/lady-glass/internal/workflow"
)

// mark-job-failed-lambda is the failure terminal of the Step Functions
// loop: it flips the JobRecord to status=failed (preserving InputURI
// and PageCount) so the persisted document state matches the workflow
// outcome.
//
// Required env:
//
//	LADY_GLASS_TABLE   DynamoDB table name
func main() {
	ctx := context.Background()

	table := mustEnv("LADY_GLASS_TABLE")

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("load aws config: %v", err)
	}

	st := store.NewDynamoStore(dynamodb.NewFromConfig(cfg), table)

	awslambda.Start(func(ctx context.Context, in workflow.MarkJobFailedInput) (workflow.MarkJobFailedOutput, error) {
		return workflow.MarkJobFailed(ctx, in, st)
	})
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("env %s is required", key)
	}
	return v
}
