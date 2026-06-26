package main

import (
	"context"
	"log"
	"os"
	"strconv"

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
//
// Optional env (with defaults):
//
//	LADY_GLASS_RETENTION_DAYS   DDB TTL window (SPEC §S9)  (14)
func main() {
	ctx := context.Background()

	table := mustEnv("LADY_GLASS_TABLE")
	retentionDays := envIntDefault("LADY_GLASS_RETENTION_DAYS", 14)

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("load aws config: %v", err)
	}

	st := store.NewDynamoStore(dynamodb.NewFromConfig(cfg), table)
	st.RetentionDays = retentionDays

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

func envIntDefault(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Fatalf("env %s must be int: %v", key, err)
	}
	return n
}
