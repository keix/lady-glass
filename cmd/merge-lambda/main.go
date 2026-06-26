package main

import (
	"context"
	"log"
	"os"
	"strconv"

	awslambda "github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/keix/lady-glass/internal/object"
	"github.com/keix/lady-glass/internal/store"
	"github.com/keix/lady-glass/internal/workflow"
)

// merge-lambda is the success terminal of the Step Functions loop: it
// gathers every per-page result body, writes the lossless merged
// document to S3, and flips the JobRecord to status=succeeded.
//
// Required env:
//
//	LADY_GLASS_TABLE    DynamoDB table name
//	LADY_GLASS_BUCKET   S3 bucket for artifacts
//
// Optional env (with defaults):
//
//	LADY_GLASS_RETENTION_DAYS   DDB TTL window (SPEC §S9)  (14)
func main() {
	ctx := context.Background()

	table := mustEnv("LADY_GLASS_TABLE")
	bucket := mustEnv("LADY_GLASS_BUCKET")
	retentionDays := envIntDefault("LADY_GLASS_RETENTION_DAYS", 14)

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("load aws config: %v", err)
	}

	st := store.NewDynamoStore(dynamodb.NewFromConfig(cfg), table)
	st.RetentionDays = retentionDays
	obj := object.NewS3Store(s3.NewFromConfig(cfg), bucket)

	awslambda.Start(func(ctx context.Context, in workflow.MergeInput) (workflow.MergeOutput, error) {
		return workflow.Merge(ctx, in, st, obj)
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
