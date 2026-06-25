package main

import (
	"context"
	"log"
	"os"

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
func main() {
	ctx := context.Background()

	table := mustEnv("LADY_GLASS_TABLE")
	bucket := mustEnv("LADY_GLASS_BUCKET")

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("load aws config: %v", err)
	}

	st := store.NewDynamoStore(dynamodb.NewFromConfig(cfg), table)
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
