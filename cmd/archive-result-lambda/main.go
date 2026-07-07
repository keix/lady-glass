package main

import (
	"context"
	"log"
	"os"
	"time"

	awslambda "github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/keix/lady-glass/internal/object"
	"github.com/keix/lady-glass/internal/store"
	"github.com/keix/lady-glass/internal/workflow"
)

// archive-result-lambda is the per-job Step Functions task that runs
// after Merge: it reads the merged per-job result from the 14-day stage
// bucket, flattens the per-page structure into one transactions.v1
// document, and writes that document + the raw PDF + a manifest into the
// permanent bucket. IndexKowloon downstream points Kowloon at the
// manifest the run produces.
//
// Two S3 buckets are involved and they are DISTINCT:
//
//	LADY_GLASS_BUCKET             the 14-day stage bucket (source): holds
//	                              Merge's output and the raw input PDF
//	LADY_GLASS_PERMANENT_BUCKET   the durable bucket (destination): holds
//	                              results/, raw/, and manifests/
//
// The step is idempotent on the destination side — a rerun with the
// manifest already present issues no PutObject and returns the prior
// run's URIs (workflow.ArchiveResult §5.6).
//
// Required env:
//
//	LADY_GLASS_TABLE              DynamoDB table name
//	LADY_GLASS_BUCKET             stage bucket (source)
//	LADY_GLASS_PERMANENT_BUCKET   permanent bucket (destination)
func main() {
	ctx := context.Background()

	table := mustEnv("LADY_GLASS_TABLE")
	stageBucket := mustEnv("LADY_GLASS_BUCKET")
	permanentBucket := mustEnv("LADY_GLASS_PERMANENT_BUCKET")

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("load aws config: %v", err)
	}

	s3Client := s3.NewFromConfig(cfg)
	st := store.NewDynamoStore(dynamodb.NewFromConfig(cfg), table)
	source := object.NewS3Store(s3Client, stageBucket)
	destination := object.NewS3Store(s3Client, permanentBucket)

	awslambda.Start(func(ctx context.Context, in workflow.ArchiveResultInput) (workflow.ArchiveResultOutput, error) {
		return workflow.ArchiveResult(ctx, in, st, source, destination, time.Now)
	})
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("env %s is required", key)
	}
	return v
}
