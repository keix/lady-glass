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
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/keix/lady-glass/internal/executor"
	lglambda "github.com/keix/lady-glass/internal/lambda"
	"github.com/keix/lady-glass/internal/object"
	"github.com/keix/lady-glass/internal/queue"
	"github.com/keix/lady-glass/internal/stage/normalize/cardstatement"
	"github.com/keix/lady-glass/internal/store"
)

// normalize-card-statement-lambda is the post-processing stage that runs
// after Gemini extraction in the credit-card-statement chain. It does
// not talk to any AI provider; it only reads the previous stage's
// PageExtractionResult from S3, applies the v1 normaliser rules, and
// writes the cleaned-up result back. For this chain it is the terminal
// stage, so no NextStage env is wired and the queue map is empty.
//
// Required env:
//
//	LADY_GLASS_TABLE              DynamoDB table name
//	LADY_GLASS_BUCKET             S3 bucket for artifacts
//
// Optional env:
//
//	LADY_GLASS_RETENTION_DAYS     DDB TTL window (SPEC §S9)  (14)
func main() {
	ctx := context.Background()

	table := mustEnv("LADY_GLASS_TABLE")
	bucket := mustEnv("LADY_GLASS_BUCKET")
	retentionDays := envIntDefault("LADY_GLASS_RETENTION_DAYS", 14)

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("load aws config: %v", err)
	}

	objects := object.NewS3Store(s3.NewFromConfig(cfg), bucket)
	st := store.NewDynamoStore(dynamodb.NewFromConfig(cfg), table)
	st.RetentionDays = retentionDays
	q := queue.NewSQSQueue(sqs.NewFromConfig(cfg), map[string]string{})

	step := &cardstatement.Step{Objects: objects}

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
