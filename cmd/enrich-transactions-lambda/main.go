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
	"github.com/keix/lady-glass/internal/pipeline"
	"github.com/keix/lady-glass/internal/queue"
	enrichtx "github.com/keix/lady-glass/internal/stage/enrich/transactions"
	"github.com/keix/lady-glass/internal/store"
)

// enrich-transactions-lambda is the post-normalisation stage in the
// credit-card-statement chain: it reads the previous stage's
// PageExtractionResult from S3, attaches MerchantNormalized / Category /
// Country to every transaction using the embedded merchants dictionary
// (rules only in v1, no AI call), and writes the enriched result back.
// It runs after normalize_card_statement and is the last per-page chain
// stage — the SFN Merge → ArchiveResult → IndexKowloon steps take over
// once every page is done.
//
// The dictionary is the embedded seed (merchants.yaml compiled into the
// binary); DefaultDictionary panics at process start if that seed is
// invalid, so a broken dictionary surfaces on cold start rather than on
// the first message.
//
// Required env:
//
//	LADY_GLASS_TABLE              DynamoDB table name
//	LADY_GLASS_BUCKET             S3 bucket for artifacts
//
// Optional env:
//
//	LADY_GLASS_RETENTION_DAYS     DDB TTL window (SPEC §S9)  (14)
//
// Optional env (next-stage wiring; leave unset to make enrich terminal):
//
//	LADY_GLASS_NEXT_STAGE_NAME       next stage name
//	LADY_GLASS_NEXT_STAGE_VERSION    next stage version
//	LADY_GLASS_NEXT_QUEUE_NAME       logical queue name for the next stage
//	LADY_GLASS_NEXT_QUEUE_URL        SQS URL the next stage's ESM is bound to
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

	nextStage, queueURLs := loadNextStageFromEnv()
	q := queue.NewSQSQueue(sqs.NewFromConfig(cfg), queueURLs)

	// DefaultDictionary panics on an invalid embedded seed — do it here
	// so the failure is a cold-start crash, not a per-message error.
	step := &enrichtx.Step{
		Dictionary: enrichtx.DefaultDictionary(),
		Objects:    objects,
	}

	ex := &executor.Executor{
		Step:      step,
		NextStage: nextStage,
		Store:     st,
		Queue:     q,
	}

	awslambda.Start(lglambda.NewSQSHandler(ex))
}

// loadNextStageFromEnv reads the four LADY_GLASS_NEXT_* env vars and
// returns a populated StageSpec plus the queue-URL map. All four are
// required as a set; missing any one means "no next stage" and the
// Executor falls back to terminal-stage behaviour. A partial set is
// rejected loudly rather than silently mis-routed.
func loadNextStageFromEnv() (*pipeline.StageSpec, map[string]string) {
	name := os.Getenv("LADY_GLASS_NEXT_STAGE_NAME")
	version := os.Getenv("LADY_GLASS_NEXT_STAGE_VERSION")
	queueName := os.Getenv("LADY_GLASS_NEXT_QUEUE_NAME")
	queueURL := os.Getenv("LADY_GLASS_NEXT_QUEUE_URL")

	allUnset := name == "" && version == "" && queueName == "" && queueURL == ""
	allSet := name != "" && version != "" && queueName != "" && queueURL != ""
	if !allUnset && !allSet {
		log.Fatalf("LADY_GLASS_NEXT_* env vars must be all set or all unset; got name=%q version=%q queue_name=%q queue_url_set=%v", name, version, queueName, queueURL != "")
	}
	if !allSet {
		return nil, map[string]string{}
	}
	return &pipeline.StageSpec{
			Name:      name,
			Version:   version,
			QueueName: queueName,
		}, map[string]string{
			queueName: queueURL,
		}
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
