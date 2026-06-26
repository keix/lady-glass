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

	"github.com/keix/lady-glass/internal/stage/ai/gemini"
	"github.com/keix/lady-glass/internal/executor"
	lglambda "github.com/keix/lady-glass/internal/lambda"
	"github.com/keix/lady-glass/internal/object"
	"github.com/keix/lady-glass/internal/pipeline"
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
//	LADY_GLASS_RETENTION_DAYS     DDB TTL window (SPEC §S9)        (14)
//
// Optional env (next-stage wiring; leave unset to make gemini terminal):
//
//	LADY_GLASS_NEXT_STAGE_NAME       e.g. "normalize_card_statement"
//	LADY_GLASS_NEXT_STAGE_VERSION    e.g. "v1"
//	LADY_GLASS_NEXT_QUEUE_NAME       logical queue name for the next stage
//	LADY_GLASS_NEXT_QUEUE_URL        SQS URL the next stage's ESM is bound to
func main() {
	ctx := context.Background()

	table := mustEnv("LADY_GLASS_TABLE")
	bucket := mustEnv("LADY_GLASS_BUCKET")
	apiKey := mustEnv("LADY_GLASS_GEMINI_API_KEY")
	model := os.Getenv("LADY_GLASS_GEMINI_MODEL")
	if model == "" {
		model = "gemini-2.5-flash"
	}
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

	sdkClient, err := gemini.NewSDKClient(ctx, apiKey, model)
	if err != nil {
		log.Fatalf("init gemini client: %v", err)
	}

	step := &gemini.Step{Client: sdkClient, Objects: objects}

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
