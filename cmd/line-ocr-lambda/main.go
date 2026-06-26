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

	"github.com/keix/lady-glass/internal/stage/ai/lineocr"
	"github.com/keix/lady-glass/internal/executor"
	lglambda "github.com/keix/lady-glass/internal/lambda"
	"github.com/keix/lady-glass/internal/object"
	"github.com/keix/lady-glass/internal/pipeline"
	"github.com/keix/lady-glass/internal/queue"
	"github.com/keix/lady-glass/internal/store"
)

// line-ocr-lambda consumes from line-ocr-queue, runs the line_ocr step,
// writes its result to S3 / DynamoDB, and enqueues the gemini stage.
//
// v0 status: intentionally wired with lineocr.Mock. The line_ocr stage
// is a chain-demonstration scaffold; multimodal Gemini handles OCR
// alongside extraction in gemini-lambda, so a real pre-processing OCR
// Step is not on the critical path. The binary is kept buildable so
// the multi-stage chain stays end-to-end validatable; deploy it only
// if real OCR pre-processing later becomes useful.
func main() {
	ctx := context.Background()

	table := mustEnv("LADY_GLASS_TABLE")
	bucket := mustEnv("LADY_GLASS_BUCKET")
	geminiURL := mustEnv("LADY_GLASS_QUEUE_GEMINI")

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("load aws config: %v", err)
	}

	objects := object.NewS3Store(s3.NewFromConfig(cfg), bucket)
	st := store.NewDynamoStore(dynamodb.NewFromConfig(cfg), table)
	q := queue.NewSQSQueue(sqs.NewFromConfig(cfg), map[string]string{
		"gemini": geminiURL,
	})

	step := &lineocr.Mock{Objects: objects}

	ex := &executor.Executor{
		Step: step,
		NextStage: &pipeline.StageSpec{
			Name:      "gemini",
			Version:   "v1",
			QueueName: "gemini",
		},
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
