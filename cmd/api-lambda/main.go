package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-lambda-go/events"
	awslambda "github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sfn"

	"github.com/keix/lady-glass/internal/api"
	"github.com/keix/lady-glass/internal/object"
	"github.com/keix/lady-glass/internal/store"
)

// api-lambda is the HTTP API Lambda. It wraps internal/api.Handler with
// SDK-backed Presigner and SFnRunner implementations and exposes it via
// awslambda.Start.
//
// Required env:
//
//	LADY_GLASS_TABLE                DynamoDB table name
//	LADY_GLASS_BUCKET               S3 artifact bucket
//	LADY_GLASS_STATE_MACHINE_ARN    SFn workflow ARN
//	LADY_GLASS_API_KEY              shared API key (compared against X-Api-Key)
//
// Optional env (with defaults):
//
//	LADY_GLASS_FIRST_QUEUE          first stage queue name        (gemini)
//	LADY_GLASS_FINAL_STAGE          final stage name              (gemini)
//	LADY_GLASS_FINAL_VERSION        final stage version           (v1)
//	LADY_GLASS_UPLOAD_EXPIRES_MIN   presigned PUT validity in min (15)
func main() {
	ctx := context.Background()

	table := mustEnv("LADY_GLASS_TABLE")
	bucket := mustEnv("LADY_GLASS_BUCKET")
	stateMachineARN := mustEnv("LADY_GLASS_STATE_MACHINE_ARN")
	apiKey := mustEnv("LADY_GLASS_API_KEY")

	firstQueue := envDefault("LADY_GLASS_FIRST_QUEUE", "gemini")
	finalStage := envDefault("LADY_GLASS_FINAL_STAGE", "gemini")
	finalVersion := envDefault("LADY_GLASS_FINAL_VERSION", "v1")
	uploadExpiresMin := envIntDefault("LADY_GLASS_UPLOAD_EXPIRES_MIN", 15)

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("load aws config: %v", err)
	}

	s3Client := s3.NewFromConfig(cfg)
	presigner := &sdkPresigner{
		client: s3.NewPresignClient(s3Client),
		bucket: bucket,
	}

	handler := &api.Handler{
		Store:           store.NewDynamoStore(dynamodb.NewFromConfig(cfg), table),
		Objects:         object.NewS3Store(s3Client, bucket),
		Presigner:       presigner,
		SFn:             &sdkSFn{client: sfn.NewFromConfig(cfg)},
		Bucket:          bucket,
		StateMachineARN: stateMachineARN,
		FirstQueue:      firstQueue,
		FinalStage:      finalStage,
		FinalVersion:    finalVersion,
		APIKey:          apiKey,
		UploadExpiresIn: time.Duration(uploadExpiresMin) * time.Minute,
	}

	awslambda.Start(func(ctx context.Context, req events.APIGatewayV2HTTPRequest) (events.APIGatewayV2HTTPResponse, error) {
		return handler.Handle(ctx, req)
	})
}

// sdkPresigner wraps the SDK's S3 PresignClient and satisfies
// api.Presigner.
type sdkPresigner struct {
	client *s3.PresignClient
	bucket string
}

func (p *sdkPresigner) PresignPut(ctx context.Context, key, contentType string, expires time.Duration) (string, time.Time, error) {
	expiresAt := time.Now().Add(expires)
	out, err := p.client.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(p.bucket),
		Key:         aws.String(key),
		ContentType: aws.String(contentType),
	}, s3.WithPresignExpires(expires))
	if err != nil {
		return "", time.Time{}, err
	}
	return out.URL, expiresAt, nil
}

// sdkSFn wraps the SDK's Step Functions client and satisfies
// api.SFnRunner.
type sdkSFn struct {
	client *sfn.Client
}

func (s *sdkSFn) StartExecution(ctx context.Context, stateMachineARN, input string) (string, error) {
	out, err := s.client.StartExecution(ctx, &sfn.StartExecutionInput{
		StateMachineArn: aws.String(stateMachineARN),
		Input:           aws.String(input),
	})
	if err != nil {
		return "", err
	}
	return aws.ToString(out.ExecutionArn), nil
}

// --- env helpers -----------------------------------------------------

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("env %s is required", key)
	}
	return v
}

func envDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
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
