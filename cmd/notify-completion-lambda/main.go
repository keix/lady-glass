package main

import (
	"context"
	"log"
	"os"
	"strconv"

	awslambda "github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/keix/lady-glass/internal/notify"
	"github.com/keix/lady-glass/internal/store"
	"github.com/keix/lady-glass/internal/workflow"
)

// notify-completion-lambda is the post-commit observer (SPEC §S11).
// It runs after either Merge (succeeded) or MarkJobFailed (failed),
// reads the JobRecord to discover which terminal state was committed,
// and invokes the matching Notifier endpoint. It is the only workflow
// Lambda that intentionally observes — and never mutates — the
// JobRecord.
//
// In v0 the Notifier is the silent NoOp: the boundary exists but
// nothing is on the other side. Replace with a webhook / Slack /
// EventBridge implementation here when an external subscriber lands.
//
// Required env:
//
//	LADY_GLASS_TABLE             DynamoDB table name
//
// Optional env (with defaults):
//
//	LADY_GLASS_RETENTION_DAYS    DDB TTL window (SPEC §S9)  (14)
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

	notifier := notify.Notifier(notify.NoOp{})

	awslambda.Start(func(ctx context.Context, in workflow.NotifyCompletionInput) (workflow.NotifyCompletionOutput, error) {
		return workflow.NotifyCompletion(ctx, in, st, notifier)
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
