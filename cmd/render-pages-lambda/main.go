package main

import (
	"context"
	"log"
	"os"

	awslambda "github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/keix/lady-glass/internal/object"
	"github.com/keix/lady-glass/internal/workflow"
)

// render-pages-lambda is the SFN task that splits a source PDF into
// one-page PDFs on the workflow's rendered branch. It is bypassed
// entirely on the passthrough branch — the ASL's ModeChoice routes
// straight to SubmitPages when $.mode != "rendered".
//
// Required env:
//
//	LADY_GLASS_BUCKET   S3 bucket for artifacts
func main() {
	ctx := context.Background()

	bucket := mustEnv("LADY_GLASS_BUCKET")

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("load aws config: %v", err)
	}

	obj := object.NewS3Store(s3.NewFromConfig(cfg), bucket)

	awslambda.Start(func(ctx context.Context, in workflow.RenderPagesInput) (workflow.RenderPagesOutput, error) {
		return workflow.RenderPages(ctx, in, obj)
	})
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("env %s is required", key)
	}
	return v
}
