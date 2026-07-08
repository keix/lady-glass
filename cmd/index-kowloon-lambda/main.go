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

	"github.com/keix/lady-glass/internal/client/kowloon"
	"github.com/keix/lady-glass/internal/object"
	"github.com/keix/lady-glass/internal/store"
	"github.com/keix/lady-glass/internal/workflow"
)

// index-kowloon-lambda is the per-job Step Functions task that runs
// after ArchiveResult: the single HTTP hop to Kowloon. It reads the
// manifest ArchiveResult wrote (from the permanent bucket), POSTs the
// archive URI to Kowloon's /v1/index-result, and persists Kowloon's
// typed response as a sidecar so a rerun short-circuits.
//
// The permanent bucket is the ONLY object store this step touches — it
// holds both the manifest (read) and the sidecar (written); the stage
// bucket is irrelevant here.
//
// Auth (choose one, OAuth preferred once Kowloon has OIDC verification):
//   - OAuth2 client_credentials: set KOWLOON_OAUTH_TOKEN_URL (+ client
//     id/secret, audience). The client fetches a bearer token from the
//     provider (Asteroid) and Kowloon verifies it.
//   - X-Api-Key: set KOWLOON_API_KEY. Legacy / pre-OIDC path.
//   - Neither: unauthenticated (loopback / dev Kowloon).
//
// Required env:
//
//	LADY_GLASS_TABLE              DynamoDB table name
//	LADY_GLASS_PERMANENT_BUCKET   permanent bucket (manifest + sidecar)
//	KOWLOON_BASE_URL              Kowloon front door, e.g. https://kowloon.internal
//
// Optional env:
//
//	KOWLOON_OAUTH_TOKEN_URL       provider token endpoint (enables OAuth)
//	KOWLOON_OAUTH_CLIENT_ID       client_credentials client id
//	KOWLOON_OAUTH_CLIENT_SECRET   client_credentials client secret
//	KOWLOON_OAUTH_AUDIENCE        "audience" token param / expected aud
//	KOWLOON_API_KEY               shared X-Api-Key secret (legacy path)
func main() {
	ctx := context.Background()

	table := mustEnv("LADY_GLASS_TABLE")
	permanentBucket := mustEnv("LADY_GLASS_PERMANENT_BUCKET")
	kowloonBaseURL := mustEnv("KOWLOON_BASE_URL")

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("load aws config: %v", err)
	}

	st := store.NewDynamoStore(dynamodb.NewFromConfig(cfg), table)
	destination := object.NewS3Store(s3.NewFromConfig(cfg), permanentBucket)

	// OAuth wins when its token URL is set; otherwise fall back to the
	// X-Api-Key constructor (which also covers the unauthenticated case
	// when the key is empty).
	var client kowloon.Client
	if tokenURL := os.Getenv("KOWLOON_OAUTH_TOKEN_URL"); tokenURL != "" {
		client = kowloon.NewWithOAuth(kowloonBaseURL, kowloon.OAuthConfig{
			TokenURL:     tokenURL,
			ClientID:     os.Getenv("KOWLOON_OAUTH_CLIENT_ID"),
			ClientSecret: os.Getenv("KOWLOON_OAUTH_CLIENT_SECRET"),
			Audience:     os.Getenv("KOWLOON_OAUTH_AUDIENCE"),
		})
	} else {
		client = kowloon.New(kowloonBaseURL, os.Getenv("KOWLOON_API_KEY"))
	}

	awslambda.Start(func(ctx context.Context, in workflow.IndexKowloonInput) (workflow.IndexKowloonOutput, error) {
		return workflow.IndexKowloon(ctx, in, st, destination, client, time.Now)
	})
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("env %s is required", key)
	}
	return v
}
