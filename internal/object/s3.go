package object

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithy "github.com/aws/smithy-go"
)

// S3Client is the subset of the AWS S3 client used by S3Store. Tests
// substitute a fake; the real SDK Client satisfies it directly.
type S3Client interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
}

type S3Store struct {
	Client S3Client
	Bucket string
}

func NewS3Store(client S3Client, bucket string) *S3Store {
	return &S3Store{Client: client, Bucket: bucket}
}

func (s *S3Store) Get(ctx context.Context, uri string) ([]byte, error) {
	bucket, key, err := parseS3URI(uri)
	if err != nil {
		return nil, err
	}

	out, err := s.Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("object: get %q: %w", uri, err)
	}
	defer out.Body.Close()

	body, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("object: read %q: %w", uri, err)
	}
	return body, nil
}

func (s *S3Store) PutJSON(ctx context.Context, key string, v any) (string, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("object: marshal %q: %w", key, err)
	}
	return s.PutBytes(ctx, key, body, "application/json")
}

func (s *S3Store) PutText(ctx context.Context, key string, text string) (string, error) {
	return s.PutBytes(ctx, key, []byte(text), "text/plain")
}

func (s *S3Store) PutBytes(ctx context.Context, key string, body []byte, contentType string) (string, error) {
	if _, err := s.Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.Bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(body),
		ContentType: aws.String(contentType),
	}); err != nil {
		return "", fmt.Errorf("object: put %q: %w", key, err)
	}
	return fmt.Sprintf("s3://%s/%s", s.Bucket, key), nil
}

// URIFor is the s3:// URI at which key would land after a subsequent
// Put. Mirrors the return of PutBytes exactly so callers can pre-compute
// the URI without touching the network.
func (s *S3Store) URIFor(key string) string {
	return fmt.Sprintf("s3://%s/%s", s.Bucket, key)
}

// Exists issues a HeadObject and maps NoSuchKey / 404 to (false, nil).
// Any other error surfaces so callers do not silently proceed on a
// transient network failure.
func (s *S3Store) Exists(ctx context.Context, uri string) (bool, error) {
	bucket, key, err := parseS3URI(uri)
	if err != nil {
		return false, err
	}
	if _, err := s.Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}); err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			return false, nil
		}
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			// S3 HeadObject returns NotFound (not NoSuchKey) when the
			// key is absent; match on the code so both spellings behave
			// the same way.
			if apiErr.ErrorCode() == "NotFound" || apiErr.ErrorCode() == "NoSuchKey" {
				return false, nil
			}
		}
		return false, fmt.Errorf("object: head %q: %w", uri, err)
	}
	return true, nil
}

func parseS3URI(uri string) (bucket, key string, err error) {
	const prefix = "s3://"
	if !strings.HasPrefix(uri, prefix) {
		return "", "", fmt.Errorf("object: %q is not an s3 URI", uri)
	}
	rest := strings.TrimPrefix(uri, prefix)
	slash := strings.Index(rest, "/")
	if slash <= 0 || slash == len(rest)-1 {
		return "", "", fmt.Errorf("object: %q has no key", uri)
	}
	return rest[:slash], rest[slash+1:], nil
}
