package object_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/keix/lady-glass/internal/object"
)

type fakeS3Client struct {
	objects map[string][]byte
	lastPut *s3.PutObjectInput
	putErr  error
	getErr  error
}

func newFakeS3Client() *fakeS3Client {
	return &fakeS3Client{objects: make(map[string][]byte)}
}

func (f *fakeS3Client) PutObject(_ context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if f.putErr != nil {
		return nil, f.putErr
	}
	body, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}
	f.objects[aws.ToString(in.Bucket)+"/"+aws.ToString(in.Key)] = body
	f.lastPut = in
	return &s3.PutObjectOutput{}, nil
}

func (f *fakeS3Client) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	body, ok := f.objects[aws.ToString(in.Bucket)+"/"+aws.ToString(in.Key)]
	if !ok {
		return nil, fmt.Errorf("object not found: %s/%s", aws.ToString(in.Bucket), aws.ToString(in.Key))
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(body))}, nil
}

func TestS3Store_PutBytes_PutsAndReturnsURI(t *testing.T) {
	fake := newFakeS3Client()
	store := object.NewS3Store(fake, "lady-glass-bucket")

	uri, err := store.PutBytes(context.Background(), "jobs/j1/raw.bin", []byte("hello"), "application/octet-stream")
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if uri != "s3://lady-glass-bucket/jobs/j1/raw.bin" {
		t.Fatalf("uri = %q", uri)
	}
	if got := aws.ToString(fake.lastPut.ContentType); got != "application/octet-stream" {
		t.Fatalf("content type = %q, want application/octet-stream", got)
	}
}

func TestS3Store_PutJSON_RoundTripsThroughGet(t *testing.T) {
	fake := newFakeS3Client()
	store := object.NewS3Store(fake, "bkt")

	type payload struct {
		Page int    `json:"page"`
		Text string `json:"text"`
	}
	in := payload{Page: 17, Text: "mock"}

	uri, err := store.PutJSON(context.Background(), "jobs/j1/page.json", in)
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	body, err := store.Get(context.Background(), uri)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	var out payload
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out != in {
		t.Fatalf("round-trip = %+v, want %+v", out, in)
	}
	if got := aws.ToString(fake.lastPut.ContentType); got != "application/json" {
		t.Fatalf("content type = %q, want application/json", got)
	}
}

func TestS3Store_PutText_UsesTextContentType(t *testing.T) {
	fake := newFakeS3Client()
	store := object.NewS3Store(fake, "bkt")

	if _, err := store.PutText(context.Background(), "jobs/j1/text.txt", "abc"); err != nil {
		t.Fatalf("put: %v", err)
	}
	if got := aws.ToString(fake.lastPut.ContentType); got != "text/plain" {
		t.Fatalf("content type = %q, want text/plain", got)
	}
}

func TestS3Store_Get_RejectsNonS3URI(t *testing.T) {
	fake := newFakeS3Client()
	store := object.NewS3Store(fake, "bkt")

	cases := []string{
		"file:///tmp/foo",
		"s3://only-bucket",
		"s3://bucket/",
	}
	for _, uri := range cases {
		t.Run(uri, func(t *testing.T) {
			_, err := store.Get(context.Background(), uri)
			if err == nil {
				t.Fatalf("expected error for %q, got nil", uri)
			}
		})
	}
}

func TestS3Store_Put_WrapsSDKError(t *testing.T) {
	fake := newFakeS3Client()
	fake.putErr = errors.New("simulated put failure")
	store := object.NewS3Store(fake, "bkt")

	_, err := store.PutBytes(context.Background(), "k", []byte("x"), "application/octet-stream")
	if err == nil {
		t.Fatal("expected error from failing PutObject, got nil")
	}
	if !strings.Contains(err.Error(), "simulated put failure") {
		t.Fatalf("error %q does not include underlying SDK error", err)
	}
}
