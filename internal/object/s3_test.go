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
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithy "github.com/aws/smithy-go"

	"github.com/keix/lady-glass/internal/object"
)

type fakeS3Client struct {
	objects map[string][]byte
	lastPut *s3.PutObjectInput
	putErr  error
	getErr  error
	headErr error
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

func (f *fakeS3Client) HeadObject(_ context.Context, in *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	if f.headErr != nil {
		return nil, f.headErr
	}
	key := aws.ToString(in.Bucket) + "/" + aws.ToString(in.Key)
	if _, ok := f.objects[key]; !ok {
		// The real SDK returns a NotFound API error with code
		// "NotFound"; the fake reproduces that shape so S3Store.Exists
		// exercises its NotFound-mapping branch.
		return nil, &smithy.GenericAPIError{Code: "NotFound", Message: "not found"}
	}
	return &s3.HeadObjectOutput{}, nil
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

func TestS3Store_Exists_ReturnsTrueForPresentObject(t *testing.T) {
	fake := newFakeS3Client()
	store := object.NewS3Store(fake, "bkt")

	uri, err := store.PutBytes(context.Background(), "manifests/j.json", []byte("{}"), "application/json")
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	ok, err := store.Exists(context.Background(), uri)
	if err != nil {
		t.Fatalf("exists: %v", err)
	}
	if !ok {
		t.Fatal("exists = false, want true")
	}
}

func TestS3Store_Exists_ReturnsFalseForNotFound(t *testing.T) {
	// The archive-result stage relies on this branch to short-circuit
	// its rerun: HeadObject → NotFound must be (false, nil), not an
	// error that stops the stage from proceeding.
	fake := newFakeS3Client()
	store := object.NewS3Store(fake, "bkt")

	ok, err := store.Exists(context.Background(), "s3://bkt/nothing-here.json")
	if err != nil {
		t.Fatalf("exists: %v", err)
	}
	if ok {
		t.Fatal("exists = true, want false for missing object")
	}
}

func TestS3Store_Exists_ReturnsFalseForNoSuchKey(t *testing.T) {
	// LocalStack / some SDK code paths surface NoSuchKey (the typed
	// exception) instead of a generic NotFound APIError. Both spellings
	// must map to (false, nil).
	fake := newFakeS3Client()
	fake.headErr = &types.NoSuchKey{}
	store := object.NewS3Store(fake, "bkt")

	ok, err := store.Exists(context.Background(), "s3://bkt/whatever.json")
	if err != nil {
		t.Fatalf("exists: %v", err)
	}
	if ok {
		t.Fatal("exists = true, want false")
	}
}

func TestS3Store_Exists_SurfacesTransportError(t *testing.T) {
	// A non-NotFound HeadObject error is a transport / permissions
	// failure; the stage MUST NOT interpret that as "safe to write" or
	// the retry story turns into an accidental overwrite.
	fake := newFakeS3Client()
	fake.headErr = errors.New("simulated head failure")
	store := object.NewS3Store(fake, "bkt")

	if _, err := store.Exists(context.Background(), "s3://bkt/k.json"); err == nil {
		t.Fatal("expected transport error to propagate, got nil")
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
