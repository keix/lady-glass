package store_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/keix/lady-glass/internal/pipeline"
	"github.com/keix/lady-glass/internal/store"
)

type fakeDynamoClient struct {
	items   map[string]map[string]types.AttributeValue
	lastPut *dynamodb.PutItemInput
	getErr  error
	putErr  error
}

func newFakeDynamoClient() *fakeDynamoClient {
	return &fakeDynamoClient{items: make(map[string]map[string]types.AttributeValue)}
}

func keyOf(item map[string]types.AttributeValue) string {
	pk := item["pk"].(*types.AttributeValueMemberS).Value
	sk := item["sk"].(*types.AttributeValueMemberS).Value
	return pk + "|" + sk
}

func (f *fakeDynamoClient) GetItem(_ context.Context, in *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	if item, ok := f.items[keyOf(in.Key)]; ok {
		return &dynamodb.GetItemOutput{Item: item}, nil
	}
	return &dynamodb.GetItemOutput{}, nil
}

func (f *fakeDynamoClient) PutItem(_ context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	if f.putErr != nil {
		return nil, f.putErr
	}
	f.items[keyOf(in.Item)] = in.Item
	f.lastPut = in
	return &dynamodb.PutItemOutput{}, nil
}

func TestDynamoStore_GetStage_ReturnsNilWhenAbsent(t *testing.T) {
	fake := newFakeDynamoClient()
	st := store.NewDynamoStore(fake, "lady-glass")

	rec, err := st.GetStage(context.Background(), "missing-job", 1, "line_ocr", "v1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if rec != nil {
		t.Fatalf("expected nil record, got %+v", rec)
	}
}

func TestDynamoStore_MarkAndGet_RoundTripsSucceeded(t *testing.T) {
	fake := newFakeDynamoClient()
	st := store.NewDynamoStore(fake, "lady-glass")
	ctx := context.Background()

	out := pipeline.StepOutput{
		JobID:     "job_x",
		Page:      17,
		Stage:     "line_ocr",
		Version:   "v1",
		ResultURI: "s3://bkt/jobs/job_x/pages/000017/line_ocr/v1/result.json",
	}
	if err := st.MarkSucceeded(ctx, out, "gemini"); err != nil {
		t.Fatalf("mark succeeded: %v", err)
	}

	if pk := aws.ToString(fake.lastPut.TableName); pk != "lady-glass" {
		t.Fatalf("table = %q", pk)
	}
	if got := fake.lastPut.Item["sk"].(*types.AttributeValueMemberS).Value; got != "PAGE#000017#STAGE#line_ocr#v1" {
		t.Fatalf("sk = %q", got)
	}

	rec, err := st.GetStage(ctx, "job_x", 17, "line_ocr", "v1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if rec == nil {
		t.Fatal("expected record, got nil")
	}
	if rec.Status != store.StageStatusSucceeded {
		t.Fatalf("status = %q, want succeeded", rec.Status)
	}
	if rec.ResultURI != out.ResultURI {
		t.Fatalf("result_uri = %q", rec.ResultURI)
	}
	if rec.NextStage != "gemini" {
		t.Fatalf("next_stage = %q", rec.NextStage)
	}
	if rec.IdempotencyKey != "job_x:page:000017:line_ocr:v1" {
		t.Fatalf("idempotency_key = %q", rec.IdempotencyKey)
	}
}

func TestDynamoStore_MarkRunning_SetsRunningStatusAndInputURI(t *testing.T) {
	fake := newFakeDynamoClient()
	st := store.NewDynamoStore(fake, "lady-glass")

	in := pipeline.StepInput{
		JobID:    "job_y",
		Page:     1,
		Stage:    "line_ocr",
		Version:  "v1",
		InputURI: "s3://bkt/jobs/job_y/pages/000001/input.png",
	}
	if err := st.MarkRunning(context.Background(), in); err != nil {
		t.Fatalf("mark running: %v", err)
	}

	rec, err := st.GetStage(context.Background(), "job_y", 1, "line_ocr", "v1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if rec.Status != store.StageStatusRunning {
		t.Fatalf("status = %q, want running", rec.Status)
	}
	if rec.InputURI != in.InputURI {
		t.Fatalf("input_uri = %q", rec.InputURI)
	}
}

func TestDynamoStore_MarkFailed_RecordsErrorMessage(t *testing.T) {
	fake := newFakeDynamoClient()
	st := store.NewDynamoStore(fake, "lady-glass")

	in := pipeline.StepInput{JobID: "job_z", Page: 1, Stage: "gemini", Version: "v1"}
	if err := st.MarkFailed(context.Background(), in, errors.New("simulated step failure")); err != nil {
		t.Fatalf("mark failed: %v", err)
	}

	rec, err := st.GetStage(context.Background(), "job_z", 1, "gemini", "v1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if rec.Status != store.StageStatusFailed {
		t.Fatalf("status = %q, want failed", rec.Status)
	}
	if rec.Error != "simulated step failure" {
		t.Fatalf("error = %q", rec.Error)
	}
}

func TestDynamoStore_PageZero_UsesJobLevelSortKey(t *testing.T) {
	fake := newFakeDynamoClient()
	st := store.NewDynamoStore(fake, "lady-glass")

	out := pipeline.StepOutput{
		JobID:     "job_merge",
		Page:      0,
		Stage:     "merge",
		Version:   "v1",
		ResultURI: "s3://bkt/jobs/job_merge/merged/v1/result.json",
	}
	if err := st.MarkSucceeded(context.Background(), out, ""); err != nil {
		t.Fatalf("mark: %v", err)
	}
	if got := fake.lastPut.Item["sk"].(*types.AttributeValueMemberS).Value; got != "STAGE#merge#v1" {
		t.Fatalf("sk = %q, want STAGE#merge#v1", got)
	}
}

func TestDynamoStore_PutError_IsWrapped(t *testing.T) {
	fake := newFakeDynamoClient()
	fake.putErr = errors.New("simulated put failure")
	st := store.NewDynamoStore(fake, "lady-glass")

	err := st.MarkRunning(context.Background(), pipeline.StepInput{JobID: "j", Page: 1, Stage: "line_ocr", Version: "v1"})
	if err == nil {
		t.Fatal("expected wrapped put error, got nil")
	}
	if !strings.Contains(err.Error(), "simulated put failure") {
		t.Fatalf("error %q does not include underlying SDK error", err)
	}
}
