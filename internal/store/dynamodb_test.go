package store_test

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/keix/lady-glass/internal/pipeline"
	"github.com/keix/lady-glass/internal/store"
)

type fakeDynamoClient struct {
	items    map[string]map[string]types.AttributeValue
	lastPut  *dynamodb.PutItemInput
	getErr   error
	putErr   error
	queryErr error
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

// Query honours the "PK = :pk AND begins_with(SK, :sk_prefix)" pattern
// emitted by DynamoStore.ListStagesByJob. Extra Query shapes can be
// added when the production code starts using them.
func (f *fakeDynamoClient) Query(_ context.Context, in *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	if f.queryErr != nil {
		return nil, f.queryErr
	}
	pk := in.ExpressionAttributeValues[":pk"].(*types.AttributeValueMemberS).Value
	skPrefix := in.ExpressionAttributeValues[":sk_prefix"].(*types.AttributeValueMemberS).Value

	var matches []map[string]types.AttributeValue
	for key, item := range f.items {
		if !strings.HasPrefix(key, pk+"|"+skPrefix) {
			continue
		}
		matches = append(matches, item)
	}
	return &dynamodb.QueryOutput{Items: matches}, nil
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

func TestDynamoStore_GetJob_ReturnsNilWhenAbsent(t *testing.T) {
	fake := newFakeDynamoClient()
	st := store.NewDynamoStore(fake, "lady-glass")

	rec, err := st.GetJob(context.Background(), "missing-job")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if rec != nil {
		t.Fatalf("expected nil record, got %+v", rec)
	}
}

func TestDynamoStore_PutAndGetJob_RoundTrips(t *testing.T) {
	fake := newFakeDynamoClient()
	st := store.NewDynamoStore(fake, "lady-glass")
	ctx := context.Background()

	err := st.PutJob(ctx, store.JobRecord{
		JobID:     "job_x",
		Status:    store.JobStatusRunning,
		InputURI:  "s3://bkt/jobs/job_x/input.pdf",
		PageCount: 3,
	})
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	if got := fake.lastPut.Item["sk"].(*types.AttributeValueMemberS).Value; got != "META" {
		t.Fatalf("sk = %q, want META", got)
	}

	rec, err := st.GetJob(ctx, "job_x")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if rec == nil {
		t.Fatal("expected record, got nil")
	}
	if rec.Status != store.JobStatusRunning {
		t.Fatalf("status = %q, want running", rec.Status)
	}
	if rec.PageCount != 3 {
		t.Fatalf("page count = %d, want 3", rec.PageCount)
	}
	if rec.InputURI != "s3://bkt/jobs/job_x/input.pdf" {
		t.Fatalf("input_uri = %q", rec.InputURI)
	}
}

func TestDynamoStore_ListStagesByJob_FiltersAndIgnoresUnrelatedStages(t *testing.T) {
	fake := newFakeDynamoClient()
	st := store.NewDynamoStore(fake, "lady-glass")
	ctx := context.Background()

	// Seed: three gemini v1 pages, one line_ocr v1 page (must be filtered),
	// one gemini v2 page (must be filtered).
	for _, page := range []int{1, 2, 3} {
		if err := st.MarkSucceeded(ctx, pipeline.StepOutput{
			JobID: "job_y", Page: page, Stage: "gemini", Version: "v1",
			ResultURI: "s3://bkt/r",
		}, ""); err != nil {
			t.Fatalf("seed page %d: %v", page, err)
		}
	}
	if err := st.MarkSucceeded(ctx, pipeline.StepOutput{
		JobID: "job_y", Page: 1, Stage: "line_ocr", Version: "v1",
		ResultURI: "s3://bkt/r",
	}, "gemini"); err != nil {
		t.Fatalf("seed line_ocr: %v", err)
	}
	if err := st.MarkSucceeded(ctx, pipeline.StepOutput{
		JobID: "job_y", Page: 2, Stage: "gemini", Version: "v2",
		ResultURI: "s3://bkt/r",
	}, ""); err != nil {
		t.Fatalf("seed v2: %v", err)
	}

	recs, err := st.ListStagesByJob(ctx, "job_y", "gemini", "v1")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(recs) != 3 {
		t.Fatalf("matched %d records, want 3", len(recs))
	}
	for _, r := range recs {
		if r.Stage != "gemini" || r.Version != "v1" {
			t.Fatalf("record %+v leaked through filter", r)
		}
	}
}

func TestDynamoStore_Put_AttachesExpiresAtWhenRetentionConfigured(t *testing.T) {
	fake := newFakeDynamoClient()
	frozen := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	st := &store.DynamoStore{
		Client:        fake,
		Table:         "lady-glass",
		RetentionDays: 14,
		Now:           func() time.Time { return frozen },
	}

	if err := st.PutJob(context.Background(), store.JobRecord{
		JobID:  "job_ttl",
		Status: store.JobStatusRunning,
	}); err != nil {
		t.Fatalf("put job: %v", err)
	}

	got, ok := fake.lastPut.Item["expires_at"].(*types.AttributeValueMemberN)
	if !ok {
		t.Fatalf("expires_at missing or wrong type: %#v", fake.lastPut.Item["expires_at"])
	}
	wantUnix := frozen.Add(14 * 24 * time.Hour).Unix()
	gotUnix, err := strconv.ParseInt(got.Value, 10, 64)
	if err != nil {
		t.Fatalf("expires_at not an integer: %q", got.Value)
	}
	if gotUnix != wantUnix {
		t.Fatalf("expires_at = %d, want %d (now + 14d)", gotUnix, wantUnix)
	}
}

func TestDynamoStore_Put_OmitsExpiresAtWhenRetentionZero(t *testing.T) {
	fake := newFakeDynamoClient()
	st := store.NewDynamoStore(fake, "lady-glass")

	if err := st.PutJob(context.Background(), store.JobRecord{
		JobID:  "job_no_ttl",
		Status: store.JobStatusRunning,
	}); err != nil {
		t.Fatalf("put: %v", err)
	}
	if _, present := fake.lastPut.Item["expires_at"]; present {
		t.Fatalf("expires_at should be omitted when RetentionDays=0")
	}
}

func TestDynamoStore_GetJob_FiltersExpiredRows(t *testing.T) {
	fake := newFakeDynamoClient()
	// Clock starts in 2026-06-01, retention = 14d; we advance the clock
	// past the row's expiry and expect Get to behave as if the row is
	// already gone (defending against DDB's async TTL reaper lag).
	clock := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	st := &store.DynamoStore{
		Client:        fake,
		Table:         "lady-glass",
		RetentionDays: 14,
		Now:           func() time.Time { return clock },
	}

	if err := st.PutJob(context.Background(), store.JobRecord{
		JobID:  "job_expired",
		Status: store.JobStatusSucceeded,
	}); err != nil {
		t.Fatalf("put: %v", err)
	}
	clock = clock.Add(15 * 24 * time.Hour)

	rec, err := st.GetJob(context.Background(), "job_expired")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if rec != nil {
		t.Fatalf("expected nil for expired row, got %+v", rec)
	}
}

func TestDynamoStore_ListStagesByJob_FiltersExpiredRows(t *testing.T) {
	fake := newFakeDynamoClient()
	clock := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	st := &store.DynamoStore{
		Client:        fake,
		Table:         "lady-glass",
		RetentionDays: 14,
		Now:           func() time.Time { return clock },
	}

	// Seed two pages at t0, then advance the clock past their expiry and
	// add a third page so only the third should survive the read-time
	// filter.
	for _, page := range []int{1, 2} {
		if err := st.MarkSucceeded(context.Background(), pipeline.StepOutput{
			JobID: "job_ttl", Page: page, Stage: "gemini", Version: "v1",
			ResultURI: "s3://bkt/r",
		}, ""); err != nil {
			t.Fatalf("seed page %d: %v", page, err)
		}
	}
	clock = clock.Add(15 * 24 * time.Hour)
	if err := st.MarkSucceeded(context.Background(), pipeline.StepOutput{
		JobID: "job_ttl", Page: 3, Stage: "gemini", Version: "v1",
		ResultURI: "s3://bkt/r",
	}, ""); err != nil {
		t.Fatalf("seed page 3: %v", err)
	}

	recs, err := st.ListStagesByJob(context.Background(), "job_ttl", "gemini", "v1")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(recs) != 1 || recs[0].Page != 3 {
		t.Fatalf("expected only page 3 to survive, got %+v", recs)
	}
}

func TestDynamoStore_PutAndGetJob_RoundTripsChain(t *testing.T) {
	fake := newFakeDynamoClient()
	st := store.NewDynamoStore(fake, "lady-glass")
	ctx := context.Background()

	chain := []pipeline.StageSpec{
		{Name: "gemini", Version: "v1", QueueName: "gemini"},
		{Name: "normalize_card_statement", Version: "v1", QueueName: "normalize_card_statement"},
	}
	if err := st.PutJob(ctx, store.JobRecord{
		JobID:   "job_chain",
		Status:  store.JobStatusCreated,
		ChainID: "card-statement-v1",
		Chain:   chain,
	}); err != nil {
		t.Fatalf("put: %v", err)
	}

	rec, err := st.GetJob(ctx, "job_chain")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if rec.ChainID != "card-statement-v1" {
		t.Fatalf("ChainID = %q", rec.ChainID)
	}
	if len(rec.Chain) != 2 {
		t.Fatalf("Chain = %+v, want 2 stages", rec.Chain)
	}
	if rec.Chain[0].Name != "gemini" || rec.Chain[1].Name != "normalize_card_statement" {
		t.Fatalf("Chain ordering lost: %+v", rec.Chain)
	}
}

func TestDynamoStore_LegacyRowReadsEmptyChain(t *testing.T) {
	// A row written before the chain-binding feature has no chain_id
	// or chain_json attribute. GetJob must read it back with empty
	// ChainID and nil Chain so the API layer's fallback to
	// chain.DefaultChainID can kick in.
	fake := newFakeDynamoClient()
	st := store.NewDynamoStore(fake, "lady-glass")
	ctx := context.Background()

	if err := st.PutJob(ctx, store.JobRecord{
		JobID:  "job_legacy",
		Status: store.JobStatusSucceeded,
	}); err != nil {
		t.Fatalf("put: %v", err)
	}

	rec, err := st.GetJob(ctx, "job_legacy")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if rec.ChainID != "" || len(rec.Chain) != 0 {
		t.Fatalf("legacy row should round-trip empty chain, got id=%q chain=%+v", rec.ChainID, rec.Chain)
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
