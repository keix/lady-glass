package api_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"

	"github.com/keix/lady-glass/internal/api"
	"github.com/keix/lady-glass/internal/object"
	"github.com/keix/lady-glass/internal/pipeline"
	"github.com/keix/lady-glass/internal/store"
	"github.com/keix/lady-glass/internal/workflow"
)

// --- fakes -----------------------------------------------------------

type fakePresigner struct {
	url       string
	expiresAt time.Time
	err       error
}

func (f *fakePresigner) PresignPut(_ context.Context, _, _ string, _ time.Duration) (string, time.Time, error) {
	if f.err != nil {
		return "", time.Time{}, f.err
	}
	return f.url, f.expiresAt, nil
}

type fakeSFn struct {
	executionARN string
	lastInput    string
	err          error
}

func (f *fakeSFn) StartExecution(_ context.Context, _, input string) (string, error) {
	if f.err != nil {
		return "", f.err
	}
	f.lastInput = input
	return f.executionARN, nil
}

// --- harness ---------------------------------------------------------

func newHandler(t *testing.T) (*api.Handler, *fakePresigner, *fakeSFn) {
	t.Helper()
	pre := &fakePresigner{
		url:       "https://example/presigned",
		expiresAt: time.Date(2026, 6, 26, 12, 0, 0, 0, time.UTC),
	}
	sf := &fakeSFn{executionARN: "arn:aws:states:...:execution:e1"}
	h := &api.Handler{
		Store:           store.NewMemoryStore(),
		Objects:         object.NewFileStore(t.TempDir()),
		Presigner:       pre,
		SFn:             sf,
		Bucket:          "bkt",
		StateMachineARN: "arn:aws:states:...:stateMachine:wf",
		FirstQueue:      "gemini",
		FinalStage:      "gemini",
		FinalVersion:    "v1",
		APIKey:          "secret",
		UploadExpiresIn: 15 * time.Minute,
		NewJobID:        func() string { return "job_test_001" },
	}
	return h, pre, sf
}

func authHeaders() map[string]string {
	return map[string]string{"x-api-key": "secret"}
}

func makeReq(method, path, body string, query map[string]string) events.APIGatewayV2HTTPRequest {
	r := events.APIGatewayV2HTTPRequest{
		RawPath:               path,
		Body:                  body,
		Headers:               authHeaders(),
		QueryStringParameters: query,
	}
	r.RequestContext.HTTP.Method = method
	return r
}

func decode[T any](t *testing.T, body string) T {
	t.Helper()
	var v T
	if err := json.Unmarshal([]byte(body), &v); err != nil {
		t.Fatalf("decode %s: %v", body, err)
	}
	return v
}

// --- POST /jobs ------------------------------------------------------

func TestCreateJob_HappyPath(t *testing.T) {
	h, _, _ := newHandler(t)

	in := api.CreateJobRequest{Filename: "smbc.pdf"}
	body, _ := json.Marshal(in)
	resp, _ := h.Handle(context.Background(), makeReq("POST", "/jobs", string(body), nil))

	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, body = %s", resp.StatusCode, resp.Body)
	}
	out := decode[api.CreateJobResponse](t, resp.Body)
	if out.JobID != "job_test_001" {
		t.Fatalf("job_id = %q", out.JobID)
	}
	if out.UploadURL != "https://example/presigned" {
		t.Fatalf("upload_url = %q", out.UploadURL)
	}
	if !strings.HasSuffix(out.ExpiresAt, "Z") {
		t.Fatalf("expires_at not RFC3339 Zulu: %q", out.ExpiresAt)
	}

	// JobRecord was persisted with status=created and the input s3 URI.
	rec, err := h.Store.GetJob(context.Background(), "job_test_001")
	if err != nil || rec == nil {
		t.Fatalf("job record missing: %v", err)
	}
	if rec.Status != store.JobStatusCreated {
		t.Fatalf("status = %q", rec.Status)
	}
	if !strings.HasPrefix(rec.InputURI, "s3://bkt/jobs/job_test_001/input.pdf") {
		t.Fatalf("input_uri = %q", rec.InputURI)
	}
}

func TestCreateJob_RejectsMissingFilename(t *testing.T) {
	h, _, _ := newHandler(t)
	body, _ := json.Marshal(api.CreateJobRequest{})

	resp, _ := h.Handle(context.Background(), makeReq("POST", "/jobs", string(body), nil))

	if resp.StatusCode != 400 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	out := decode[api.ErrorResponse](t, resp.Body)
	if out.Error != api.ErrCodeBadRequest {
		t.Fatalf("error = %q", out.Error)
	}
}

// --- POST /jobs/{id}/start ------------------------------------------

func TestStartJob_KicksSFnExecution(t *testing.T) {
	h, _, sf := newHandler(t)
	ctx := context.Background()

	// Pre-stage a job created by an earlier POST /jobs.
	if err := h.Store.PutJob(ctx, store.JobRecord{
		JobID:    "job_x",
		Status:   store.JobStatusCreated,
		InputURI: "s3://bkt/jobs/job_x/input.pdf",
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	resp, _ := h.Handle(ctx, makeReq("POST", "/jobs/job_x/start", "", nil))
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, body = %s", resp.StatusCode, resp.Body)
	}
	out := decode[api.StartJobResponse](t, resp.Body)
	if out.ExecutionARN != "arn:aws:states:...:execution:e1" {
		t.Fatalf("execution_arn = %q", out.ExecutionARN)
	}

	// SFn input round-trips the job_id and the final_stage/version.
	var sfnInput map[string]any
	_ = json.Unmarshal([]byte(sf.lastInput), &sfnInput)
	if sfnInput["job_id"] != "job_x" {
		t.Fatalf("sfn input job_id = %v", sfnInput["job_id"])
	}
	if sfnInput["final_stage"] != "gemini" || sfnInput["final_version"] != "v1" {
		t.Fatalf("sfn input chain = %v", sfnInput)
	}
}

func TestStartJob_RejectsMissingJob(t *testing.T) {
	h, _, _ := newHandler(t)
	resp, _ := h.Handle(context.Background(), makeReq("POST", "/jobs/missing/start", "", nil))
	if resp.StatusCode != 404 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if decode[api.ErrorResponse](t, resp.Body).Error != api.ErrCodeNotFound {
		t.Fatalf("error token = %q", decode[api.ErrorResponse](t, resp.Body).Error)
	}
}

// --- GET /jobs/{id} --------------------------------------------------

func TestStatus_AggregatesPerPageCounts(t *testing.T) {
	h, _, _ := newHandler(t)
	ctx := context.Background()

	if err := h.Store.PutJob(ctx, store.JobRecord{
		JobID:     "job_status",
		Status:    store.JobStatusRunning,
		InputURI:  "s3://bkt/jobs/job_status/input.pdf",
		PageCount: 3,
		UpdatedAt: "2026-06-26T00:00:00Z",
	}); err != nil {
		t.Fatalf("seed job: %v", err)
	}
	if err := h.Store.MarkSucceeded(ctx, pipeline.StepOutput{
		JobID: "job_status", Page: 1, Stage: "gemini", Version: "v1",
		ResultURI: "file://stub",
	}, ""); err != nil {
		t.Fatalf("seed page 1: %v", err)
	}
	if err := h.Store.MarkFailed(ctx, pipeline.StepInput{
		JobID: "job_status", Page: 2, Stage: "gemini", Version: "v1",
	}, errors.New("boom")); err != nil {
		t.Fatalf("seed page 2: %v", err)
	}

	resp, _ := h.Handle(ctx, makeReq("GET", "/jobs/job_status", "", nil))
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, body = %s", resp.StatusCode, resp.Body)
	}
	out := decode[api.JobStatusResponse](t, resp.Body)
	if out.SucceededCount != 1 || out.FailedCount != 1 || out.PendingCount != 1 {
		t.Fatalf("counts = %+v", out)
	}
	if out.Status != api.JobStatusRunning {
		t.Fatalf("status = %q", out.Status)
	}
}

// --- GET /jobs/{id}/result -------------------------------------------

func seedSucceededWithMerged(t *testing.T, h *api.Handler, jobID string, merged workflow.MergedDocument) {
	t.Helper()
	ctx := context.Background()
	body, err := json.Marshal(merged)
	if err != nil {
		t.Fatalf("marshal merged: %v", err)
	}
	uri, err := h.Objects.PutBytes(ctx, fmt.Sprintf("jobs/%s/merged/v1/result.json", jobID), body, "application/json")
	if err != nil {
		t.Fatalf("write merged: %v", err)
	}
	if err := h.Store.PutJob(ctx, store.JobRecord{
		JobID:     jobID,
		Status:    store.JobStatusSucceeded,
		PageCount: merged.PageCount,
		ResultURI: uri,
	}); err != nil {
		t.Fatalf("seed job: %v", err)
	}
}

func mergedFor(jobID string, results ...pipeline.PageExtractionResult) workflow.MergedDocument {
	pages := make([]workflow.MergedPage, len(results))
	for i, r := range results {
		body, _ := json.Marshal(r)
		pages[i] = workflow.MergedPage{Page: i + 1, Result: body}
	}
	return workflow.MergedDocument{
		JobID:     jobID,
		PageCount: len(results),
		Pages:     pages,
	}
}

func TestResult_ReturnsTypedPages(t *testing.T) {
	h, _, _ := newHandler(t)

	seedSucceededWithMerged(t, h, "job_r",
		mergedFor("job_r", pipeline.PageExtractionResult{
			Text:         "page 1 transcription",
			DocumentType: pipeline.DocumentTypeCreditCardStatement,
			Transactions: []pipeline.Transaction{
				{Date: "26/06/22", Merchant: "ファミリーマート", Amount: "150", Currency: "JPY"},
			},
		}),
	)

	resp, _ := h.Handle(context.Background(), makeReq("GET", "/jobs/job_r/result", "", nil))
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, body = %s", resp.StatusCode, resp.Body)
	}
	out := decode[api.ResultResponse](t, resp.Body)
	if out.PageCount != 1 || len(out.Pages) != 1 {
		t.Fatalf("pages = %+v", out)
	}
	page := out.Pages[0]
	if page.Result.DocumentType != pipeline.DocumentTypeCreditCardStatement {
		t.Fatalf("document_type = %q", page.Result.DocumentType)
	}
	if len(page.Result.Transactions) != 1 || page.Result.Transactions[0].Merchant != "ファミリーマート" {
		t.Fatalf("transactions = %+v", page.Result.Transactions)
	}
}

func TestResult_RejectsRunningJob(t *testing.T) {
	h, _, _ := newHandler(t)
	if err := h.Store.PutJob(context.Background(), store.JobRecord{
		JobID:  "job_p",
		Status: store.JobStatusRunning,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	resp, _ := h.Handle(context.Background(), makeReq("GET", "/jobs/job_p/result", "", nil))
	if resp.StatusCode != 409 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if decode[api.ErrorResponse](t, resp.Body).Error != api.ErrCodeJobNotReady {
		t.Fatalf("error token = %q", decode[api.ErrorResponse](t, resp.Body).Error)
	}
}

// --- GET /jobs/{id}/aggregate ----------------------------------------

func TestAggregate_SumsMatchingMerchant(t *testing.T) {
	h, _, _ := newHandler(t)

	seedSucceededWithMerged(t, h, "job_a", mergedFor("job_a",
		pipeline.PageExtractionResult{
			Text:         "p1",
			DocumentType: pipeline.DocumentTypeCreditCardStatement,
			Transactions: []pipeline.Transaction{
				{Date: "26/06/22", Merchant: "ファミリーマート", Amount: "150"},
				{Date: "26/06/22", Merchant: "セブン-イレブン", Amount: "140"},
				{Date: "26/06/21", Merchant: "ファミリーマート", Amount: "1,680"},
			},
		},
		pipeline.PageExtractionResult{
			Text:         "p2",
			DocumentType: pipeline.DocumentTypeCreditCardStatement,
			Transactions: []pipeline.Transaction{
				{Date: "26/06/20", Merchant: "ファミリーマート", Amount: "401"},
			},
		},
	))

	resp, _ := h.Handle(context.Background(),
		makeReq("GET", "/jobs/job_a/aggregate", "", map[string]string{"merchant": "ファミリーマート"}))

	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, body = %s", resp.StatusCode, resp.Body)
	}
	out := decode[api.AggregateResponse](t, resp.Body)
	if out.Count != 3 {
		t.Fatalf("count = %d, want 3", out.Count)
	}
	if out.TotalJPY != 150+1680+401 {
		t.Fatalf("total_jpy = %d, want %d", out.TotalJPY, 150+1680+401)
	}
	if out.Currency != "JPY" {
		t.Fatalf("currency = %q", out.Currency)
	}
	if len(out.Transactions) != 3 {
		t.Fatalf("matched transactions = %d", len(out.Transactions))
	}
	// Page numbers are attached for the breakdown.
	if out.Transactions[0].Page != 1 || out.Transactions[2].Page != 2 {
		t.Fatalf("page tags = %+v", out.Transactions)
	}
}

func TestAggregate_RejectsMissingMerchant(t *testing.T) {
	h, _, _ := newHandler(t)
	seedSucceededWithMerged(t, h, "job_a", mergedFor("job_a"))

	resp, _ := h.Handle(context.Background(), makeReq("GET", "/jobs/job_a/aggregate", "", nil))
	if resp.StatusCode != 400 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

// --- auth + routing --------------------------------------------------

func TestAuth_RejectsMissingApiKey(t *testing.T) {
	h, _, _ := newHandler(t)
	req := events.APIGatewayV2HTTPRequest{RawPath: "/jobs", Body: "{}"}
	req.RequestContext.HTTP.Method = "POST"
	// No Headers, no X-Api-Key.

	resp, _ := h.Handle(context.Background(), req)
	if resp.StatusCode != 401 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if decode[api.ErrorResponse](t, resp.Body).Error != api.ErrCodeUnauthorized {
		t.Fatalf("error token = %q", decode[api.ErrorResponse](t, resp.Body).Error)
	}
}

func TestRoute_UnknownPathReturns404(t *testing.T) {
	h, _, _ := newHandler(t)
	resp, _ := h.Handle(context.Background(), makeReq("GET", "/nope", "", nil))
	if resp.StatusCode != 404 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}
