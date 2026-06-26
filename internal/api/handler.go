package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"

	"github.com/keix/lady-glass/internal/object"
	"github.com/keix/lady-glass/internal/pipeline"
	"github.com/keix/lady-glass/internal/store"
	"github.com/keix/lady-glass/internal/workflow"
)

// Presigner abstracts S3 presigned URL generation so tests can swap a
// fake without spinning up the SDK. The returned ExpiresAt is the
// absolute time the URL stops accepting PUTs.
type Presigner interface {
	PresignPut(ctx context.Context, key, contentType string, expires time.Duration) (url string, expiresAt time.Time, err error)
}

// SFnRunner abstracts Step Functions StartExecution so tests can
// substitute a fake. The input is the raw JSON the state machine's
// SubmitPages task consumes.
type SFnRunner interface {
	StartExecution(ctx context.Context, stateMachineARN, input string) (executionARN string, err error)
}

// Handler holds the per-request dependencies and dispatches the five
// API routes. The HTTP-API Lambda binary constructs this once and
// reuses it for every invocation.
type Handler struct {
	Store     store.Store
	Objects   object.Store
	Presigner Presigner
	SFn       SFnRunner

	// Bucket is the artifact bucket name; presigned URLs and merged
	// reads both go to it.
	Bucket string

	// StateMachineARN is the Lady Glass SFn workflow ARN.
	StateMachineARN string

	// FirstQueue, FinalStage, FinalVersion are the per-chain
	// parameters embedded into SFn execution input. FinalStage /
	// FinalVersion also drive ListStagesByJob in status / result /
	// aggregate.
	FirstQueue   string
	FinalStage   string
	FinalVersion string

	// APIKey is the shared secret the client sends in X-Api-Key.
	// Compared via direct string equality; rotation is by SSM put.
	APIKey string

	// UploadExpiresIn is how long presigned PUT URLs stay valid.
	UploadExpiresIn time.Duration

	// Now is the time source; tests override it.
	Now func() time.Time

	// NewJobID is the job-id generator; tests override it.
	NewJobID func() string
}

// Handle is the Lambda entry point. It checks auth, routes on
// method + path, and returns a typed JSON response.
func (h *Handler) Handle(ctx context.Context, req events.APIGatewayV2HTTPRequest) (events.APIGatewayV2HTTPResponse, error) {
	if err := h.checkAuth(req); err != nil {
		return errorResponse(401, ErrCodeUnauthorized, err.Error()), nil
	}

	method := req.RequestContext.HTTP.Method
	path := req.RawPath

	switch {
	case method == "POST" && path == "/jobs":
		return h.createJob(ctx, req)
	case method == "POST" && hasSuffix(path, "/jobs/", "/start"):
		return h.startJob(ctx, req, idBetween(path, "/jobs/", "/start"))
	case method == "GET" && hasSuffix(path, "/jobs/", "/result"):
		return h.getResult(ctx, req, idBetween(path, "/jobs/", "/result"))
	case method == "GET" && hasSuffix(path, "/jobs/", "/aggregate"):
		return h.aggregate(ctx, req, idBetween(path, "/jobs/", "/aggregate"))
	case method == "GET" && isBareJobPath(path):
		return h.getStatus(ctx, req, strings.TrimPrefix(path, "/jobs/"))
	default:
		return errorResponse(404, ErrCodeNotFound, fmt.Sprintf("no route matches %s %s", method, path)), nil
	}
}

// --- POST /jobs ------------------------------------------------------

func (h *Handler) createJob(ctx context.Context, req events.APIGatewayV2HTTPRequest) (events.APIGatewayV2HTTPResponse, error) {
	var in CreateJobRequest
	if err := json.Unmarshal([]byte(req.Body), &in); err != nil {
		return errorResponse(400, ErrCodeBadRequest, "request body is not JSON"), nil
	}
	if in.Filename == "" {
		return errorResponse(400, ErrCodeBadRequest, "filename is required"), nil
	}

	contentType := in.ContentType
	if contentType == "" {
		contentType = contentTypeForExt(filepath.Ext(in.Filename))
	}

	jobID := h.newJobID()
	key := fmt.Sprintf("jobs/%s/input%s", jobID, filepath.Ext(in.Filename))

	url, expiresAt, err := h.Presigner.PresignPut(ctx, key, contentType, h.UploadExpiresIn)
	if err != nil {
		return errorResponse(500, ErrCodeInternalError, fmt.Sprintf("presign: %v", err)), nil
	}

	mode := in.Mode
	if mode == "" {
		mode = ModePassthrough
	}

	if err := h.Store.PutJob(ctx, store.JobRecord{
		JobID:    jobID,
		Status:   store.JobStatusCreated,
		InputURI: fmt.Sprintf("s3://%s/%s", h.Bucket, key),
		Mode:     string(mode),
	}); err != nil {
		return errorResponse(500, ErrCodeInternalError, fmt.Sprintf("put job: %v", err)), nil
	}

	return jsonResponse(200, CreateJobResponse{
		JobID:     jobID,
		UploadURL: url,
		ExpiresAt: expiresAt.UTC().Format(time.RFC3339),
	}), nil
}

// --- POST /jobs/{id}/start ------------------------------------------

func (h *Handler) startJob(ctx context.Context, _ events.APIGatewayV2HTTPRequest, jobID string) (events.APIGatewayV2HTTPResponse, error) {
	if jobID == "" {
		return errorResponse(400, ErrCodeBadRequest, "job_id is required"), nil
	}

	rec, err := h.Store.GetJob(ctx, jobID)
	if err != nil {
		return errorResponse(500, ErrCodeInternalError, fmt.Sprintf("get job: %v", err)), nil
	}
	if rec == nil {
		return errorResponse(404, ErrCodeNotFound, fmt.Sprintf("job %q does not exist", jobID)), nil
	}
	if rec.InputURI == "" {
		return errorResponse(400, ErrCodeBadRequest, "job has no input_uri; was the document uploaded?"), nil
	}

	mode := rec.Mode
	if mode == "" {
		mode = string(ModePassthrough)
	}

	// pages is only consumed on the passthrough branch (the rendered
	// branch projects $.render_result.pages instead); send the source
	// URI as a single-element placeholder so the ModeChoice can
	// resolve either way.
	execInput, err := json.Marshal(map[string]any{
		"job_id":        jobID,
		"input_uri":     rec.InputURI,
		"pages":         []string{rec.InputURI},
		"mode":          mode,
		"first_queue":   h.FirstQueue,
		"final_stage":   h.FinalStage,
		"final_version": h.FinalVersion,
	})
	if err != nil {
		return errorResponse(500, ErrCodeInternalError, fmt.Sprintf("marshal sfn input: %v", err)), nil
	}

	execARN, err := h.SFn.StartExecution(ctx, h.StateMachineARN, string(execInput))
	if err != nil {
		return errorResponse(500, ErrCodeInternalError, fmt.Sprintf("start execution: %v", err)), nil
	}

	return jsonResponse(200, StartJobResponse{
		JobID:        jobID,
		ExecutionARN: execARN,
	}), nil
}

// --- GET /jobs/{id} --------------------------------------------------

func (h *Handler) getStatus(ctx context.Context, _ events.APIGatewayV2HTTPRequest, jobID string) (events.APIGatewayV2HTTPResponse, error) {
	rec, err := h.Store.GetJob(ctx, jobID)
	if err != nil {
		return errorResponse(500, ErrCodeInternalError, fmt.Sprintf("get job: %v", err)), nil
	}
	if rec == nil {
		return errorResponse(404, ErrCodeNotFound, fmt.Sprintf("job %q does not exist", jobID)), nil
	}

	stages, err := h.Store.ListStagesByJob(ctx, jobID, h.FinalStage, h.FinalVersion)
	if err != nil {
		return errorResponse(500, ErrCodeInternalError, fmt.Sprintf("list stages: %v", err)), nil
	}

	out := JobStatusResponse{
		JobID:     jobID,
		Status:    JobStatus(rec.Status),
		PageCount: rec.PageCount,
		InputURI:  rec.InputURI,
		ResultURI: rec.ResultURI,
		Error:     rec.Error,
		UpdatedAt: rec.UpdatedAt,
	}
	if rec.ExpiresAt > 0 {
		out.ExpiresAt = time.Unix(rec.ExpiresAt, 0).UTC().Format(time.RFC3339)
	}
	for _, s := range stages {
		switch s.Status {
		case store.StageStatusSucceeded:
			out.SucceededCount++
		case store.StageStatusFailed:
			out.FailedCount++
		}
	}
	out.PendingCount = rec.PageCount - out.SucceededCount - out.FailedCount
	if out.PendingCount < 0 {
		out.PendingCount = 0
	}

	return jsonResponse(200, out), nil
}

// --- GET /jobs/{id}/result -------------------------------------------

func (h *Handler) getResult(ctx context.Context, _ events.APIGatewayV2HTTPRequest, jobID string) (events.APIGatewayV2HTTPResponse, error) {
	merged, status, errResp := h.loadMerged(ctx, jobID)
	if errResp != nil {
		return *errResp, nil
	}
	_ = status

	out := ResultResponse{
		JobID:     merged.JobID,
		PageCount: merged.PageCount,
		Pages:     make([]ResultPage, 0, len(merged.Pages)),
	}
	for _, p := range merged.Pages {
		var typed pipeline.PageExtractionResult
		if err := json.Unmarshal(p.Result, &typed); err != nil {
			return errorResponse(500, ErrCodeInternalError, fmt.Sprintf("decode page %d result: %v", p.Page, err)), nil
		}
		out.Pages = append(out.Pages, ResultPage{
			Page:   p.Page,
			Result: typed,
		})
	}

	return jsonResponse(200, out), nil
}

// --- GET /jobs/{id}/aggregate ----------------------------------------

func (h *Handler) aggregate(ctx context.Context, req events.APIGatewayV2HTTPRequest, jobID string) (events.APIGatewayV2HTTPResponse, error) {
	merchant := strings.TrimSpace(req.QueryStringParameters["merchant"])
	if merchant == "" {
		return errorResponse(400, ErrCodeBadRequest, "merchant query parameter is required"), nil
	}

	merged, _, errResp := h.loadMerged(ctx, jobID)
	if errResp != nil {
		return *errResp, nil
	}

	out := AggregateResponse{
		JobID:    jobID,
		Merchant: merchant,
		Currency: "JPY",
	}

	for _, p := range merged.Pages {
		var typed pipeline.PageExtractionResult
		if err := json.Unmarshal(p.Result, &typed); err != nil {
			return errorResponse(500, ErrCodeInternalError, fmt.Sprintf("decode page %d result: %v", p.Page, err)), nil
		}
		for _, tx := range typed.Transactions {
			if tx.Merchant != merchant {
				continue
			}
			amt, err := parseAmountJPY(tx.Amount)
			if err != nil {
				// Skip un-parseable rows so a single bad row does not
				// kill the whole aggregate.
				continue
			}
			out.Count++
			out.TotalJPY += amt
			out.Transactions = append(out.Transactions, AggregatedTransaction{
				Transaction: tx,
				Page:        p.Page,
			})
		}
	}

	return jsonResponse(200, out), nil
}

// --- Shared helpers --------------------------------------------------

func (h *Handler) loadMerged(ctx context.Context, jobID string) (workflow.MergedDocument, JobStatus, *events.APIGatewayV2HTTPResponse) {
	rec, err := h.Store.GetJob(ctx, jobID)
	if err != nil {
		r := errorResponse(500, ErrCodeInternalError, fmt.Sprintf("get job: %v", err))
		return workflow.MergedDocument{}, "", &r
	}
	if rec == nil {
		r := errorResponse(404, ErrCodeNotFound, fmt.Sprintf("job %q does not exist", jobID))
		return workflow.MergedDocument{}, "", &r
	}
	if rec.Status == store.JobStatusFailed {
		r := errorResponse(409, ErrCodeJobFailed, rec.Error)
		return workflow.MergedDocument{}, JobStatusFailed, &r
	}
	if rec.Status != store.JobStatusSucceeded || rec.ResultURI == "" {
		r := errorResponse(409, ErrCodeJobNotReady, fmt.Sprintf("job %q is %s; result not available yet", jobID, rec.Status))
		return workflow.MergedDocument{}, JobStatus(rec.Status), &r
	}

	body, err := h.Objects.Get(ctx, rec.ResultURI)
	if err != nil {
		r := errorResponse(500, ErrCodeInternalError, fmt.Sprintf("read merged: %v", err))
		return workflow.MergedDocument{}, "", &r
	}

	var merged workflow.MergedDocument
	if err := json.Unmarshal(body, &merged); err != nil {
		r := errorResponse(500, ErrCodeInternalError, fmt.Sprintf("decode merged: %v", err))
		return workflow.MergedDocument{}, "", &r
	}
	return merged, JobStatusSucceeded, nil
}

func (h *Handler) checkAuth(req events.APIGatewayV2HTTPRequest) error {
	if h.APIKey == "" {
		// No key configured = no auth. Used only by tests that
		// explicitly leave APIKey empty.
		return nil
	}
	got := req.Headers["x-api-key"]
	if got == "" {
		got = req.Headers["X-Api-Key"]
	}
	if got != h.APIKey {
		return fmt.Errorf("missing or invalid x-api-key")
	}
	return nil
}

func (h *Handler) newJobID() string {
	if h.NewJobID != nil {
		return h.NewJobID()
	}
	now := time.Now
	if h.Now != nil {
		now = h.Now
	}
	var b [3]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("job_%d_%s", now().Unix(), hex.EncodeToString(b[:]))
}

// --- path helpers ----------------------------------------------------

// hasSuffix reports whether path has the shape "<prefix><id><suffix>"
// with a non-empty id between.
func hasSuffix(path, prefix, suffix string) bool {
	if !strings.HasPrefix(path, prefix) || !strings.HasSuffix(path, suffix) {
		return false
	}
	mid := strings.TrimPrefix(path, prefix)
	mid = strings.TrimSuffix(mid, suffix)
	return mid != "" && !strings.Contains(mid, "/")
}

func idBetween(path, prefix, suffix string) string {
	mid := strings.TrimPrefix(path, prefix)
	return strings.TrimSuffix(mid, suffix)
}

func isBareJobPath(path string) bool {
	if !strings.HasPrefix(path, "/jobs/") {
		return false
	}
	rest := strings.TrimPrefix(path, "/jobs/")
	return rest != "" && !strings.Contains(rest, "/")
}

// --- response helpers ------------------------------------------------

func jsonResponse(status int, body any) events.APIGatewayV2HTTPResponse {
	b, _ := json.Marshal(body)
	return events.APIGatewayV2HTTPResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       string(b),
	}
}

func errorResponse(status int, code, message string) events.APIGatewayV2HTTPResponse {
	return jsonResponse(status, ErrorResponse{Error: code, Message: message})
}

// --- misc helpers ----------------------------------------------------

func parseAmountJPY(s string) (int, error) {
	s = strings.ReplaceAll(s, ",", "")
	s = strings.TrimSpace(s)
	return strconv.Atoi(s)
}

func contentTypeForExt(ext string) string {
	switch strings.ToLower(ext) {
	case ".png":
		return "image/png"
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".webp":
		return "image/webp"
	case ".pdf":
		return "application/pdf"
	default:
		return "application/octet-stream"
	}
}
