// Package api defines the HTTP contract between the lady-glass CLI
// client and the API Gateway Lambda. The types in this file are
// authoritative: the client serialises requests into them and the
// handler deserialises responses out of them, so both sides agree
// on field names, tags, and semantics by referring to one place.
package api

import (
	"github.com/keix/lady-glass/internal/pipeline"
)

// --- POST /jobs ------------------------------------------------------

// Mode selects how the SFN workflow processes the uploaded document.
//
//   - ModePassthrough sends the source document straight to the AI
//     stage as a single page. Cheapest path, ideal for short PDFs
//     (≤ ~5 pages) and images.
//   - ModeRendered runs a RenderPages step first that splits a PDF
//     into one-page PDFs, then SubmitPages fans out N messages to
//     the stage chain — true per-page parallelism, retry, and
//     idempotency.
//
// Default when omitted is ModePassthrough.
type Mode string

const (
	ModePassthrough Mode = "passthrough"
	ModeRendered    Mode = "rendered"
)

// CreateJobRequest opens a new job slot and asks for a presigned PUT
// URL so the client can upload the source document directly to S3
// without round-tripping through the API Lambda (avoiding the 6 MB
// payload limit and keeping cost flat regardless of document size).
type CreateJobRequest struct {
	// Filename is the original local filename. Used only to derive
	// the S3 key suffix and to log; the actual stored key is
	// determined by the server.
	Filename string `json:"filename"`

	// ContentType is the MIME type the client will PUT with. Optional;
	// the server derives a default from the filename extension when
	// empty.
	ContentType string `json:"content_type,omitempty"`

	// Mode selects the workflow shape. Empty = passthrough.
	Mode Mode `json:"mode,omitempty"`

	// ChainID names the stage chain the job runs on (SPEC §S10).
	// The empty value resolves to chain.DefaultChainID inside the
	// API handler; supplying an unknown id yields 400. Specified
	// here for completeness — v0 deployments hardcode a single
	// chain so most callers will leave it empty.
	ChainID string `json:"chain_id,omitempty"`
}

// CreateJobResponse hands the client a presigned URL and the server-
// generated job_id. The client is expected to PUT the document body
// to UploadURL before ExpiresAt, then call POST /jobs/{job_id}/start
// to begin processing.
type CreateJobResponse struct {
	JobID     string `json:"job_id"`
	UploadURL string `json:"upload_url"`
	// ExpiresAt is the RFC3339 timestamp at which UploadURL stops
	// accepting PUTs. The CLI displays it directly to the user.
	ExpiresAt string `json:"expires_at"`
}

// --- POST /jobs/{id}/start -------------------------------------------

// StartJobResponse is the result of starting a SFn execution against an
// uploaded job. Returned to the client so it can either poll status or
// inspect the execution in the AWS console.
type StartJobResponse struct {
	JobID        string `json:"job_id"`
	ExecutionARN string `json:"execution_arn"`
}

// --- GET /jobs/{id} --------------------------------------------------

// JobStatus mirrors the document-level status the workflow produces.
// Repeating it here (rather than reusing store.JobStatus) keeps the
// HTTP vocabulary independent of the storage type.
type JobStatus string

const (
	JobStatusCreated   JobStatus = "created"
	JobStatusRunning   JobStatus = "running"
	JobStatusSucceeded JobStatus = "succeeded"
	JobStatusFailed    JobStatus = "failed"
)

// JobStatusResponse is the status snapshot the client polls. Counts
// come from a fresh ListStagesByJob aggregation against the final
// stage so the response reflects in-flight progress, not just the
// JobRecord's terminal status.
type JobStatusResponse struct {
	JobID          string    `json:"job_id"`
	Status         JobStatus `json:"status"`
	PageCount      int       `json:"page_count"`
	SucceededCount int       `json:"succeeded_count"`
	FailedCount    int       `json:"failed_count"`
	PendingCount   int       `json:"pending_count"`

	// InputURI / ResultURI are populated when known. ResultURI appears
	// only after Merge has finalised the job.
	InputURI  string `json:"input_uri,omitempty"`
	ResultURI string `json:"result_uri,omitempty"`

	// Error is the message MarkJobFailed wrote, if status == failed.
	Error string `json:"error,omitempty"`

	// UpdatedAt is RFC3339 from the JobRecord. CreatedAt is not
	// tracked separately in v0 — the JobRecord's first UpdatedAt
	// stands in.
	UpdatedAt string `json:"updated_at,omitempty"`

	// ExpiresAt is the RFC3339 wall-clock time at which the job's
	// DynamoDB row and S3 artifacts become eligible for deletion
	// under the SPEC §S9 retention policy. Operators use it to
	// know when status / result calls will stop working. Empty
	// when the store has retention disabled.
	ExpiresAt string `json:"expires_at,omitempty"`
}

// --- GET /jobs/{id}/result -------------------------------------------

// ResultResponse is the merged extraction result with typed per-page
// bodies. Compared to the raw merged blob written by Merge (which
// embeds each page's result as json.RawMessage for lossless storage),
// this endpoint parses each page into pipeline.PageExtractionResult so
// the client can rely on the typed contract.
type ResultResponse struct {
	JobID     string       `json:"job_id"`
	PageCount int          `json:"page_count"`
	Pages     []ResultPage `json:"pages"`
}

// ResultPage is one page's typed extraction inside a ResultResponse.
type ResultPage struct {
	Page   int                           `json:"page"`
	Result pipeline.PageExtractionResult `json:"result"`
}

// --- GET /jobs/{id}/aggregate ----------------------------------------

// AggregateRequest names the single filter dimension to aggregate by.
// FilterKey MUST match one of the JSON tags on pipeline.Transaction
// (a string-typed field; e.g. "merchant", "foreign_currency",
// "currency", "date"). FilterValue is the exact-match value. Exactly
// one filter is applied per request; combining filters is not
// supported in v0.
type AggregateRequest struct {
	FilterKey   string `json:"filter_key,omitempty"`
	FilterValue string `json:"filter_value,omitempty"`
}

// AggregateResponse is the rollup the API computes by walking every
// page's Transactions list and summing the matching rows. Total and
// Currency are determined by the filter key:
//
//   - filter_key=foreign_currency → Total is ForeignAmount sum, Currency
//     is the filter value (e.g. "MYR");
//   - filter_key=currency         → Total is Amount sum, Currency is the
//     filter value;
//   - any other filter            → Total is Amount sum, Currency is "JPY"
//     (the primary-currency assumption of the credit-card-statement
//     use case).
//
// Total is a string so foreign-currency decimals ("1234.56") survive the
// round-trip without float coercion at the JSON boundary.
type AggregateResponse struct {
	JobID       string `json:"job_id"`
	FilterKey   string `json:"filter_key"`
	FilterValue string `json:"filter_value"`
	Count       int    `json:"count"`
	Total       string `json:"total"`
	Currency    string `json:"currency"`

	// Transactions are the matched rows (with page number attached)
	// so the client can display the breakdown beside the totals.
	Transactions []AggregatedTransaction `json:"transactions"`
}

// AggregatedTransaction embeds pipeline.Transaction and tags on the
// page number for display in the breakdown.
type AggregatedTransaction struct {
	pipeline.Transaction

	// Page is the 1-based page number the transaction appeared on.
	Page int `json:"page"`
}

// --- Error envelope --------------------------------------------------

// ErrorResponse is returned with any non-2xx status. Error is a short
// machine-readable token (e.g. "not_found", "bad_request") and
// Message is a human-readable explanation.
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message,omitempty"`
}

// --- Error tokens -----------------------------------------------------

const (
	ErrCodeBadRequest    = "bad_request"
	ErrCodeUnauthorized  = "unauthorized"
	ErrCodeNotFound      = "not_found"
	ErrCodeJobNotReady   = "job_not_ready"
	ErrCodeJobFailed     = "job_failed"
	ErrCodeInternalError = "internal_error"
)
