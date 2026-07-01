// Package kowloon is the Lady Glass side thin client for the Kowloon
// semantic memory service (https://github.com/keix/kowloon). It is
// intentionally NOT a re-export of Kowloon's own client/ package —
// pulling that in would drag the AWS SDK, Milvus SDK, and OpenAI
// client tree behind it, which are irrelevant on the Lady Glass
// side. A hand-rolled net/http wrapper against Kowloon's two public
// endpoints (§8 of kowloon-integration.md) keeps the dependency
// surface flat.
//
// Only IndexResult is implemented in this file — it is the endpoint
// the IndexKowloon workflow step calls after ArchiveResult has
// produced a permanent-bucket archive URI. /v1/search (for the query
// API in §7) and /v1/resolve/merchant (optional enrich fallback in
// §4.5) land in follow-up packages when those features ship.
package kowloon

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// IndexResultRequest is the payload for POST /v1/index-result. JobID
// scopes the record on Kowloon's idempotency store; ResultURI is the
// permanent-bucket archive body Kowloon downloads and vectorises;
// SchemaVersion is the pinned converter version so a Kowloon schema
// change never silently reinterprets an old archive.
//
// ImportBatchID is optional: some upstream flows (bulk re-index of a
// tenant's historical data) group multiple job archives under a single
// batch so operators can track progress; a single-job archive-then-
// index run leaves it empty and Kowloon treats each request as its
// own batch.
type IndexResultRequest struct {
	JobID         string `json:"job_id"`
	TenantID      string `json:"tenant_id"`
	ResultURI     string `json:"result_uri"`
	ResultType    string `json:"result_type"`
	SchemaVersion string `json:"schema_version"`
	ImportBatchID string `json:"import_batch_id,omitempty"`
}

// IndexResultResponse is Kowloon's typed reply. IndexJobID is the
// stable identifier Kowloon returns for both the first and any
// subsequent call with the same idempotency tuple (§6.5) — so a
// caller that reaches Kowloon after a Lady Glass-side crash still
// gets the same ID and can attach it to its own idempotency record
// without a divergence.
//
// VectorCount and EmbeddingModel are observability fields — Lady
// Glass persists them alongside the archive for operator dashboards;
// they never drive Lady Glass control flow.
type IndexResultResponse struct {
	Status            string    `json:"status"`
	KowloonCollection string    `json:"kowloon_collection"`
	IndexJobID        string    `json:"index_job_id"`
	VectorCount       int       `json:"vector_count"`
	EmbeddingModel    string    `json:"embedding_model"`
	IndexedAt         time.Time `json:"indexed_at"`
}

// Client is the interface the workflow step depends on. Tests
// substitute a fake; the HTTPClient below satisfies it against a
// real Kowloon endpoint.
type Client interface {
	IndexResult(ctx context.Context, req IndexResultRequest) (IndexResultResponse, error)
}

// HTTPClient is the concrete net/http implementation. BaseURL points
// at the Kowloon front door (e.g. https://kowloon.internal); the
// caller supplies APIKey per §9 of the integration doc, and HTTP is
// a shared *http.Client so callers can control timeouts and TLS
// verification centrally.
type HTTPClient struct {
	BaseURL string
	APIKey  string
	HTTP    *http.Client
}

// New returns an HTTPClient with a sensible default *http.Client and
// a 30-second timeout — the Kowloon side downloads the archive body
// from S3, computes embeddings via OpenAI, and writes vectors to
// Milvus, so the tail latency budget is generous. Callers that need
// a different budget can override the HTTP field directly.
func New(baseURL, apiKey string) *HTTPClient {
	return &HTTPClient{
		BaseURL: baseURL,
		APIKey:  apiKey,
		HTTP:    &http.Client{Timeout: 30 * time.Second},
	}
}

// IndexResult POSTs req to <BaseURL>/v1/index-result. Non-2xx status
// codes surface as typed errors so the caller (IndexKowloon workflow
// step) can distinguish retryable from permanent failures:
//
//   - 400 → *SchemaError. The archive's schema_version is not one
//     Kowloon knows about; retrying will not help. Fail the job.
//   - 429, 5xx → *TransientError. Redeliver and retry; the failure
//     is on Kowloon's side (Milvus down, OpenAI quota exhausted, etc.).
//     §6.6 explicitly notes we do NOT distinguish 429 from 5xx here;
//     both go to the same retry path, backing off until DLQ.
//   - other non-2xx → generic error, wraps the status + body prefix
//     so an operator has enough context to triage without a proxy
//     capture.
//
// A network failure (dial timeout, EOF, TLS handshake) also surfaces
// as *TransientError so the workflow step's retry policy treats it
// the same as a 5xx.
func (c *HTTPClient) IndexResult(ctx context.Context, req IndexResultRequest) (IndexResultResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return IndexResultResponse{}, fmt.Errorf("kowloon: marshal request: %w", err)
	}

	url := c.BaseURL + "/v1/index-result"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return IndexResultResponse{}, fmt.Errorf("kowloon: build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")
	if c.APIKey != "" {
		// Kowloon's front-door auth uses the same X-Api-Key convention
		// as the Lady Glass API layer. The env plumbing in §9 sets the
		// shared secret; a mismatch surfaces as a 401 from Kowloon
		// which falls through to the generic error branch below and
		// is loud enough that an operator notices at first traffic.
		httpReq.Header.Set("X-Api-Key", c.APIKey)
	}

	httpClient := c.HTTP
	if httpClient == nil {
		httpClient = http.DefaultClient
	}

	resp, err := httpClient.Do(httpReq)
	if err != nil {
		return IndexResultResponse{}, &TransientError{
			Op:  "kowloon: post /v1/index-result",
			Err: err,
		}
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return IndexResultResponse{}, &TransientError{
			Op:  "kowloon: read response body",
			Err: err,
		}
	}

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		var out IndexResultResponse
		if err := json.Unmarshal(respBody, &out); err != nil {
			return IndexResultResponse{}, fmt.Errorf("kowloon: decode response: %w", err)
		}
		return out, nil
	case resp.StatusCode == http.StatusBadRequest:
		return IndexResultResponse{}, &SchemaError{
			StatusCode: resp.StatusCode,
			Body:       truncate(string(respBody), 512),
		}
	case resp.StatusCode == http.StatusTooManyRequests, resp.StatusCode >= 500:
		return IndexResultResponse{}, &TransientError{
			Op:  fmt.Sprintf("kowloon: %d from /v1/index-result", resp.StatusCode),
			Err: fmt.Errorf("body: %s", truncate(string(respBody), 512)),
		}
	default:
		return IndexResultResponse{}, fmt.Errorf(
			"kowloon: unexpected status %d: %s",
			resp.StatusCode, truncate(string(respBody), 512),
		)
	}
}

// SchemaError is the typed 400 from Kowloon — the archive Lady Glass
// tried to index carries a schema_version Kowloon does not recognise.
// The workflow step maps this to a permanent failure: retrying will
// keep hitting the same 400, so the job's terminal state should
// become failed and a human should update the Kowloon converter
// (or fix the archive's schema pin).
type SchemaError struct {
	StatusCode int
	Body       string
}

func (e *SchemaError) Error() string {
	return fmt.Sprintf("kowloon: schema rejected (status %d): %s", e.StatusCode, e.Body)
}

// TransientError is the typed retryable failure — network hiccups,
// 429 quota, 5xx server errors. IndexKowloon returns this without
// modification so the caller (SFN retry policy) treats them as
// eligible for backoff. §6.6 notes we do not distinguish quota from
// server error at this layer; both are equally worth retrying.
type TransientError struct {
	Op  string
	Err error
}

func (e *TransientError) Error() string {
	return fmt.Sprintf("%s: %v", e.Op, e.Err)
}

func (e *TransientError) Unwrap() error { return e.Err }

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
