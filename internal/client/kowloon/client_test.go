package kowloon_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/keix/lady-glass/internal/client/kowloon"
)

// TestHTTPClient_IndexResult_RoundTripsRequestAndResponse asserts the
// happy path: the client marshals the request into the POST body
// verbatim, and Kowloon's typed response is decoded back into the
// caller-visible struct. Both directions are pinned because the
// wire shape is the coupling contract with a separately-versioned
// service (§8 boundary rule).
func TestHTTPClient_IndexResult_RoundTripsRequestAndResponse(t *testing.T) {
	var gotBody []byte
	var gotHeaders http.Header
	var gotMethod, gotPath string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotHeaders = r.Header.Clone()
		var err error
		gotBody, err = io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
		}
		resp := kowloon.IndexResultResponse{
			Status:            "indexed",
			KowloonCollection: "transactions",
			IndexJobID:        "kidx_1782619002123456789",
			VectorCount:       33,
			EmbeddingModel:    "text-embedding-3-large",
			IndexedAt:         time.Date(2026, 7, 2, 15, 21, 0, 0, time.UTC),
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			t.Errorf("encode response: %v", err)
		}
	}))
	defer server.Close()

	client := kowloon.New(server.URL, "test-api-key")
	req := kowloon.IndexResultRequest{
		JobID:         "job_001",
		TenantID:      "keix",
		ResultURI:     "s3://lady-glass-permanent/results/transactions/tenant=keix/year=2026/month=06/smbc-platinum-preferred.json",
		ResultType:    "transactions",
		SchemaVersion: "transactions.v1",
	}
	got, err := client.IndexResult(context.Background(), req)
	if err != nil {
		t.Fatalf("index: %v", err)
	}

	// Wire shape: method, path, headers.
	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if gotPath != "/v1/index-result" {
		t.Errorf("path = %q, want /v1/index-result", gotPath)
	}
	if gotHeaders.Get("Content-Type") != "application/json" {
		t.Errorf("Content-Type = %q", gotHeaders.Get("Content-Type"))
	}
	if gotHeaders.Get("X-Api-Key") != "test-api-key" {
		t.Errorf("X-Api-Key = %q, want test-api-key", gotHeaders.Get("X-Api-Key"))
	}

	// Request body decodes back into the same struct — the JSON tag
	// contract is what Kowloon is coded against.
	var echoed kowloon.IndexResultRequest
	if err := json.Unmarshal(gotBody, &echoed); err != nil {
		t.Fatalf("decode server-received body: %v", err)
	}
	if echoed != req {
		t.Errorf("round-trip request drifted: got %+v, want %+v", echoed, req)
	}

	// Response fields land where callers read them.
	if got.IndexJobID != "kidx_1782619002123456789" {
		t.Errorf("IndexJobID = %q", got.IndexJobID)
	}
	if got.VectorCount != 33 {
		t.Errorf("VectorCount = %d, want 33", got.VectorCount)
	}
	if got.KowloonCollection != "transactions" {
		t.Errorf("KowloonCollection = %q", got.KowloonCollection)
	}
	if !got.IndexedAt.Equal(time.Date(2026, 7, 2, 15, 21, 0, 0, time.UTC)) {
		t.Errorf("IndexedAt = %v", got.IndexedAt)
	}
}

// TestHTTPClient_IndexResult_MapsStatusesToTypedErrors is the §6.6
// contract in one table: 400 becomes a permanent SchemaError (do not
// retry), 429/5xx become TransientError (backoff and retry). Callers
// (the IndexKowloon workflow step) branch on those types to decide
// whether to fail the job or defer to SFN's retry policy.
func TestHTTPClient_IndexResult_MapsStatusesToTypedErrors(t *testing.T) {
	cases := []struct {
		name       string
		statusCode int
		body       string
		classify   func(err error) bool
	}{
		{
			name:       "400_schema_error",
			statusCode: 400,
			body:       `{"error":"unknown schema_version"}`,
			classify: func(err error) bool {
				var se *kowloon.SchemaError
				return errors.As(err, &se) && se.StatusCode == 400
			},
		},
		{
			name:       "429_transient",
			statusCode: 429,
			body:       `{"error":"quota exhausted"}`,
			classify: func(err error) bool {
				var te *kowloon.TransientError
				return errors.As(err, &te)
			},
		},
		{
			name:       "500_transient",
			statusCode: 500,
			body:       `internal server error`,
			classify: func(err error) bool {
				var te *kowloon.TransientError
				return errors.As(err, &te)
			},
		},
		{
			name:       "503_transient",
			statusCode: 503,
			body:       `service unavailable`,
			classify: func(err error) bool {
				var te *kowloon.TransientError
				return errors.As(err, &te)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.statusCode)
				fmt.Fprint(w, tc.body)
			}))
			defer server.Close()

			client := kowloon.New(server.URL, "")
			_, err := client.IndexResult(context.Background(), kowloon.IndexResultRequest{
				JobID: "job_x", TenantID: "keix",
			})
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tc.classify(err) {
				t.Fatalf("error not classified as expected type: %v", err)
			}
			// The body prefix appears in the error message so an
			// operator triaging from CloudWatch has context without
			// digging out a proxy capture.
			if tc.body != "" && !strings.Contains(err.Error(), tc.body[:min(len(tc.body), 32)]) {
				t.Errorf("error %q does not include a body prefix", err.Error())
			}
		})
	}
}

// A network failure (dial reset, EOF, TLS handshake failure) surfaces
// as TransientError — same retry class as a 5xx. The workflow step
// therefore does not need to special-case connectivity issues.
func TestHTTPClient_IndexResult_NetworkFailureIsTransient(t *testing.T) {
	// Server that closes the connection mid-request produces an EOF
	// on the client side — a realistic transient failure mode.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hj, ok := w.(http.Hijacker)
		if !ok {
			t.Fatal("test server does not support hijacking")
		}
		conn, _, err := hj.Hijack()
		if err != nil {
			t.Fatalf("hijack: %v", err)
		}
		conn.Close()
	}))
	defer server.Close()

	client := kowloon.New(server.URL, "")
	_, err := client.IndexResult(context.Background(), kowloon.IndexResultRequest{JobID: "job_x", TenantID: "keix"})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var te *kowloon.TransientError
	if !errors.As(err, &te) {
		t.Fatalf("network failure not classified as TransientError: %v", err)
	}
}

// A 2xx response with garbage JSON is a Kowloon bug — a retry will
// not fix it. The client returns a generic decode error rather than
// TransientError so the workflow step surfaces the mismatch loudly
// instead of silently redriving until DLQ.
func TestHTTPClient_IndexResult_RejectsMalformedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"index_job_id": ]not valid[}`)
	}))
	defer server.Close()

	client := kowloon.New(server.URL, "")
	_, err := client.IndexResult(context.Background(), kowloon.IndexResultRequest{JobID: "job_x", TenantID: "keix"})
	if err == nil {
		t.Fatal("expected decode error, got nil")
	}
	var te *kowloon.TransientError
	if errors.As(err, &te) {
		t.Fatalf("malformed 2xx should NOT be transient: %v", err)
	}
}

// Empty API key omits the X-Api-Key header entirely — some
// deployments (dev, private VPC) do not require auth. The client
// must not send an empty header value, which some servers reject.
func TestHTTPClient_IndexResult_OmitsAuthHeaderWhenAPIKeyEmpty(t *testing.T) {
	var gotAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("X-Api-Key")
		w.WriteHeader(200)
		fmt.Fprint(w, `{}`)
	}))
	defer server.Close()

	client := kowloon.New(server.URL, "")
	if _, err := client.IndexResult(context.Background(), kowloon.IndexResultRequest{JobID: "j"}); err != nil {
		t.Fatalf("index: %v", err)
	}
	if gotAuth != "" {
		t.Errorf("X-Api-Key = %q, want empty (header omitted)", gotAuth)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
