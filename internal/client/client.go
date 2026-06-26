// Package client is the HTTP client for the Lady Glass API. It is
// the inverse of internal/api: the CLI marshals into the api package's
// request types, the API handler unmarshals out of them, and this
// client moves the bytes between them over HTTP.
//
// Network calls never hide the underlying error: a non-2xx response
// is parsed into the typed api.ErrorResponse and returned as *Error
// so the CLI can branch on api.ErrCode* tokens.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/keix/lady-glass/internal/api"
)

// Client talks to the Lady Glass API. Construct with New; reuse the
// same Client across calls.
type Client struct {
	BaseURL string
	APIKey  string
	HTTP    *http.Client
}

// New returns a Client configured with sensible defaults. APIKey is
// the shared secret the operator put into SSM and pulled from .env
// or process env via LADY_GLASS_API_TOKEN.
func New(baseURL, apiKey string) *Client {
	return &Client{
		BaseURL: strings.TrimRight(baseURL, "/"),
		APIKey:  apiKey,
		HTTP:    &http.Client{Timeout: 30 * time.Second},
	}
}

// Error is the typed error returned by every Client method on a
// non-2xx response. CLI code can detect specific error categories
// via errors.As + e.Code.
type Error struct {
	StatusCode int
	Code       string
	Message    string
}

func (e *Error) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("api: %d %s", e.StatusCode, e.Code)
	}
	return fmt.Sprintf("api: %d %s: %s", e.StatusCode, e.Code, e.Message)
}

// IsCode reports whether err is an *Error with the given code. Useful
// for CLI branches like errors.Is(err, ...) but for the api.ErrCode*
// token strings.
func IsCode(err error, code string) bool {
	var apiErr *Error
	if errors.As(err, &apiErr) {
		return apiErr.Code == code
	}
	return false
}

// --- API methods -----------------------------------------------------

// CreateJob calls POST /jobs and returns the upload URL and job_id.
func (c *Client) CreateJob(ctx context.Context, in api.CreateJobRequest) (api.CreateJobResponse, error) {
	var out api.CreateJobResponse
	if err := c.do(ctx, "POST", "/jobs", in, &out); err != nil {
		return api.CreateJobResponse{}, err
	}
	return out, nil
}

// UploadFile PUTs the file at path to uploadURL with the given
// content-type. The presigned URL has the content-type signed in, so
// the header must match exactly.
func (c *Client) UploadFile(ctx context.Context, uploadURL, path, contentType string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("client: open %s: %w", path, err)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("client: stat %s: %w", path, err)
	}

	req, err := http.NewRequestWithContext(ctx, "PUT", uploadURL, f)
	if err != nil {
		return fmt.Errorf("client: build upload request: %w", err)
	}
	req.Header.Set("Content-Type", contentType)
	req.ContentLength = fi.Size()

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return fmt.Errorf("client: upload to S3: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("client: upload returned %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

// StartJob calls POST /jobs/{id}/start and returns the SFn execution
// ARN.
func (c *Client) StartJob(ctx context.Context, jobID string) (api.StartJobResponse, error) {
	var out api.StartJobResponse
	if err := c.do(ctx, "POST", "/jobs/"+url.PathEscape(jobID)+"/start", nil, &out); err != nil {
		return api.StartJobResponse{}, err
	}
	return out, nil
}

// GetJobStatus calls GET /jobs/{id} and returns the current status
// snapshot with per-page counts.
func (c *Client) GetJobStatus(ctx context.Context, jobID string) (api.JobStatusResponse, error) {
	var out api.JobStatusResponse
	if err := c.do(ctx, "GET", "/jobs/"+url.PathEscape(jobID), nil, &out); err != nil {
		return api.JobStatusResponse{}, err
	}
	return out, nil
}

// GetJobResult calls GET /jobs/{id}/result and returns the typed
// per-page extraction. Returns *Error with Code=job_not_ready when
// the job is still running and Code=job_failed when it failed.
func (c *Client) GetJobResult(ctx context.Context, jobID string) (api.ResultResponse, error) {
	var out api.ResultResponse
	if err := c.do(ctx, "GET", "/jobs/"+url.PathEscape(jobID)+"/result", nil, &out); err != nil {
		return api.ResultResponse{}, err
	}
	return out, nil
}

// AggregateJob calls GET /jobs/{id}/aggregate with the single filter
// dimension expressed as a query parameter (the API rejects requests
// without a filter or with more than one filter).
func (c *Client) AggregateJob(ctx context.Context, jobID string, req api.AggregateRequest) (api.AggregateResponse, error) {
	q := url.Values{}
	if req.FilterKey != "" {
		q.Set(req.FilterKey, req.FilterValue)
	}
	path := "/jobs/" + url.PathEscape(jobID) + "/aggregate"
	if encoded := q.Encode(); encoded != "" {
		path += "?" + encoded
	}

	var out api.AggregateResponse
	if err := c.do(ctx, "GET", path, nil, &out); err != nil {
		return api.AggregateResponse{}, err
	}
	return out, nil
}

// --- transport -------------------------------------------------------

// do is the single HTTP code path: marshal in, send, parse out or
// typed error.
func (c *Client) do(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return fmt.Errorf("client: marshal request: %w", err)
		}
		body = bytes.NewReader(b)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, body)
	if err != nil {
		return fmt.Errorf("client: build request: %w", err)
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.APIKey != "" {
		req.Header.Set("X-Api-Key", c.APIKey)
	}

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return fmt.Errorf("client: %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("client: read body: %w", err)
	}

	if resp.StatusCode/100 != 2 {
		var apiErr api.ErrorResponse
		_ = json.Unmarshal(respBody, &apiErr)
		return &Error{
			StatusCode: resp.StatusCode,
			Code:       apiErr.Error,
			Message:    apiErr.Message,
		}
	}

	if out != nil {
		if err := json.Unmarshal(respBody, out); err != nil {
			return fmt.Errorf("client: decode response: %w", err)
		}
	}
	return nil
}
