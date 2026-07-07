package kowloon

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestNewWithOAuth_AttachesBearer stands up a fake provider (/token) and
// a fake Kowloon (/v1/index-result) and asserts the client fetches a
// client_credentials token and presents it as a bearer on the index call
// — with the audience parameter forwarded to the token endpoint.
func TestNewWithOAuth_AttachesBearer(t *testing.T) {
	var (
		sawGrantType string
		sawAudience  string
		sawBearer    string
	)

	mux := http.NewServeMux()
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		sawGrantType = r.PostForm.Get("grant_type")
		sawAudience = r.PostForm.Get("audience")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": "tok-123",
			"token_type":   "Bearer",
			"expires_in":   3600,
		})
	})
	mux.HandleFunc("/v1/index-result", func(w http.ResponseWriter, r *http.Request) {
		sawBearer = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(IndexResultResponse{
			Status:            "indexed",
			KowloonCollection: "transactions",
			IndexJobID:        "kidx_1",
			VectorCount:       3,
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewWithOAuth(srv.URL, OAuthConfig{
		TokenURL:     srv.URL + "/token",
		ClientID:     "lady-glass",
		ClientSecret: "s3cr3t",
		Audience:     "kowloon",
	})

	resp, err := c.IndexResult(context.Background(), IndexResultRequest{
		JobID:         "job_1",
		TenantID:      "keix",
		ResultURI:     "s3://bucket/results/transactions/tenant=keix/x.json",
		ResultType:    "transactions",
		SchemaVersion: "transactions.v1",
	})
	if err != nil {
		t.Fatalf("IndexResult: %v", err)
	}
	if resp.VectorCount != 3 {
		t.Errorf("vector_count=%d, want 3", resp.VectorCount)
	}
	if sawGrantType != "client_credentials" {
		t.Errorf("grant_type=%q, want client_credentials", sawGrantType)
	}
	if sawAudience != "kowloon" {
		t.Errorf("audience=%q, want kowloon", sawAudience)
	}
	if sawBearer != "Bearer tok-123" {
		t.Errorf("Authorization=%q, want %q", sawBearer, "Bearer tok-123")
	}
	if strings.Contains(sawBearer, "X-Api-Key") {
		t.Errorf("unexpected api-key path")
	}
}
