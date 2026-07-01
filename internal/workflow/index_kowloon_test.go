package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/keix/lady-glass/internal/client/kowloon"
	"github.com/keix/lady-glass/internal/object"
	"github.com/keix/lady-glass/internal/pipeline"
	"github.com/keix/lady-glass/internal/store"
	"github.com/keix/lady-glass/internal/workflow"
)

// TestIndexKowloon_HappyPathCallsKowloonAndPersistsSidecar covers the
// fresh run: manifest present, sidecar absent, Kowloon returns 200.
// The step calls Kowloon exactly once, persists a sidecar with the
// typed response, and returns the identifiers so an SFN task result
// can carry them downstream.
func TestIndexKowloon_HappyPathCallsKowloonAndPersistsSidecar(t *testing.T) {
	ctx := context.Background()
	dst, manifestURI := seedManifest(t, "job_100", "keix",
		"s3://lady-glass-permanent/results/transactions/tenant=keix/year=2026/month=06/smbc-platinum-preferred.json",
	)
	st := seedJob(t, "job_100")

	fake := newFakeKowloon(kowloon.IndexResultResponse{
		Status:            "indexed",
		KowloonCollection: "transactions",
		IndexJobID:        "kidx_first",
		VectorCount:       33,
		EmbeddingModel:    "text-embedding-3-large",
		IndexedAt:         time.Date(2026, 7, 2, 15, 21, 0, 0, time.UTC),
	})

	out, err := workflow.IndexKowloon(ctx,
		workflow.IndexKowloonInput{
			JobID:       "job_100",
			TenantID:    "keix",
			ManifestURI: manifestURI,
		},
		st, dst, fake, fixedNow("2026-07-02T15:22:00Z"),
	)
	if err != nil {
		t.Fatalf("index: %v", err)
	}

	if !out.Indexed {
		t.Error("Indexed = false on a fresh run")
	}
	if out.IndexJobID != "kidx_first" {
		t.Errorf("IndexJobID = %q, want kidx_first", out.IndexJobID)
	}
	if out.VectorCount != 33 {
		t.Errorf("VectorCount = %d, want 33", out.VectorCount)
	}
	if out.Collection != "transactions" {
		t.Errorf("Collection = %q, want transactions", out.Collection)
	}
	if fake.calls != 1 {
		t.Errorf("Kowloon called %d time(s), want 1", fake.calls)
	}

	// Wire shape: Kowloon received the archive URI from the manifest,
	// not the manifest URI itself.
	wantResultURI := "s3://lady-glass-permanent/results/transactions/tenant=keix/year=2026/month=06/smbc-platinum-preferred.json"
	if fake.lastReq.ResultURI != wantResultURI {
		t.Errorf("Kowloon request.result_uri = %q, want %q", fake.lastReq.ResultURI, wantResultURI)
	}
	if fake.lastReq.SchemaVersion != "transactions.v1" {
		t.Errorf("Kowloon request.schema_version = %q, want transactions.v1", fake.lastReq.SchemaVersion)
	}
	if fake.lastReq.ResultType != "transactions" {
		t.Errorf("Kowloon request.result_type = %q, want transactions", fake.lastReq.ResultType)
	}
	if fake.lastReq.TenantID != "keix" {
		t.Errorf("Kowloon request.tenant_id = %q, want keix", fake.lastReq.TenantID)
	}

	// Sidecar landed at the deterministic key.
	if !strings.HasSuffix(out.SidecarURI, "index/jobs/job_100.json") {
		t.Errorf("SidecarURI = %q, want suffix index/jobs/job_100.json", out.SidecarURI)
	}
}

// TestIndexKowloon_RerunSkipsKowloonWhenSidecarExists is §11.4's
// first case adapted for the workflow shape: sidecar present ⇒ NO
// Kowloon call, cached response returned, Indexed = false. This is
// the property that makes retry cost bounded to at most one S3 GET.
func TestIndexKowloon_RerunSkipsKowloonWhenSidecarExists(t *testing.T) {
	ctx := context.Background()
	dst, manifestURI := seedManifest(t, "job_101", "keix",
		"s3://lady-glass-permanent/results/transactions/tenant=keix/year=2026/month=06/smbc.json",
	)
	st := seedJob(t, "job_101")

	first := newFakeKowloon(kowloon.IndexResultResponse{
		IndexJobID: "kidx_first", VectorCount: 5, KowloonCollection: "transactions",
	})
	if _, err := workflow.IndexKowloon(ctx,
		workflow.IndexKowloonInput{JobID: "job_101", TenantID: "keix", ManifestURI: manifestURI},
		st, dst, first, fixedNow("2026-07-02T15:22:00Z"),
	); err != nil {
		t.Fatalf("first run: %v", err)
	}

	// Second run against a client that would fail the test if
	// touched — proves the sidecar short-circuit is upstream of
	// Kowloon.
	tripwire := &trippingKowloon{t: t}
	out, err := workflow.IndexKowloon(ctx,
		workflow.IndexKowloonInput{JobID: "job_101", TenantID: "keix", ManifestURI: manifestURI},
		st, dst, tripwire, fixedNow("2026-07-02T16:00:00Z"),
	)
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	if out.Indexed {
		t.Error("Indexed = true on rerun; want false (short-circuited)")
	}
	if out.IndexJobID != "kidx_first" {
		t.Errorf("IndexJobID drifted: %q vs kidx_first (from cached sidecar)", out.IndexJobID)
	}
	if out.VectorCount != 5 {
		t.Errorf("VectorCount = %d, want 5 (cached)", out.VectorCount)
	}
}

// TestIndexKowloon_KowloonCollisionReturnsSameIndexJobID is §11.4's
// second case: sidecar absent (edge case — a crash between "call
// Kowloon" and "write sidecar" wiped Lady Glass's marker), but
// Kowloon's own idempotency store still holds the entry. The retry
// reaches Kowloon, and Kowloon returns the SAME IndexJobID as the
// first call. The workflow step accepts that without treating it as
// a divergence, and re-writes the sidecar with the returned ID.
//
// This test uses a fake that returns the same IndexJobID for repeat
// calls with the same (job_id, result_uri) — matching Kowloon's
// documented behavior.
func TestIndexKowloon_KowloonCollisionReturnsSameIndexJobID(t *testing.T) {
	ctx := context.Background()
	dst, manifestURI := seedManifest(t, "job_102", "keix",
		"s3://lady-glass-permanent/results/transactions/tenant=keix/year=2026/month=06/smbc.json",
	)
	st := seedJob(t, "job_102")

	// Kowloon-style idempotent fake: same (job, result_uri) → same
	// IndexJobID, no matter how many times called.
	idempotent := &idempotentKowloon{
		resp: kowloon.IndexResultResponse{
			IndexJobID: "kidx_stable", VectorCount: 12, KowloonCollection: "transactions",
		},
	}

	first, err := workflow.IndexKowloon(ctx,
		workflow.IndexKowloonInput{JobID: "job_102", TenantID: "keix", ManifestURI: manifestURI},
		st, dst, idempotent, fixedNow("2026-07-02T15:22:00Z"),
	)
	if err != nil {
		t.Fatalf("first run: %v", err)
	}

	// Wipe the sidecar to simulate the "sidecar absent but Kowloon
	// remembers" crash window.
	if fs, ok := dst.(*object.FileStore); ok {
		if err := deleteFile(t, fs, first.SidecarURI); err != nil {
			t.Fatalf("wipe sidecar: %v", err)
		}
	} else {
		t.Fatal("destination is not a FileStore; cannot simulate sidecar wipe")
	}

	second, err := workflow.IndexKowloon(ctx,
		workflow.IndexKowloonInput{JobID: "job_102", TenantID: "keix", ManifestURI: manifestURI},
		st, dst, idempotent, fixedNow("2026-07-02T16:00:00Z"),
	)
	if err != nil {
		t.Fatalf("second run: %v", err)
	}

	if !second.Indexed {
		t.Error("Indexed = false on the sidecar-wiped rerun; want true (Kowloon was recalled)")
	}
	if idempotent.calls != 2 {
		t.Errorf("Kowloon called %d time(s), want 2 (once per Lady Glass invocation)", idempotent.calls)
	}
	if second.IndexJobID != first.IndexJobID {
		t.Errorf("IndexJobID drifted across retries: %q vs %q", second.IndexJobID, first.IndexJobID)
	}
}

// A 5xx from Kowloon surfaces as a *TransientError through the
// workflow — the caller (SFN retry policy) uses that to schedule a
// backoff. The sidecar is NOT written, so the next retry actually
// calls Kowloon again.
func TestIndexKowloon_TransientKowloonErrorSurfaces(t *testing.T) {
	ctx := context.Background()
	dst, manifestURI := seedManifest(t, "job_103", "keix",
		"s3://lady-glass-permanent/results/transactions/tenant=keix/year=2026/month=06/smbc.json",
	)
	st := seedJob(t, "job_103")

	failing := &erroringKowloon{err: &kowloon.TransientError{Op: "kowloon: 503", Err: errors.New("upstream unavailable")}}
	_, err := workflow.IndexKowloon(ctx,
		workflow.IndexKowloonInput{JobID: "job_103", TenantID: "keix", ManifestURI: manifestURI},
		st, dst, failing, fixedNow("2026-07-02T15:22:00Z"),
	)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var te *kowloon.TransientError
	if !errors.As(err, &te) {
		t.Fatalf("expected TransientError, got %v", err)
	}

	// Sidecar must NOT exist — otherwise the next retry short-circuits
	// past a call that never actually succeeded.
	exists, err := dst.Exists(ctx, dst.URIFor("index/jobs/job_103.json"))
	if err != nil {
		t.Fatalf("probe sidecar: %v", err)
	}
	if exists {
		t.Error("sidecar written despite transient Kowloon error")
	}
}

// 400 from Kowloon is a schema mismatch — permanent. The workflow
// surfaces it as *SchemaError so the SFN can catch on that type and
// route to MarkJobFailed instead of retrying forever.
func TestIndexKowloon_SchemaErrorSurfaces(t *testing.T) {
	ctx := context.Background()
	dst, manifestURI := seedManifest(t, "job_104", "keix",
		"s3://lady-glass-permanent/results/transactions/tenant=keix/year=2026/month=06/smbc.json",
	)
	st := seedJob(t, "job_104")

	failing := &erroringKowloon{err: &kowloon.SchemaError{StatusCode: 400, Body: "unknown schema"}}
	_, err := workflow.IndexKowloon(ctx,
		workflow.IndexKowloonInput{JobID: "job_104", TenantID: "keix", ManifestURI: manifestURI},
		st, dst, failing, fixedNow("2026-07-02T15:22:00Z"),
	)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var se *kowloon.SchemaError
	if !errors.As(err, &se) {
		t.Fatalf("expected SchemaError, got %v", err)
	}
}

// The manifest is the authoritative source for TenantID. A caller
// mismatch (SFN input says one tenant, manifest says another) is a
// hard error — the alternative is silently indexing a job under the
// wrong scope.
func TestIndexKowloon_TenantMismatchRejects(t *testing.T) {
	ctx := context.Background()
	dst, manifestURI := seedManifest(t, "job_105", "keix",
		"s3://lady-glass-permanent/results/transactions/tenant=keix/year=2026/month=06/smbc.json",
	)
	st := seedJob(t, "job_105")

	fake := newFakeKowloon(kowloon.IndexResultResponse{})
	_, err := workflow.IndexKowloon(ctx,
		workflow.IndexKowloonInput{JobID: "job_105", TenantID: "wrong-tenant", ManifestURI: manifestURI},
		st, dst, fake, fixedNow("2026-07-02T00:00:00Z"),
	)
	if err == nil {
		t.Fatal("expected tenant mismatch error, got nil")
	}
	if fake.calls != 0 {
		t.Error("Kowloon was called despite tenant mismatch")
	}
}

// Input validation: empty JobID / TenantID / ManifestURI / nil client
// all fail before any S3 or Kowloon access.
func TestIndexKowloon_RejectsBadInput(t *testing.T) {
	ctx := context.Background()
	dst, manifestURI := seedManifest(t, "job_ok", "keix",
		"s3://lady-glass-permanent/results/transactions/tenant=keix/year=2026/month=06/smbc.json",
	)
	st := seedJob(t, "job_ok")

	fake := newFakeKowloon(kowloon.IndexResultResponse{})
	cases := []struct {
		name   string
		in     workflow.IndexKowloonInput
		client kowloon.Client
	}{
		{"empty_job_id", workflow.IndexKowloonInput{JobID: "", TenantID: "keix", ManifestURI: manifestURI}, fake},
		{"empty_tenant", workflow.IndexKowloonInput{JobID: "job_ok", TenantID: "", ManifestURI: manifestURI}, fake},
		{"empty_manifest_uri", workflow.IndexKowloonInput{JobID: "job_ok", TenantID: "keix", ManifestURI: ""}, fake},
		{"nil_client", workflow.IndexKowloonInput{JobID: "job_ok", TenantID: "keix", ManifestURI: manifestURI}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := workflow.IndexKowloon(ctx, tc.in, st, dst, tc.client, fixedNow("2026-07-02T00:00:00Z")); err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

// A manifest without a results.transactions URI is a bug on the
// upstream side (ArchiveResult should never produce one). The
// workflow refuses to send an empty result_uri to Kowloon.
func TestIndexKowloon_RejectsManifestWithoutResultURI(t *testing.T) {
	ctx := context.Background()
	dst := object.NewFileStore(t.TempDir())
	// Manifest missing Results.Transactions — write it directly to
	// simulate a corrupt or partial ArchiveResult run.
	body, _ := json.Marshal(workflow.Manifest{
		JobID:    "job_bad",
		TenantID: "keix",
		Source:   workflow.ManifestSource{Kind: "card_statement"},
		// Results is zero-valued → empty Transactions URI.
	})
	manifestURI, err := dst.PutBytes(context.Background(), "manifests/jobs/job_bad.json", body, "application/json")
	if err != nil {
		t.Fatalf("seed manifest: %v", err)
	}
	st := seedJob(t, "job_bad")

	fake := newFakeKowloon(kowloon.IndexResultResponse{})
	if _, err := workflow.IndexKowloon(ctx,
		workflow.IndexKowloonInput{JobID: "job_bad", TenantID: "keix", ManifestURI: manifestURI},
		st, dst, fake, fixedNow("2026-07-02T00:00:00Z"),
	); err == nil {
		t.Fatal("expected error for empty result URI, got nil")
	}
	if fake.calls != 0 {
		t.Error("Kowloon was called despite missing result_uri in manifest")
	}
}

// -- Helpers -------------------------------------------------------

// seedManifest writes a Manifest to a fresh FileStore-backed
// destination bucket and returns (dst, manifestURI) so tests can
// wire it into an IndexKowloonInput without hand-rolling the setup.
func seedManifest(t *testing.T, jobID, tenantID, archiveURI string) (object.Store, string) {
	t.Helper()
	dst := object.NewFileStore(t.TempDir())
	mf := workflow.Manifest{
		JobID:    jobID,
		TenantID: tenantID,
		Source: workflow.ManifestSource{
			Kind:     "card_statement",
			Issuer:   "SMBC",
			CardName: "Platinum Preferred",
			RawURI:   "s3://lady-glass-permanent/raw/card-statements/smbc/2026/06/original.pdf",
		},
		Results:    workflow.ManifestResults{Transactions: archiveURI},
		Chain:      workflow.ManifestChain{ID: "card-statement-v1", Stages: []string{"gemini", "normalize_card_statement", "enrich_transactions"}},
		ArchivedAt: "2026-07-02T15:20:00Z",
	}
	body, err := json.Marshal(mf)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	uri, err := dst.PutBytes(context.Background(), "manifests/jobs/"+jobID+".json", body, "application/json")
	if err != nil {
		t.Fatalf("put manifest: %v", err)
	}
	return dst, uri
}

// seedJob writes a JobRecord that IndexKowloon's defensive load
// expects. Values beyond JobID are not read by the step but populated
// to mirror what SubmitPages + Merge would leave behind.
func seedJob(t *testing.T, jobID string) store.Store {
	t.Helper()
	st := store.NewMemoryStore()
	if err := st.PutJob(context.Background(), store.JobRecord{
		JobID:     jobID,
		Status:    store.JobStatusSucceeded,
		ResultURI: "s3://stage/jobs/" + jobID + "/merged/v1/result.json",
		ChainID:   "card-statement-v1",
		Chain: []pipeline.StageSpec{
			{Name: "gemini", Version: "v1", QueueName: "gemini"},
			{Name: "normalize_card_statement", Version: "v1", QueueName: "normalize_card_statement"},
			{Name: "enrich_transactions", Version: "v1", QueueName: "enrich_transactions"},
		},
	}); err != nil {
		t.Fatalf("put job: %v", err)
	}
	return st
}

// deleteFile simulates the "sidecar wiped between runs" crash window
// by removing the underlying file from a FileStore-backed URI. The
// two-layer idempotency test uses this to exercise the case where
// Kowloon still holds the record but Lady Glass's local marker is
// gone.
func deleteFile(t *testing.T, _ *object.FileStore, uri string) error {
	t.Helper()
	const prefix = "file://"
	if !strings.HasPrefix(uri, prefix) {
		return errors.New("deleteFile: URI is not a file:// URI")
	}
	return os.Remove(strings.TrimPrefix(uri, prefix))
}

// fakeKowloon returns a fixed response and counts calls.
type fakeKowloon struct {
	resp    kowloon.IndexResultResponse
	calls   int
	lastReq kowloon.IndexResultRequest
}

func newFakeKowloon(resp kowloon.IndexResultResponse) *fakeKowloon {
	return &fakeKowloon{resp: resp}
}

func (f *fakeKowloon) IndexResult(_ context.Context, req kowloon.IndexResultRequest) (kowloon.IndexResultResponse, error) {
	f.calls++
	f.lastReq = req
	return f.resp, nil
}

// trippingKowloon fails the test if IndexResult is invoked. Used to
// assert that the rerun short-circuit is upstream of the client.
type trippingKowloon struct {
	t *testing.T
}

func (k *trippingKowloon) IndexResult(_ context.Context, _ kowloon.IndexResultRequest) (kowloon.IndexResultResponse, error) {
	k.t.Fatal("Kowloon client was called on a rerun that should have short-circuited")
	return kowloon.IndexResultResponse{}, nil
}

// idempotentKowloon returns the same response for every call. Models
// Kowloon's own KowloonIdempotency store returning the same
// IndexJobID for a repeat call with the same (job_id, result_uri).
type idempotentKowloon struct {
	resp  kowloon.IndexResultResponse
	calls int
}

func (k *idempotentKowloon) IndexResult(_ context.Context, _ kowloon.IndexResultRequest) (kowloon.IndexResultResponse, error) {
	k.calls++
	return k.resp, nil
}

// erroringKowloon returns a fixed error on every call.
type erroringKowloon struct {
	err error
}

func (k *erroringKowloon) IndexResult(_ context.Context, _ kowloon.IndexResultRequest) (kowloon.IndexResultResponse, error) {
	return kowloon.IndexResultResponse{}, k.err
}
