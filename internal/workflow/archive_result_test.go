package workflow_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/keix/lady-glass/internal/object"
	"github.com/keix/lady-glass/internal/pipeline"
	"github.com/keix/lady-glass/internal/store"
	"github.com/keix/lady-glass/internal/workflow"
)

// TestArchiveResult_WritesArchiveRawAndManifestOnFreshRun asserts the
// happy path: three objects land at the deterministic keys, and the
// returned URIs point at each of them. The archive body is FLAT (§4
// of this file's implementation notes) — one Transactions slice
// spanning every page, not the per-page nesting Merge produced.
func TestArchiveResult_WritesArchiveRawAndManifestOnFreshRun(t *testing.T) {
	ctx := context.Background()
	src, dst := newStores(t)
	st := store.NewMemoryStore()

	mergedURI, rawURI := seedMergedAndRaw(t, src, "job_001",
		[]pipeline.PageExtractionResult{
			{
				DocumentType: pipeline.DocumentTypeCreditCardStatement,
				Fields: map[string]any{
					"issuer_name":    "SMBC",
					"card_name":      "Platinum Preferred",
					"statement_date": "2026-06-10",
				},
				Transactions: []pipeline.Transaction{
					{Date: "2026-06-01", Merchant: "FamilyMart", Amount: "401", MerchantNormalized: "FamilyMart", Country: "JP"},
				},
			},
			{
				Fields: map[string]any{}, // no override
				Transactions: []pipeline.Transaction{
					{Date: "2026-06-05", Merchant: "Starbucks", Amount: "480", MerchantNormalized: "Starbucks", Country: "JP"},
				},
			},
		},
	)
	putJob(t, st, "job_001", mergedURI, rawURI)

	out, err := workflow.ArchiveResult(ctx,
		workflow.ArchiveResultInput{JobID: "job_001", TenantID: "keix"},
		st, src, dst, fixedNow("2026-07-02T09:30:00Z"),
	)
	if err != nil {
		t.Fatalf("archive: %v", err)
	}
	if !out.Archived {
		t.Fatal("Archived = false on a fresh run")
	}

	// Deterministic key shapes — a change here will silently break
	// Kowloon's ability to find the archive body, so pin them.
	wantArchiveSuffix := "results/transactions/tenant=keix/year=2026/month=06/smbc-platinum-preferred.json"
	wantRawSuffix := "raw/card-statements/smbc/2026/06/original.pdf"
	wantManifestSuffix := "manifests/jobs/job_001.json"

	if !strings.HasSuffix(out.ArchiveURI, wantArchiveSuffix) {
		t.Errorf("archive_uri = %q, want suffix %q", out.ArchiveURI, wantArchiveSuffix)
	}
	if !strings.HasSuffix(out.RawURI, wantRawSuffix) {
		t.Errorf("raw_uri = %q, want suffix %q", out.RawURI, wantRawSuffix)
	}
	if !strings.HasSuffix(out.ManifestURI, wantManifestSuffix) {
		t.Errorf("manifest_uri = %q, want suffix %q", out.ManifestURI, wantManifestSuffix)
	}

	// Archive body is flat: two transactions total, in page order.
	var archive workflow.ArchivedDocument
	readJSON(t, dst, out.ArchiveURI, &archive)
	if archive.TenantID != "keix" {
		t.Errorf("archive.tenant_id = %q, want keix", archive.TenantID)
	}
	if archive.DocumentType != pipeline.DocumentTypeCreditCardStatement {
		t.Errorf("archive.document_type = %q", archive.DocumentType)
	}
	if got := len(archive.Transactions); got != 2 {
		t.Fatalf("archive.transactions len = %d, want 2", got)
	}
	if archive.Transactions[0].Merchant != "FamilyMart" || archive.Transactions[1].Merchant != "Starbucks" {
		t.Errorf("transaction order lost: %+v", archive.Transactions)
	}

	// Manifest shape matches §5.4 pin-for-pin.
	var mf workflow.Manifest
	readJSON(t, dst, out.ManifestURI, &mf)
	if mf.JobID != "job_001" || mf.TenantID != "keix" {
		t.Errorf("manifest job/tenant wrong: %+v", mf)
	}
	if mf.Source.Kind != "card_statement" {
		t.Errorf("manifest source.kind = %q, want card_statement", mf.Source.Kind)
	}
	if mf.Source.Issuer != "SMBC" || mf.Source.CardName != "Platinum Preferred" {
		t.Errorf("manifest source issuer/card = %+v", mf.Source)
	}
	if mf.Source.RawURI != out.RawURI {
		t.Errorf("manifest source.raw_uri = %q, want %q", mf.Source.RawURI, out.RawURI)
	}
	if mf.Results.Transactions != out.ArchiveURI {
		t.Errorf("manifest results.transactions = %q, want %q", mf.Results.Transactions, out.ArchiveURI)
	}
	if mf.ArchivedAt != "2026-07-02T09:30:00Z" {
		t.Errorf("archived_at = %q, want 2026-07-02T09:30:00Z", mf.ArchivedAt)
	}
	if mf.Chain.ID != "card-statement-v1" || len(mf.Chain.Stages) == 0 {
		t.Errorf("manifest chain = %+v", mf.Chain)
	}

	// Raw PDF bytes round-tripped verbatim.
	rawBody, err := dst.Get(ctx, out.RawURI)
	if err != nil {
		t.Fatalf("read raw: %v", err)
	}
	if string(rawBody) != "PDFBYTES" {
		t.Errorf("raw body = %q, want PDFBYTES", rawBody)
	}
}

// TestArchiveResult_RerunIsANoOp asserts §11.3 of kowloon-integration:
// when the manifest already exists at the deterministic key, no
// PutObject is issued and the returned URIs match what the previous
// run wrote. This is the property that makes SFN retry, Lambda
// re-invocation, and manual re-drive all cost-safe on the S3 side.
func TestArchiveResult_RerunIsANoOp(t *testing.T) {
	ctx := context.Background()
	src, inner := newStores(t)
	st := store.NewMemoryStore()

	mergedURI, rawURI := seedMergedAndRaw(t, src, "job_002",
		[]pipeline.PageExtractionResult{{
			Fields: map[string]any{
				"issuer_name":    "SMBC",
				"card_name":      "Platinum Preferred",
				"statement_date": "2026-06-10",
			},
			Transactions: []pipeline.Transaction{
				{Date: "2026-06-01", Merchant: "FamilyMart", Amount: "401"},
			},
		}},
	)
	putJob(t, st, "job_002", mergedURI, rawURI)

	// First run to prime the destination.
	first, err := workflow.ArchiveResult(ctx,
		workflow.ArchiveResultInput{JobID: "job_002", TenantID: "keix"},
		st, src, inner, fixedNow("2026-07-02T09:30:00Z"),
	)
	if err != nil {
		t.Fatalf("first run: %v", err)
	}

	// Second run with a spy that will fail the test if ANY Put*
	// reaches the destination.
	spy := &spyStore{inner: inner}
	second, err := workflow.ArchiveResult(ctx,
		workflow.ArchiveResultInput{JobID: "job_002", TenantID: "keix"},
		st, src, spy, fixedNow("2026-07-02T10:45:00Z"),
	)
	if err != nil {
		t.Fatalf("second run: %v", err)
	}

	if second.Archived {
		t.Error("Archived = true on rerun; want false (short-circuited)")
	}
	if spy.puts != 0 {
		t.Errorf("PutObject issued on rerun: %d call(s)", spy.puts)
	}

	// URIs from the second run must match the first, because the
	// second read the persisted manifest instead of re-deriving them.
	if second.ArchiveURI != first.ArchiveURI {
		t.Errorf("archive_uri drifted: %q vs %q", second.ArchiveURI, first.ArchiveURI)
	}
	if second.RawURI != first.RawURI {
		t.Errorf("raw_uri drifted: %q vs %q", second.RawURI, first.RawURI)
	}
	if second.ManifestURI != first.ManifestURI {
		t.Errorf("manifest_uri drifted: %q vs %q", second.ManifestURI, first.ManifestURI)
	}
}

// TestArchiveResult_CardSlugFallsBackWhenFieldsMissing exercises the
// two-level slug fallback: no card_name → issuer-only; neither → last
// 12 chars of the job id. A run that reaches the JobID fallback is
// still archivable — the alternative is failing the whole step, which
// blocks Kowloon indexing for a job whose extraction happened to miss
// the header.
func TestArchiveResult_CardSlugFallsBackWhenFieldsMissing(t *testing.T) {
	ctx := context.Background()

	t.Run("issuer_only_when_no_card_name", func(t *testing.T) {
		src, dst := newStores(t)
		st := store.NewMemoryStore()
		mergedURI, rawURI := seedMergedAndRaw(t, src, "job_iss",
			[]pipeline.PageExtractionResult{{
				Fields: map[string]any{"issuer_name": "SMBC", "statement_date": "2026-06-10"},
			}},
		)
		putJob(t, st, "job_iss", mergedURI, rawURI)

		out, err := workflow.ArchiveResult(ctx,
			workflow.ArchiveResultInput{JobID: "job_iss", TenantID: "keix"},
			st, src, dst, fixedNow("2026-07-02T00:00:00Z"),
		)
		if err != nil {
			t.Fatalf("archive: %v", err)
		}
		if !strings.HasSuffix(out.ArchiveURI, "/smbc.json") {
			t.Errorf("archive_uri = %q, want /smbc.json suffix", out.ArchiveURI)
		}
	})

	t.Run("jobid_when_no_issuer_or_card", func(t *testing.T) {
		src, dst := newStores(t)
		st := store.NewMemoryStore()
		// Long-ish job id so the last-12-chars fallback has something to
		// strip — matches production job_<epoch>_<hex> shape.
		jobID := "job_1782619002_20a307"
		mergedURI, rawURI := seedMergedAndRaw(t, src, jobID,
			[]pipeline.PageExtractionResult{{
				Fields: map[string]any{"statement_date": "2026-06-10"},
			}},
		)
		putJob(t, st, jobID, mergedURI, rawURI)

		out, err := workflow.ArchiveResult(ctx,
			workflow.ArchiveResultInput{JobID: jobID, TenantID: "keix"},
			st, src, dst, fixedNow("2026-07-02T00:00:00Z"),
		)
		if err != nil {
			t.Fatalf("archive: %v", err)
		}
		// Last 12 chars of "job_1782619002_20a307" = "19002_20a307".
		if !strings.HasSuffix(out.ArchiveURI, "/19002_20a307.json") {
			t.Errorf("archive_uri = %q, want /19002_20a307.json suffix (last 12 chars of jobID)", out.ArchiveURI)
		}
		// Raw path uses issuer_slug = "unknown" when Fields lacks issuer.
		if !strings.Contains(out.RawURI, "/unknown/") {
			t.Errorf("raw_uri = %q, want /unknown/ in path", out.RawURI)
		}
	})
}

// TestArchiveResult_YearMonthFallsBackToArchivedAt: when the model
// couldn't pull a statement_date, the object still lands under a
// year/month partition — the archived_at timestamp acts as the
// fallback. This keeps the S3 layout consistent so an operator
// scanning by year/month never sees an empty partition marker.
func TestArchiveResult_YearMonthFallsBackToArchivedAt(t *testing.T) {
	ctx := context.Background()
	src, dst := newStores(t)
	st := store.NewMemoryStore()

	mergedURI, rawURI := seedMergedAndRaw(t, src, "job_nd",
		[]pipeline.PageExtractionResult{{
			Fields: map[string]any{"issuer_name": "SMBC", "card_name": "Gold"},
			// No statement_date.
		}},
	)
	putJob(t, st, "job_nd", mergedURI, rawURI)

	out, err := workflow.ArchiveResult(ctx,
		workflow.ArchiveResultInput{JobID: "job_nd", TenantID: "keix"},
		st, src, dst, fixedNow("2026-07-02T00:00:00Z"),
	)
	if err != nil {
		t.Fatalf("archive: %v", err)
	}
	if !strings.Contains(out.ArchiveURI, "year=2026/month=07") {
		t.Errorf("archive_uri = %q, want year=2026/month=07 (from archived_at)", out.ArchiveURI)
	}
}

// TestArchiveResult_YearMonthParsesJapaneseStatementDate — Gemini
// occasionally emits "2026年6月10日" for statement_date; the same
// year/month must land at the same partition as the ISO form.
func TestArchiveResult_YearMonthParsesJapaneseStatementDate(t *testing.T) {
	ctx := context.Background()
	src, dst := newStores(t)
	st := store.NewMemoryStore()

	mergedURI, rawURI := seedMergedAndRaw(t, src, "job_jp",
		[]pipeline.PageExtractionResult{{
			Fields: map[string]any{
				"issuer_name":    "SMBC",
				"card_name":      "Platinum Preferred",
				"statement_date": "2026年6月10日",
			},
		}},
	)
	putJob(t, st, "job_jp", mergedURI, rawURI)

	out, err := workflow.ArchiveResult(ctx,
		workflow.ArchiveResultInput{JobID: "job_jp", TenantID: "keix"},
		st, src, dst, fixedNow("2026-07-02T00:00:00Z"),
	)
	if err != nil {
		t.Fatalf("archive: %v", err)
	}
	if !strings.Contains(out.ArchiveURI, "year=2026/month=06") {
		t.Errorf("archive_uri = %q, want year=2026/month=06", out.ArchiveURI)
	}
}

// TestArchiveResult_SkipsRawCopyWhenInputURIEmpty: some job creation
// flows write per-page rendered images directly and leave InputURI
// empty. The archive still lands, but the manifest's raw_uri is empty
// — a downstream auditor sees "no raw available" rather than a broken
// pointer at a stage-bucket path that expired.
func TestArchiveResult_SkipsRawCopyWhenInputURIEmpty(t *testing.T) {
	ctx := context.Background()
	src, dst := newStores(t)
	st := store.NewMemoryStore()

	mergedURI, _ := seedMergedAndRaw(t, src, "job_nr",
		[]pipeline.PageExtractionResult{{
			Fields: map[string]any{"issuer_name": "SMBC", "card_name": "Gold", "statement_date": "2026-06-10"},
		}},
	)
	putJob(t, st, "job_nr", mergedURI, "") // InputURI intentionally empty.

	out, err := workflow.ArchiveResult(ctx,
		workflow.ArchiveResultInput{JobID: "job_nr", TenantID: "keix"},
		st, src, dst, fixedNow("2026-07-02T00:00:00Z"),
	)
	if err != nil {
		t.Fatalf("archive: %v", err)
	}
	if out.RawURI != "" {
		t.Errorf("raw_uri = %q, want empty when input_uri is empty", out.RawURI)
	}

	var mf workflow.Manifest
	readJSON(t, dst, out.ManifestURI, &mf)
	if mf.Source.RawURI != "" {
		t.Errorf("manifest.raw_uri = %q, want empty", mf.Source.RawURI)
	}
}

// Input validation: empty JobID / TenantID / unsafe TenantID all fail
// before any DDB or S3 access. The unsafe-tenant case is the last
// line of defence before the value becomes an S3 key partition.
func TestArchiveResult_RejectsBadInput(t *testing.T) {
	ctx := context.Background()
	src, dst := newStores(t)
	st := store.NewMemoryStore()

	cases := []struct {
		name string
		in   workflow.ArchiveResultInput
	}{
		{"empty_job_id", workflow.ArchiveResultInput{JobID: "", TenantID: "keix"}},
		{"empty_tenant", workflow.ArchiveResultInput{JobID: "job_x", TenantID: ""}},
		{"tenant_with_slash", workflow.ArchiveResultInput{JobID: "job_x", TenantID: "keix/evil"}},
		{"tenant_with_equals", workflow.ArchiveResultInput{JobID: "job_x", TenantID: "tenant=other"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := workflow.ArchiveResult(ctx, tc.in, st, src, dst, fixedNow("2026-07-02T00:00:00Z")); err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

// Missing JobRecord — the workflow refuses to run against a job the
// store has no record of. This is a defensive guard, not an expected
// flow: SFN only invokes ArchiveResult after SubmitPages has written
// the row.
func TestArchiveResult_RejectsUnknownJob(t *testing.T) {
	ctx := context.Background()
	src, dst := newStores(t)
	st := store.NewMemoryStore()

	if _, err := workflow.ArchiveResult(ctx,
		workflow.ArchiveResultInput{JobID: "job_missing", TenantID: "keix"},
		st, src, dst, fixedNow("2026-07-02T00:00:00Z"),
	); err == nil {
		t.Fatal("expected error for unknown job, got nil")
	}
}

// JobRecord without ResultURI — Merge hasn't run yet, so there's no
// merged blob to archive. Fail loud rather than write a placeholder.
func TestArchiveResult_RejectsJobWithoutMergedResult(t *testing.T) {
	ctx := context.Background()
	src, dst := newStores(t)
	st := store.NewMemoryStore()

	if err := st.PutJob(ctx, store.JobRecord{JobID: "job_pre", Status: store.JobStatusRunning}); err != nil {
		t.Fatalf("put job: %v", err)
	}
	if _, err := workflow.ArchiveResult(ctx,
		workflow.ArchiveResultInput{JobID: "job_pre", TenantID: "keix"},
		st, src, dst, fixedNow("2026-07-02T00:00:00Z"),
	); err == nil {
		t.Fatal("expected error for job without result_uri, got nil")
	}
}

// -- Helpers -------------------------------------------------------

func newStores(t *testing.T) (source, destination object.Store) {
	t.Helper()
	return object.NewFileStore(t.TempDir()), object.NewFileStore(t.TempDir())
}

// seedMergedAndRaw seeds src with (a) a MergedDocument wrapping the
// given per-page PageExtractionResult list, and (b) a stub raw PDF
// body. Returns the merged and raw URIs so the caller can wire them
// onto a JobRecord.
func seedMergedAndRaw(
	t *testing.T,
	src object.Store,
	jobID string,
	pages []pipeline.PageExtractionResult,
) (mergedURI, rawURI string) {
	t.Helper()
	ctx := context.Background()

	mergedPages := make([]workflow.MergedPage, len(pages))
	for i, p := range pages {
		body, err := json.Marshal(p)
		if err != nil {
			t.Fatalf("marshal page %d: %v", i+1, err)
		}
		mergedPages[i] = workflow.MergedPage{Page: i + 1, Result: body}
	}
	mergedBody, err := json.Marshal(workflow.MergedDocument{
		JobID:     jobID,
		PageCount: len(pages),
		Pages:     mergedPages,
	})
	if err != nil {
		t.Fatalf("marshal merged: %v", err)
	}
	mergedURI, err = src.PutBytes(ctx, "jobs/"+jobID+"/merged/v1/result.json", mergedBody, "application/json")
	if err != nil {
		t.Fatalf("put merged: %v", err)
	}
	rawURI, err = src.PutBytes(ctx, "jobs/"+jobID+"/input/original.pdf", []byte("PDFBYTES"), "application/pdf")
	if err != nil {
		t.Fatalf("put raw: %v", err)
	}
	return mergedURI, rawURI
}

// putJob writes a JobRecord that looks like what SubmitPages + Merge
// would leave behind: chain frozen, InputURI set to the raw upload,
// ResultURI set to the merged blob.
func putJob(t *testing.T, st store.Store, jobID, mergedURI, inputURI string) {
	t.Helper()
	rec := store.JobRecord{
		JobID:     jobID,
		Status:    store.JobStatusSucceeded,
		InputURI:  inputURI,
		ResultURI: mergedURI,
		ChainID:   "card-statement-v1",
		Chain: []pipeline.StageSpec{
			{Name: "gemini", Version: "v1", QueueName: "gemini"},
			{Name: "normalize_card_statement", Version: "v1", QueueName: "normalize_card_statement"},
			{Name: "enrich_transactions", Version: "v1", QueueName: "enrich_transactions"},
		},
	}
	if err := st.PutJob(context.Background(), rec); err != nil {
		t.Fatalf("put job: %v", err)
	}
}

func readJSON(t *testing.T, s object.Store, uri string, v any) {
	t.Helper()
	body, err := s.Get(context.Background(), uri)
	if err != nil {
		t.Fatalf("get %q: %v", uri, err)
	}
	if err := json.Unmarshal(body, v); err != nil {
		t.Fatalf("decode %q: %v", uri, err)
	}
}

func fixedNow(rfc3339 string) func() time.Time {
	t, err := time.Parse(time.RFC3339, rfc3339)
	if err != nil {
		panic(err)
	}
	return func() time.Time { return t }
}

// spyStore wraps another object.Store and counts writes. Used by the
// rerun test to assert that a short-circuited archive-result run
// issues NO Put* calls.
type spyStore struct {
	inner object.Store
	puts  int
}

func (s *spyStore) Get(ctx context.Context, uri string) ([]byte, error) {
	return s.inner.Get(ctx, uri)
}

func (s *spyStore) PutJSON(ctx context.Context, key string, v any) (string, error) {
	s.puts++
	return s.inner.PutJSON(ctx, key, v)
}

func (s *spyStore) PutText(ctx context.Context, key string, text string) (string, error) {
	s.puts++
	return s.inner.PutText(ctx, key, text)
}

func (s *spyStore) PutBytes(ctx context.Context, key string, body []byte, contentType string) (string, error) {
	s.puts++
	return s.inner.PutBytes(ctx, key, body, contentType)
}

func (s *spyStore) Exists(ctx context.Context, uri string) (bool, error) {
	return s.inner.Exists(ctx, uri)
}

func (s *spyStore) URIFor(key string) string {
	return s.inner.URIFor(key)
}
