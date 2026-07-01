// archive_result.go implements the ArchiveResult SFN task from
// kowloon-integration §5: it copies the merged per-job result and the
// raw input PDF from the 14-day stage bucket into the permanent bucket,
// and writes a per-job manifest that lets Kowloon (and any future
// auditor) rebuild the index without Lady Glass being reachable.
//
// The step is per-job — it runs AFTER Merge, not as a per-page chain
// stage. The manifest key (manifests/jobs/<job_id>.json) and the
// archive key (results/transactions/tenant=<tid>/year=<yr>/month=<mo>/
// <card_slug>.json) both carry per-job semantics, and the Kowloon call
// downstream expects one result_uri per job. Doing this as a chain
// stage would just write the same manifest N times per page and rely
// on last-writer-wins to keep the state consistent.
//
// Idempotency (§5.6): the manifest is written LAST, and the whole step
// short-circuits when it already exists at the deterministic key. That
// makes SFN task retries, Lambda re-invocations, and a manual re-drive
// all no-ops on the S3 side — no PutObject is issued, no cost is
// incurred, and the returned URIs point at the objects the first run
// wrote. Callers can distinguish the two paths via the Archived flag
// on the returned output.
package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/keix/lady-glass/internal/object"
	"github.com/keix/lady-glass/internal/pipeline"
	"github.com/keix/lady-glass/internal/store"
)

// ArchiveResultInput is the SFN task payload. JobID identifies the job
// whose merged result should be archived; TenantID is the customer
// scope injected by the API layer at job creation, carried through the
// workflow as free-form task input.
//
// TenantID is required — the archive key includes it as a partition
// (tenant=<tid>/...) so Kowloon can enforce tenant scoping on search
// results without walking each match's metadata.
type ArchiveResultInput struct {
	JobID    string `json:"job_id"`
	TenantID string `json:"tenant_id"`
}

// ArchiveResultOutput carries the URIs of the three persisted objects
// so a downstream step (index-kowloon) can point Kowloon at the archive
// without another DDB lookup, and Archived tells the caller whether
// this run actually wrote anything or short-circuited on a prior run's
// manifest.
type ArchiveResultOutput struct {
	JobID       string `json:"job_id"`
	Archived    bool   `json:"archived"`
	ArchiveURI  string `json:"archive_uri"`
	RawURI      string `json:"raw_uri,omitempty"`
	ManifestURI string `json:"manifest_uri"`
}

// Manifest is the persisted manifests/jobs/<job_id>.json — the
// pointer-only record Kowloon reads to locate the archive body without
// scanning the permanent bucket.
type Manifest struct {
	JobID      string          `json:"job_id"`
	TenantID   string          `json:"tenant_id"`
	Source     ManifestSource  `json:"source"`
	Results    ManifestResults `json:"results"`
	Chain      ManifestChain   `json:"chain"`
	ArchivedAt string          `json:"archived_at"`
}

// ManifestSource describes the original ingest artifact. Kind is fixed
// as "card_statement" in v1; a future document family (receipt,
// invoice) surfaces here without changing the manifest schema.
type ManifestSource struct {
	Kind     string `json:"kind"`
	Issuer   string `json:"issuer,omitempty"`
	CardName string `json:"card_name,omitempty"`
	RawURI   string `json:"raw_uri,omitempty"`
}

// ManifestResults lists the archive body URIs, one per Kowloon
// collection. Only transactions.v1 exists in v1; a summary or embedding
// artifact would land as an additional key here.
type ManifestResults struct {
	Transactions string `json:"transactions"`
}

// ManifestChain is the frozen chain shape at the time of archiving —
// so a downstream re-index (e.g. Kowloon rebuilding from S3) knows
// exactly which stages produced the archive body.
type ManifestChain struct {
	ID     string   `json:"id"`
	Stages []string `json:"stages"`
}

// ArchivedDocument is the shape written to results/transactions/...
// It is intentionally FLAT — doc-level Fields plus a concatenated
// Transactions slice across every page — so Kowloon's transactions.v1
// schema converter can walk the record without knowing about Lady
// Glass's per-page nesting.
type ArchivedDocument struct {
	JobID        string                 `json:"job_id"`
	TenantID     string                 `json:"tenant_id"`
	DocumentType pipeline.DocumentType  `json:"document_type,omitempty"`
	Fields       map[string]any         `json:"fields,omitempty"`
	Transactions []pipeline.Transaction `json:"transactions"`
}

// tenantSafe restricts TenantID to characters that keep the S3 key
// unambiguous: no whitespace, no path separators, no "=" (which would
// collide with the tenant=<tid> partition marker), no ".." (which
// would let a caller escape the tenant partition). This is a
// authorisation-adjacent boundary — the value comes from the API
// layer, but archive-result is the last line of defence before it
// becomes an object key.
var tenantSafe = regexp.MustCompile(`^[a-zA-Z0-9_\-]+$`)

// ArchiveResult performs the copy + manifest write. source is the
// 14-day stage bucket (holds Merge's output at JobRecord.ResultURI and
// the raw input PDF at JobRecord.InputURI). destination is the
// permanent bucket. now supplies archived_at and the year/month
// fallback when Fields lacks a statement_date.
//
// The three side-effects, in order:
//
//  1. write ArchivedDocument JSON at results/transactions/tenant=<tid>/
//     year=<yr>/month=<mo>/<card_slug>.json
//  2. copy raw PDF from source to raw/card-statements/<issuer_slug>/
//     <yr>/<mo>/original.pdf (skipped when JobRecord.InputURI is empty)
//  3. write Manifest JSON at manifests/jobs/<job_id>.json
//
// Order matters: the manifest is written last so that a mid-flight
// failure (S3 partial outage, Lambda kill) leaves the manifest absent
// and the next run treats the state as "fresh" and re-writes both
// bodies — safe because bodies are content-deterministic. Reversing
// the order would leave a manifest pointing at half-written bodies.
func ArchiveResult(
	ctx context.Context,
	in ArchiveResultInput,
	st store.Store,
	source object.Store,
	destination object.Store,
	now func() time.Time,
) (ArchiveResultOutput, error) {
	if in.JobID == "" {
		return ArchiveResultOutput{}, fmt.Errorf("archive_result: empty job_id")
	}
	if in.TenantID == "" {
		return ArchiveResultOutput{}, fmt.Errorf("archive_result: empty tenant_id")
	}
	if !tenantSafe.MatchString(in.TenantID) {
		return ArchiveResultOutput{}, fmt.Errorf("archive_result: tenant_id %q contains unsafe characters", in.TenantID)
	}
	if now == nil {
		now = time.Now
	}

	job, err := st.GetJob(ctx, in.JobID)
	if err != nil {
		return ArchiveResultOutput{}, fmt.Errorf("archive_result: get job: %w", err)
	}
	if job == nil {
		return ArchiveResultOutput{}, fmt.Errorf("archive_result: job %q not found", in.JobID)
	}
	if job.ResultURI == "" {
		return ArchiveResultOutput{}, fmt.Errorf("archive_result: job %q has no merged result_uri", in.JobID)
	}

	mergedBody, err := source.Get(ctx, job.ResultURI)
	if err != nil {
		return ArchiveResultOutput{}, fmt.Errorf("archive_result: read merged %q: %w", job.ResultURI, err)
	}
	var merged MergedDocument
	if err := json.Unmarshal(mergedBody, &merged); err != nil {
		return ArchiveResultOutput{}, fmt.Errorf("archive_result: decode merged: %w", err)
	}

	fields, txs, docType, err := flattenPages(merged.Pages)
	if err != nil {
		return ArchiveResultOutput{}, err
	}
	issuer := stringField(fields, "issuer_name", "issuer")
	cardName := stringField(fields, "card_name", "card")
	year, month := yearMonth(stringField(fields, "statement_date"), now())

	issuerSlug := slugify(issuer)
	if issuerSlug == "" {
		issuerSlug = "unknown"
	}
	cardSlug := deriveCardSlug(issuerSlug, cardName, in.JobID)

	archiveKey := fmt.Sprintf(
		"results/transactions/tenant=%s/year=%s/month=%s/%s.json",
		in.TenantID, year, month, cardSlug,
	)
	rawKey := fmt.Sprintf(
		"raw/card-statements/%s/%s/%s/original.pdf",
		issuerSlug, year, month,
	)
	manifestKey := fmt.Sprintf("manifests/jobs/%s.json", in.JobID)
	manifestURI := destination.URIFor(manifestKey)

	// §5.6 short-circuit: the manifest is the atomic "we've been here"
	// marker. If it exists, the previous run's URIs are the answer —
	// don't re-issue any PutObject.
	exists, err := destination.Exists(ctx, manifestURI)
	if err != nil {
		return ArchiveResultOutput{}, fmt.Errorf("archive_result: probe manifest: %w", err)
	}
	if exists {
		body, err := destination.Get(ctx, manifestURI)
		if err != nil {
			return ArchiveResultOutput{}, fmt.Errorf("archive_result: read existing manifest: %w", err)
		}
		var existing Manifest
		if err := json.Unmarshal(body, &existing); err != nil {
			return ArchiveResultOutput{}, fmt.Errorf("archive_result: decode existing manifest: %w", err)
		}
		return ArchiveResultOutput{
			JobID:       in.JobID,
			Archived:    false,
			ArchiveURI:  existing.Results.Transactions,
			RawURI:      existing.Source.RawURI,
			ManifestURI: manifestURI,
		}, nil
	}

	archive := ArchivedDocument{
		JobID:        in.JobID,
		TenantID:     in.TenantID,
		DocumentType: docType,
		Fields:       fields,
		Transactions: txs,
	}
	archiveURI, err := destination.PutJSON(ctx, archiveKey, archive)
	if err != nil {
		return ArchiveResultOutput{}, fmt.Errorf("archive_result: write archive: %w", err)
	}

	// Raw PDF copy is best-effort skipped when the job has no input
	// URI (an unusual mode where the API layer wrote pages directly).
	// The manifest simply carries an empty raw_uri in that case rather
	// than failing the whole archive.
	rawURI := ""
	if job.InputURI != "" {
		rawBody, err := source.Get(ctx, job.InputURI)
		if err != nil {
			return ArchiveResultOutput{}, fmt.Errorf("archive_result: read raw %q: %w", job.InputURI, err)
		}
		rawURI, err = destination.PutBytes(ctx, rawKey, rawBody, "application/pdf")
		if err != nil {
			return ArchiveResultOutput{}, fmt.Errorf("archive_result: write raw: %w", err)
		}
	}

	manifest := Manifest{
		JobID:    in.JobID,
		TenantID: in.TenantID,
		Source: ManifestSource{
			Kind:     "card_statement",
			Issuer:   issuer,
			CardName: cardName,
			RawURI:   rawURI,
		},
		Results:    ManifestResults{Transactions: archiveURI},
		Chain:      chainDescriptor(job),
		ArchivedAt: now().UTC().Format(time.RFC3339),
	}
	writtenManifestURI, err := destination.PutJSON(ctx, manifestKey, manifest)
	if err != nil {
		return ArchiveResultOutput{}, fmt.Errorf("archive_result: write manifest: %w", err)
	}
	if writtenManifestURI != manifestURI {
		return ArchiveResultOutput{}, fmt.Errorf(
			"archive_result: URIFor/PutJSON URI mismatch: %q vs %q",
			manifestURI, writtenManifestURI,
		)
	}

	return ArchiveResultOutput{
		JobID:       in.JobID,
		Archived:    true,
		ArchiveURI:  archiveURI,
		RawURI:      rawURI,
		ManifestURI: writtenManifestURI,
	}, nil
}

// flattenPages walks the per-page MergedPage list and produces:
//
//   - Fields: first non-empty value wins for each key. Statement-level
//     metadata (issuer_name, statement_date) is usually on page 1, but
//     falling back to later pages when page 1 lacks a value survives
//     OCR / prompt glitches on the first page.
//   - Transactions: concatenated in page order — the aggregate over the
//     whole document, which is what Kowloon indexes.
//   - DocumentType: taken from the first page that declares one.
func flattenPages(pages []MergedPage) (map[string]any, []pipeline.Transaction, pipeline.DocumentType, error) {
	fields := map[string]any{}
	var txs []pipeline.Transaction
	var docType pipeline.DocumentType

	for _, mp := range pages {
		var page pipeline.PageExtractionResult
		if len(mp.Result) == 0 {
			continue
		}
		if err := json.Unmarshal(mp.Result, &page); err != nil {
			return nil, nil, "", fmt.Errorf("archive_result: decode page %d: %w", mp.Page, err)
		}
		if docType == "" && page.DocumentType != "" {
			docType = page.DocumentType
		}
		for k, v := range page.Fields {
			if _, seen := fields[k]; seen {
				continue
			}
			if !isEmptyValue(v) {
				fields[k] = v
			}
		}
		txs = append(txs, page.Transactions...)
	}

	if len(fields) == 0 {
		fields = nil
	}
	return fields, txs, docType, nil
}

// chainDescriptor projects the frozen chain on the JobRecord into the
// manifest's small pointer shape (chain id + stage names). Empty when
// the JobRecord predates the chain-binding feature (chain is empty on
// pre-§S10 rows).
func chainDescriptor(job *store.JobRecord) ManifestChain {
	if job == nil {
		return ManifestChain{}
	}
	names := make([]string, len(job.Chain))
	for i, s := range job.Chain {
		names[i] = s.Name
	}
	return ManifestChain{ID: job.ChainID, Stages: names}
}

// deriveCardSlug picks a stable filename component for the archive
// key. In priority order: "<issuer>-<card>", "<card>", "<issuer>",
// then the last 12 chars of the JobID. The JobID fallback avoids ever
// producing a key that collides across jobs when the extraction
// stage failed to pull any header field.
func deriveCardSlug(issuerSlug, cardName, jobID string) string {
	card := slugify(cardName)
	switch {
	case issuerSlug != "" && issuerSlug != "unknown" && card != "":
		return issuerSlug + "-" + card
	case card != "":
		return card
	case issuerSlug != "" && issuerSlug != "unknown":
		return issuerSlug
	default:
		if len(jobID) > 12 {
			return jobID[len(jobID)-12:]
		}
		return jobID
	}
}

// yearMonth extracts (yyyy, mm) from the statement_date field on the
// page. Recognises the common representations Gemini emits: ISO
// ("2026-06-10"), slash-separated ("2026/06/10"), and Japanese
// ("2026年6月10日"). When no year+month can be pulled, falls back to
// the archived_at time so the object still lands under a sensible
// partition instead of an empty one.
func yearMonth(statementDate string, fallback time.Time) (string, string) {
	if y, m, ok := parseYearMonth(statementDate); ok {
		return y, m
	}
	fb := fallback.UTC()
	return fmt.Sprintf("%04d", fb.Year()), fmt.Sprintf("%02d", int(fb.Month()))
}

var yearMonthRE = regexp.MustCompile(`(\d{4})[\-/年](\d{1,2})`)

func parseYearMonth(s string) (string, string, bool) {
	m := yearMonthRE.FindStringSubmatch(s)
	if len(m) != 3 {
		return "", "", false
	}
	year := m[1]
	monthNum := m[2]
	if len(monthNum) == 1 {
		monthNum = "0" + monthNum
	}
	return year, monthNum, true
}

// slugify collapses a free-form string into an S3-key-safe token:
// lowercase, whitespace → hyphen, non-alphanumeric-hyphen dropped.
// Multiple consecutive hyphens collapse. Non-ASCII characters are
// dropped entirely rather than transliterated — issuer / card names on
// real card statements always have an ASCII rendering somewhere in
// Fields, so a Japanese-only string falling through to the JobID
// fallback is the safer outcome than an unreadable slug.
func slugify(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	var b strings.Builder
	prevHyphen := false
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
			prevHyphen = false
		case r == ' ' || r == '-' || r == '_' || r == '\t':
			if !prevHyphen && b.Len() > 0 {
				b.WriteRune('-')
				prevHyphen = true
			}
		}
	}
	out := b.String()
	return strings.TrimRight(out, "-")
}

// stringField reads a free-form Fields entry as a string, trying each
// key in priority order. Numeric or nested values map to "" so callers
// do not have to guard against the model emitting a value where a
// label was expected. Matches the helper in normalize/cardstatement.
func stringField(fields map[string]any, keys ...string) string {
	for _, k := range keys {
		v, ok := fields[k]
		if !ok {
			continue
		}
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// isEmptyValue lets flattenPages skip Fields entries that carry an
// empty string (Gemini occasionally emits "" for headers it couldn't
// find). A page-2 non-empty value should still win over a page-1 "".
func isEmptyValue(v any) bool {
	if v == nil {
		return true
	}
	if s, ok := v.(string); ok {
		return strings.TrimSpace(s) == ""
	}
	return false
}

// sortedKeys is a tiny helper for deterministic test assertions on
// the flattened Fields map. Exported for tests only; kept in the
// production file to avoid an internal _test.go with production
// helpers.
func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
