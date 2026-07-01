// index_kowloon.go implements the IndexKowloon SFN task from
// kowloon-integration §6: the single HTTP hop between Lady Glass and
// Kowloon. It reads the manifest ArchiveResult produced, hands the
// archive URI to Kowloon's /v1/index-result endpoint, and persists
// Kowloon's typed response as a sidecar so a rerun (SFN retry,
// Lambda re-invocation, manual re-drive) short-circuits before
// spending another HTTP round-trip.
//
// Same shape as ArchiveResult (per-job workflow step, not a per-page
// chain stage): the archive body is per-job, the manifest is per-job,
// and Kowloon takes ONE result_uri per job, so treating IndexKowloon
// as a chain stage would just call Kowloon N times per page under
// last-writer-wins.
//
// Two idempotency layers (§6.5):
//
//   - Lady Glass side: the sidecar at index/jobs/<job_id>.json IS the
//     "we've been here" marker. When it exists, IndexKowloon returns
//     the cached response without touching Kowloon.
//   - Kowloon side: Kowloon's own KowloonIdempotency table dedupes on
//     (job_id, result_uri, schema_version, model, dim, content_hash),
//     so a Lady Glass-side crash between "call Kowloon" and "write
//     sidecar" is still safe — the second call reaches Kowloon,
//     receives the SAME IndexJobID, and writes the sidecar afresh.
//
// The result: SQS redelivery, Lambda re-invocation, and Step Functions
// retry cost at most one HTTP GET when the sidecar is present, and
// at most one HTTP POST when it isn't — no double indexing, no
// divergent identifiers.
package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/keix/lady-glass/internal/client/kowloon"
	"github.com/keix/lady-glass/internal/object"
	"github.com/keix/lady-glass/internal/store"
)

// IndexKowloonInput is the SFN task payload. JobID is the primary
// key; TenantID mirrors the archive's tenant partition so it can be
// forwarded to Kowloon (which uses it to scope search results on
// the other side of the wire).
//
// ManifestURI is the pointer to the archive manifest that
// ArchiveResult produced. Passing it explicitly (rather than
// re-deriving it from JobID + destination.URIFor) keeps this step
// decoupled from the archive-key convention: if the manifest key
// shape ever changes, only ArchiveResult needs updating.
type IndexKowloonInput struct {
	JobID       string `json:"job_id"`
	TenantID    string `json:"tenant_id"`
	ManifestURI string `json:"manifest_uri"`
}

// IndexKowloonOutput carries Kowloon's identifiers so SFN can persist
// them onto the JobRecord (or a metrics sink) and Indexed tells the
// caller whether this run reached Kowloon or short-circuited on a
// prior run's sidecar.
type IndexKowloonOutput struct {
	JobID          string    `json:"job_id"`
	Indexed        bool      `json:"indexed"`
	Collection     string    `json:"collection,omitempty"`
	IndexJobID     string    `json:"index_job_id"`
	VectorCount    int       `json:"vector_count"`
	EmbeddingModel string    `json:"embedding_model,omitempty"`
	IndexedAt      time.Time `json:"indexed_at"`
	SidecarURI     string    `json:"sidecar_uri"`
}

// indexSidecar is the persisted shape at index/jobs/<job_id>.json —
// a superset of Kowloon's response with the request identifiers, so
// a downstream auditor can pair the archive URI with the index job
// without another Kowloon call.
type indexSidecar struct {
	JobID       string                       `json:"job_id"`
	TenantID    string                       `json:"tenant_id"`
	ResultURI   string                       `json:"result_uri"`
	ArchivedAt  string                       `json:"archived_at"`
	IndexedAt   time.Time                    `json:"indexed_at"`
	SchemaVer   string                       `json:"schema_version"`
	ResultType  string                       `json:"result_type"`
	KowloonResp kowloon.IndexResultResponse  `json:"kowloon_response"`
	Request     kowloon.IndexResultRequest   `json:"request,omitempty"`
}

// IndexKowloon reads the archive manifest, calls Kowloon, and writes
// a sidecar recording the response. destination holds both the
// manifest (produced by ArchiveResult) and the sidecar (produced
// here) so a single object.Store is enough.
//
// client is injected so tests can swap in a fake Kowloon; a nil
// client is a programming error (there's no "skip Kowloon" mode).
// now is used only for observability (last-seen time in the sidecar);
// nil defaults to time.Now.
func IndexKowloon(
	ctx context.Context,
	in IndexKowloonInput,
	st store.Store,
	destination object.Store,
	client kowloon.Client,
	now func() time.Time,
) (IndexKowloonOutput, error) {
	if in.JobID == "" {
		return IndexKowloonOutput{}, fmt.Errorf("index_kowloon: empty job_id")
	}
	if in.TenantID == "" {
		return IndexKowloonOutput{}, fmt.Errorf("index_kowloon: empty tenant_id")
	}
	if in.ManifestURI == "" {
		return IndexKowloonOutput{}, fmt.Errorf("index_kowloon: empty manifest_uri")
	}
	if client == nil {
		return IndexKowloonOutput{}, fmt.Errorf("index_kowloon: nil client")
	}
	if now == nil {
		now = time.Now
	}

	// Lady Glass-side idempotency gate: the sidecar's existence means
	// this job has already been indexed and Kowloon has an entry for
	// it. Nothing to do beyond returning the cached identifiers.
	sidecarKey := fmt.Sprintf("index/jobs/%s.json", in.JobID)
	sidecarURI := destination.URIFor(sidecarKey)
	exists, err := destination.Exists(ctx, sidecarURI)
	if err != nil {
		return IndexKowloonOutput{}, fmt.Errorf("index_kowloon: probe sidecar: %w", err)
	}
	if exists {
		body, err := destination.Get(ctx, sidecarURI)
		if err != nil {
			return IndexKowloonOutput{}, fmt.Errorf("index_kowloon: read existing sidecar: %w", err)
		}
		var prev indexSidecar
		if err := json.Unmarshal(body, &prev); err != nil {
			return IndexKowloonOutput{}, fmt.Errorf("index_kowloon: decode existing sidecar: %w", err)
		}
		return IndexKowloonOutput{
			JobID:          in.JobID,
			Indexed:        false,
			Collection:     prev.KowloonResp.KowloonCollection,
			IndexJobID:     prev.KowloonResp.IndexJobID,
			VectorCount:    prev.KowloonResp.VectorCount,
			EmbeddingModel: prev.KowloonResp.EmbeddingModel,
			IndexedAt:      prev.KowloonResp.IndexedAt,
			SidecarURI:     sidecarURI,
		}, nil
	}

	// JobRecord is a defensive load: not currently used to shape the
	// request, but a missing row is the same "invoked before the
	// upstream steps landed" case that ArchiveResult guards against,
	// and failing loudly here saves the operator a round of "why did
	// Kowloon get a request for an unknown job" triage.
	if job, err := st.GetJob(ctx, in.JobID); err != nil {
		return IndexKowloonOutput{}, fmt.Errorf("index_kowloon: get job: %w", err)
	} else if job == nil {
		return IndexKowloonOutput{}, fmt.Errorf("index_kowloon: job %q not found", in.JobID)
	}

	manifestBody, err := destination.Get(ctx, in.ManifestURI)
	if err != nil {
		return IndexKowloonOutput{}, fmt.Errorf("index_kowloon: read manifest %q: %w", in.ManifestURI, err)
	}
	var mf Manifest
	if err := json.Unmarshal(manifestBody, &mf); err != nil {
		return IndexKowloonOutput{}, fmt.Errorf("index_kowloon: decode manifest: %w", err)
	}
	if mf.Results.Transactions == "" {
		return IndexKowloonOutput{}, fmt.Errorf("index_kowloon: manifest %q has no results.transactions", in.ManifestURI)
	}
	// The manifest is authoritative for tenant_id — if the SFN input
	// somehow disagrees with the manifest, the manifest wins. The
	// alternative is silently indexing a job under the wrong tenant
	// scope, which is a much worse failure mode than a mismatch error.
	if mf.TenantID != "" && mf.TenantID != in.TenantID {
		return IndexKowloonOutput{}, fmt.Errorf(
			"index_kowloon: tenant mismatch — manifest %q, input %q",
			mf.TenantID, in.TenantID,
		)
	}

	req := kowloon.IndexResultRequest{
		JobID:         in.JobID,
		TenantID:      in.TenantID,
		ResultURI:     mf.Results.Transactions,
		ResultType:    "transactions",
		SchemaVersion: "transactions.v1",
	}
	resp, err := client.IndexResult(ctx, req)
	if err != nil {
		// Preserve the typed error so SFN's retry policy can classify:
		// SchemaError → do not retry (permanent), TransientError →
		// backoff. The wrapping fmt.Errorf keeps the operator context
		// prefix without stripping the underlying type (errors.As on
		// the caller side still finds *SchemaError / *TransientError).
		var schema *kowloon.SchemaError
		var transient *kowloon.TransientError
		switch {
		case errors.As(err, &schema):
			return IndexKowloonOutput{}, fmt.Errorf("index_kowloon: schema rejected: %w", err)
		case errors.As(err, &transient):
			return IndexKowloonOutput{}, fmt.Errorf("index_kowloon: transient: %w", err)
		default:
			return IndexKowloonOutput{}, fmt.Errorf("index_kowloon: %w", err)
		}
	}

	sidecar := indexSidecar{
		JobID:       in.JobID,
		TenantID:    in.TenantID,
		ResultURI:   mf.Results.Transactions,
		ArchivedAt:  mf.ArchivedAt,
		IndexedAt:   now().UTC(),
		SchemaVer:   req.SchemaVersion,
		ResultType:  req.ResultType,
		KowloonResp: resp,
		Request:     req,
	}
	writtenURI, err := destination.PutJSON(ctx, sidecarKey, sidecar)
	if err != nil {
		return IndexKowloonOutput{}, fmt.Errorf("index_kowloon: write sidecar: %w", err)
	}

	return IndexKowloonOutput{
		JobID:          in.JobID,
		Indexed:        true,
		Collection:     resp.KowloonCollection,
		IndexJobID:     resp.IndexJobID,
		VectorCount:    resp.VectorCount,
		EmbeddingModel: resp.EmbeddingModel,
		IndexedAt:      resp.IndexedAt,
		SidecarURI:     writtenURI,
	}, nil
}
