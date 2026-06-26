// Package cardstatement is the credit-card-statement post-processor that
// runs after the Gemini extraction stage. It is intentionally narrow in
// scope: it does NOT re-OCR, re-extract, or talk to any provider — it
// only reads the typed PageExtractionResult that the upstream stage
// emitted and produces a cleaner version of the same shape.
//
// v1 fixes two classes of upstream artefact that aggregate() over the
// raw Gemini output would surface as wrong totals:
//
//   - "phantom" rows: schedule / future-payment tables that the model
//     classifies as Transaction because they share the layout. The rule
//     is "drop any transaction whose merchant matches the page's
//     issuer_name", which catches the common case (PayPay statements
//     putting an issuer-named future-payment forecast on page 3) without
//     needing a per-issuer whitelist.
//   - zero-amount rows: cancellation/adjustment lines and future-payment
//     schedule entries with amount=0. They never represent actual spend
//     and they corrupt count totals if left in.
//
// What the stage explicitly does NOT change:
//
//   - Date / Amount / Merchant strings stay verbatim. The
//     pipeline.Transaction contract still promises "source representation
//     preserved losslessly". Format canonicalisation (ISO dates, integer
//     amounts) is a separate concern.
//   - Fields metadata is forwarded as-is.
//   - Text and DocumentType are forwarded as-is.
//
// A future v2 may layer issuer-specific rules on top of these defaults
// (per-issuer drop predicates, label normalisation). The Step's
// (Name, Version) is the spec-defined hinge for that; bumping Version
// is the only way new behaviour can ship without affecting in-flight
// jobs already running under v1 (SPEC §S2).
package cardstatement

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/keix/lady-glass/internal/object"
	"github.com/keix/lady-glass/internal/pipeline"
)

// Step is the stage.Step implementation. Objects is the artifact store
// (S3 in prod, FileStore in tests); previous-stage URIs are read from
// it and the normalised result is written back to it under the
// canonical pages/<n>/normalize_card_statement/v1/result.json key.
type Step struct {
	Objects object.Store
}

func (s *Step) Name() string    { return "normalize_card_statement" }
func (s *Step) Version() string { return "v1" }

func (s *Step) Run(ctx context.Context, in pipeline.StepInput) (pipeline.StepOutput, error) {
	if in.PreviousURI == "" {
		return pipeline.StepOutput{}, fmt.Errorf("normalize_card_statement: empty previous_uri (nothing to normalise)")
	}

	body, err := s.Objects.Get(ctx, in.PreviousURI)
	if err != nil {
		return pipeline.StepOutput{}, fmt.Errorf("normalize_card_statement: fetch previous %q: %w", in.PreviousURI, err)
	}

	var page pipeline.PageExtractionResult
	if err := json.Unmarshal(body, &page); err != nil {
		return pipeline.StepOutput{}, fmt.Errorf("normalize_card_statement: decode previous: %w", err)
	}

	page.Transactions = filterTransactions(page.Transactions, stringField(page.Fields, "issuer_name", "issuer"))

	out, err := json.Marshal(page)
	if err != nil {
		return pipeline.StepOutput{}, fmt.Errorf("normalize_card_statement: marshal: %w", err)
	}

	key := fmt.Sprintf("jobs/%s/pages/%06d/normalize_card_statement/v1/result.json", in.JobID, in.Page)
	resultURI, err := s.Objects.PutBytes(ctx, key, out, "application/json")
	if err != nil {
		return pipeline.StepOutput{}, fmt.Errorf("normalize_card_statement: persist result: %w", err)
	}

	return pipeline.StepOutput{
		JobID:     in.JobID,
		Page:      in.Page,
		Stage:     s.Name(),
		Version:   s.Version(),
		ResultURI: resultURI,
		JSONURI:   resultURI,
	}, nil
}

// filterTransactions applies the v1 drop rules. issuerName is the
// page's issuer field value, used to identify phantom rows whose
// merchant string equals the issuer (the PayPay page-3 case).
func filterTransactions(txs []pipeline.Transaction, issuerName string) []pipeline.Transaction {
	if len(txs) == 0 {
		return txs
	}
	issuer := canonicalIssuer(issuerName)
	kept := make([]pipeline.Transaction, 0, len(txs))
	for _, tx := range txs {
		if issuer != "" && canonicalIssuer(tx.Merchant) == issuer {
			continue
		}
		if amountIsZero(tx.Amount) {
			continue
		}
		kept = append(kept, tx)
	}
	return kept
}

// canonicalIssuer normalises an issuer-name-like string so a per-row
// merchant value can be compared against the page-level issuer field
// despite trivial differences (case, surrounding whitespace, the
// Japanese 株式会社 corporate suffix). Cross-script differences
// ("PayPayカード" vs "PayPay Card") are intentionally NOT collapsed:
// the comparison is within one page, where Gemini emits a single
// consistent rendering, so widening the match here only adds false-
// positive risk.
func canonicalIssuer(s string) string {
	s = strings.TrimSpace(s)
	for _, suffix := range []string{"株式会社", "(株)", "(株)"} {
		s = strings.TrimSuffix(s, suffix)
	}
	return strings.ToLower(strings.TrimSpace(s))
}

// amountIsZero reports whether the printed Amount represents zero or an
// unfilled value. Strings that fail to parse are kept (the aggregate
// layer already skips unparseable rows; double-dropping risks losing
// legitimate transactions whose printed form we have not seen yet).
func amountIsZero(s string) bool {
	s = strings.ReplaceAll(s, ",", "")
	s = strings.TrimSuffix(s, "円")
	s = strings.TrimSpace(s)
	if s == "" {
		return true
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return false
	}
	return v == 0
}

// stringField reads a free-form Fields entry as a string, trying each
// key in priority order. Anything non-string maps to the empty string
// so callers do not have to guard against the model emitting a number
// where a label was expected.
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
