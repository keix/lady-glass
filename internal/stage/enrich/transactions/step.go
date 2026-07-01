// Package transactions is the enrich_transactions stage: it takes a
// PageExtractionResult already normalised by the upstream stage and
// attaches three metadata fields to every Transaction so downstream
// indexing and aggregation have stable keys to work with:
//
//   - MerchantNormalized: canonical merchant name from the embedded
//     dictionary (merchants.yaml). Collapses OCR / provider variants of
//     the same merchant ("スターバックス コーヒー" /
//     "スターバックスコーヒージャパン") onto a single label ("Starbucks").
//   - Category: convenience_store / cafe / restaurant / … as tagged in
//     the dictionary. Only written when the enrich stage found a match
//     AND the upstream row did not already carry a category — the
//     upstream classification wins if it exists.
//   - Country: ISO 3166-1 alpha-2. Dictionary entry Country wins first;
//     otherwise derived from ForeignCurrency (MYR→MY, SGD→SG, USD→US,
//     THB→TH); otherwise defaults to "JP" when ForeignCurrency is empty
//     (a JPY-only row).
//
// The stage is intentionally rules-only in v1. §10 Phase 3 of the
// Kowloon integration design layers a Gemini-backed fallback on top for
// merchants the dictionary does not know about; a v2 bump is the hinge
// for that. Until then, unknown merchants pass through with the three
// new fields left empty — downstream code MUST tolerate that (Kowloon's
// transactions.v1 converter already does).
//
// Idempotency is delegated to the executor via the standard StageKey
// (JobID + Page + enrich_transactions + v1). Re-running against the
// same input produces byte-identical output as long as merchants.yaml
// is unchanged.
package transactions

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"strings"

	"gopkg.in/yaml.v2"

	"github.com/keix/lady-glass/internal/object"
	"github.com/keix/lady-glass/internal/pipeline"
)

// Step is the stage.Step implementation. Dictionary is the resolved
// merchant → canonical mapping (see LoadDictionary and DefaultDictionary);
// Objects is the artifact store used to read the previous stage's
// result and write the enriched one back under the canonical
// pages/<n>/enrich_transactions/v1/result.json key.
type Step struct {
	Dictionary *Dictionary
	Objects    object.Store
}

func (s *Step) Name() string    { return "enrich_transactions" }
func (s *Step) Version() string { return "v1" }

func (s *Step) Run(ctx context.Context, in pipeline.StepInput) (pipeline.StepOutput, error) {
	if in.PreviousURI == "" {
		return pipeline.StepOutput{}, fmt.Errorf("enrich_transactions: empty previous_uri (nothing to enrich)")
	}

	body, err := s.Objects.Get(ctx, in.PreviousURI)
	if err != nil {
		return pipeline.StepOutput{}, fmt.Errorf("enrich_transactions: fetch previous %q: %w", in.PreviousURI, err)
	}

	var page pipeline.PageExtractionResult
	if err := json.Unmarshal(body, &page); err != nil {
		return pipeline.StepOutput{}, fmt.Errorf("enrich_transactions: decode previous: %w", err)
	}

	dict := s.Dictionary
	if dict == nil {
		dict = DefaultDictionary()
	}
	page.Transactions = enrichTransactions(page.Transactions, dict)

	out, err := json.Marshal(page)
	if err != nil {
		return pipeline.StepOutput{}, fmt.Errorf("enrich_transactions: marshal: %w", err)
	}

	key := fmt.Sprintf("jobs/%s/pages/%06d/enrich_transactions/v1/result.json", in.JobID, in.Page)
	resultURI, err := s.Objects.PutBytes(ctx, key, out, "application/json")
	if err != nil {
		return pipeline.StepOutput{}, fmt.Errorf("enrich_transactions: persist result: %w", err)
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

// enrichTransactions applies the dictionary and currency→country rules
// to every row. Rows the dictionary does not know about pass through
// unchanged except for Country, which is derived from ForeignCurrency
// when possible.
func enrichTransactions(txs []pipeline.Transaction, dict *Dictionary) []pipeline.Transaction {
	if len(txs) == 0 {
		return txs
	}
	enriched := make([]pipeline.Transaction, len(txs))
	for i, tx := range txs {
		if entry := dict.Lookup(tx.Merchant); entry != nil {
			tx.MerchantNormalized = entry.Canonical
			// Upstream category (if any) wins — the enrich stage only
			// fills gaps.
			if tx.Category == "" && entry.Category != "" {
				tx.Category = entry.Category
			}
			if entry.Country != "" {
				tx.Country = entry.Country
			}
		}
		if tx.Country == "" {
			tx.Country = countryFromCurrency(tx.ForeignCurrency)
		}
		enriched[i] = tx
	}
	return enriched
}

// countryFromCurrency implements §4.6 of the Kowloon integration design.
// A ForeignCurrency of "" is the JPY-only case and maps to "JP"; a
// currency the table does not know about maps to "" so an operator can
// see the gap in the downstream data rather than a wrong default.
func countryFromCurrency(fc string) string {
	switch strings.ToUpper(strings.TrimSpace(fc)) {
	case "":
		return "JP"
	case "MYR":
		return "MY"
	case "SGD":
		return "SG"
	case "THB":
		return "TH"
	case "USD":
		return "US"
	default:
		return ""
	}
}

// Dictionary is the resolved lookup table used by the stage. Callers
// build it via LoadDictionary (or DefaultDictionary for the embedded
// seed) rather than constructing it directly; the internal map is an
// alias-lowercase → *Entry index so Lookup is O(1) per row.
type Dictionary struct {
	entries []Entry
	byAlias map[string]*Entry
}

// Entry is one canonical merchant with its category, optional country,
// and the raw alias strings from the YAML source. The aliases are kept
// so operators can grep the loaded dictionary for a specific spelling.
type Entry struct {
	Canonical string   `yaml:"canonical"`
	Category  string   `yaml:"category"`
	Country   string   `yaml:"country,omitempty"`
	Aliases   []string `yaml:"aliases"`
}

// Lookup returns the dictionary entry whose canonical name or one of
// whose aliases matches merchant, or nil when the merchant is unknown.
// Matching is case-insensitive with whitespace trimmed; the canonical
// name is also considered a self-match so a row that already carries
// the canonical spelling still gets its category / country attached.
func (d *Dictionary) Lookup(merchant string) *Entry {
	if d == nil {
		return nil
	}
	key := normaliseAlias(merchant)
	if key == "" {
		return nil
	}
	return d.byAlias[key]
}

// Entries returns the loaded entries in file order. Exposed so tests
// (and a future admin CLI) can enumerate the dictionary without going
// through Lookup.
func (d *Dictionary) Entries() []Entry {
	if d == nil {
		return nil
	}
	out := make([]Entry, len(d.entries))
	copy(out, d.entries)
	return out
}

// LoadDictionary parses a merchants.yaml payload into a lookup table.
// Duplicate aliases across entries are a hard error — silently
// overwriting one canonical with another would make the stage's output
// depend on file order in a way that is impossible to debug from a
// diff of the yaml.
func LoadDictionary(body []byte) (*Dictionary, error) {
	var entries []Entry
	if err := yaml.Unmarshal(body, &entries); err != nil {
		return nil, fmt.Errorf("enrich_transactions: parse merchants yaml: %w", err)
	}
	byAlias := make(map[string]*Entry, len(entries)*4)
	for i := range entries {
		e := &entries[i]
		if e.Canonical == "" {
			return nil, fmt.Errorf("enrich_transactions: entry %d has empty canonical", i)
		}
		// Canonical itself is a valid alias so callers don't have to
		// list it twice in the YAML.
		aliases := append([]string{e.Canonical}, e.Aliases...)
		for _, alias := range aliases {
			key := normaliseAlias(alias)
			if key == "" {
				continue
			}
			if existing, dup := byAlias[key]; dup && existing.Canonical != e.Canonical {
				return nil, fmt.Errorf(
					"enrich_transactions: alias %q maps to both %q and %q",
					alias, existing.Canonical, e.Canonical,
				)
			}
			byAlias[key] = e
		}
	}
	return &Dictionary{entries: entries, byAlias: byAlias}, nil
}

//go:embed merchants.yaml
var embeddedMerchants []byte

// DefaultDictionary returns the dictionary parsed from the embedded
// merchants.yaml. Panics on parse failure because the embedded file is
// part of the binary; a broken seed should surface at process start,
// not on the first message that reaches the stage.
func DefaultDictionary() *Dictionary {
	d, err := LoadDictionary(embeddedMerchants)
	if err != nil {
		panic(fmt.Errorf("enrich_transactions: embedded merchants.yaml is invalid: %w", err))
	}
	return d
}

// normaliseAlias is the single point of truth for alias key shape.
// Lowercase + trim collapses the common OCR / provider variants (case,
// leading/trailing whitespace) without touching in-string spacing so
// "スターバックス コーヒー" and "スターバックスコーヒー" remain distinct
// entries — the dictionary carries both when both are observed.
func normaliseAlias(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}
