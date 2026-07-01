package transactions_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/keix/lady-glass/internal/object"
	"github.com/keix/lady-glass/internal/pipeline"
	transactions "github.com/keix/lady-glass/internal/stage/enrich/transactions"
)

// §11.1 of the Kowloon integration design: a merchant that matches an
// alias in the dictionary gets its canonical name AND category attached.
// The alias here is one of the two Starbucks OCR variants that motivated
// the whole enrich stage.
func TestStep_AttachesCanonicalNameAndCategoryOnDictionaryHit(t *testing.T) {
	out := runStep(t, pipeline.PageExtractionResult{
		Transactions: []pipeline.Transaction{
			{Date: "2026-06-10", Merchant: "スターバックス コーヒー", Amount: "480"},
		},
	})
	if len(out.Transactions) != 1 {
		t.Fatalf("expected 1 transaction, got %+v", out.Transactions)
	}
	tx := out.Transactions[0]
	if tx.MerchantNormalized != "Starbucks" {
		t.Fatalf("MerchantNormalized = %q, want %q", tx.MerchantNormalized, "Starbucks")
	}
	if tx.Category != "cafe" {
		t.Fatalf("Category = %q, want %q", tx.Category, "cafe")
	}
	// A JPY row (no foreign currency) defaults to JP even without a
	// dictionary Country override.
	if tx.Country != "JP" {
		t.Fatalf("Country = %q, want %q", tx.Country, "JP")
	}
	// Source fidelity: original Merchant string is preserved verbatim.
	if tx.Merchant != "スターバックス コーヒー" {
		t.Fatalf("Merchant mutated: %q", tx.Merchant)
	}
}

// §11.2: ForeignCurrency drives Country when no dictionary entry
// provides one. The dictionary DOES have Rehan Restaurant marked as MY,
// so this test uses an off-dictionary MYR row to isolate the
// currency→country derivation.
func TestStep_DerivesCountryFromForeignCurrency(t *testing.T) {
	out := runStep(t, pipeline.PageExtractionResult{
		Transactions: []pipeline.Transaction{
			{Date: "2026-06-15", Merchant: "SOME KL CAFE", Amount: "8,700", ForeignAmount: "250", ForeignCurrency: "MYR"},
		},
	})
	tx := out.Transactions[0]
	if tx.Country != "MY" {
		t.Fatalf("Country = %q, want %q", tx.Country, "MY")
	}
	// Unknown to the dictionary → new fields empty except Country.
	if tx.MerchantNormalized != "" {
		t.Fatalf("unexpected MerchantNormalized on unknown merchant: %q", tx.MerchantNormalized)
	}
}

// The rules-only stage leaves fields blank when a merchant is not in
// the dictionary. Phase 3 layers an LLM fallback on top; Phase 2 must
// NOT invent values.
func TestStep_LeavesUnknownMerchantFieldsEmpty(t *testing.T) {
	out := runStep(t, pipeline.PageExtractionResult{
		Transactions: []pipeline.Transaction{
			{Date: "2026-06-16", Merchant: "SOMETHING NEVER SEEN", Amount: "500"},
		},
	})
	tx := out.Transactions[0]
	if tx.MerchantNormalized != "" {
		t.Fatalf("MerchantNormalized should be empty for unknown merchant, got %q", tx.MerchantNormalized)
	}
	if tx.Category != "" {
		t.Fatalf("Category should be empty for unknown merchant, got %q", tx.Category)
	}
	// Country still defaults to JP for JPY-only rows even without a
	// dictionary hit — that is the §4.6 rule and it is safe because the
	// alternative is leaving downstream data uncountry'd for the vast
	// majority of rows that ARE JP.
	if tx.Country != "JP" {
		t.Fatalf("Country = %q, want %q (JPY-only default)", tx.Country, "JP")
	}
}

// Dictionary entries that carry an explicit Country win over the
// currency-derived value. Rehan Restaurant is a JPY-settled Malaysian
// merchant on real statements, so Country must be MY even when the row
// has no ForeignCurrency.
func TestStep_DictionaryCountryWinsOverCurrencyDerivation(t *testing.T) {
	out := runStep(t, pipeline.PageExtractionResult{
		Transactions: []pipeline.Transaction{
			{Date: "2026-06-17", Merchant: "REHAN RESTAURANT", Amount: "1,200"},
		},
	})
	tx := out.Transactions[0]
	if tx.MerchantNormalized != "Rehan Restaurant" {
		t.Fatalf("MerchantNormalized = %q, want %q", tx.MerchantNormalized, "Rehan Restaurant")
	}
	if tx.Country != "MY" {
		t.Fatalf("Country = %q, want %q (dictionary override)", tx.Country, "MY")
	}
}

// If the upstream row already carries a category, the enrich stage
// must not overwrite it — upstream classification wins so a
// prompt-supplied category cannot be silently downgraded to the
// dictionary's default.
func TestStep_UpstreamCategoryWinsOverDictionary(t *testing.T) {
	out := runStep(t, pipeline.PageExtractionResult{
		Transactions: []pipeline.Transaction{
			{Date: "2026-06-18", Merchant: "STARBUCKS", Amount: "500", Category: "business_meal"},
		},
	})
	tx := out.Transactions[0]
	if tx.Category != "business_meal" {
		t.Fatalf("upstream category was overwritten: %q", tx.Category)
	}
	if tx.MerchantNormalized != "Starbucks" {
		t.Fatalf("MerchantNormalized = %q, want Starbucks", tx.MerchantNormalized)
	}
}

func TestStep_WritesResultUnderCanonicalKey(t *testing.T) {
	store, previousURI := newSeededStore(t, pipeline.PageExtractionResult{
		Transactions: []pipeline.Transaction{
			{Date: "2026-06-10", Merchant: "STARBUCKS", Amount: "480"},
		},
	})

	step := &transactions.Step{Objects: store}
	out, err := step.Run(context.Background(), pipeline.StepInput{
		JobID:       "job_xyz",
		Page:        7,
		PreviousURI: previousURI,
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	wantKey := "jobs/job_xyz/pages/000007/enrich_transactions/v1/result.json"
	if !endsWith(out.ResultURI, wantKey) {
		t.Fatalf("result_uri = %q, want suffix %q", out.ResultURI, wantKey)
	}
	if out.Stage != "enrich_transactions" || out.Version != "v1" {
		t.Fatalf("StepOutput stage/version wrong: %+v", out)
	}
}

func TestStep_RejectsEmptyPreviousURI(t *testing.T) {
	step := &transactions.Step{Objects: object.NewFileStore(t.TempDir())}
	_, err := step.Run(context.Background(), pipeline.StepInput{JobID: "job_x", Page: 1})
	if err == nil {
		t.Fatal("expected error for empty previous_uri")
	}
}

// LoadDictionary rejects a YAML that maps the same alias to two
// different canonical names — the alternative is silently letting file
// order decide which canonical wins, which is impossible to debug from
// a merchants.yaml diff.
func TestLoadDictionary_RejectsDuplicateAlias(t *testing.T) {
	body := []byte("" +
		"- canonical: A\n" +
		"  category: x\n" +
		"  aliases:\n" +
		"    - SAME\n" +
		"- canonical: B\n" +
		"  category: y\n" +
		"  aliases:\n" +
		"    - SAME\n",
	)
	if _, err := transactions.LoadDictionary(body); err == nil {
		t.Fatal("expected duplicate-alias error, got nil")
	}
}

func TestDefaultDictionary_ContainsSeedEntries(t *testing.T) {
	dict := transactions.DefaultDictionary()
	// Spot-check the four seed merchants from §4.4 — the seed defines
	// what the first end-to-end run against the June 2026 statement can
	// resolve, so drift here is worth catching in unit test rather than
	// at E2E.
	wants := []struct{ alias, canonical, category string }{
		{"ファミリーマート", "FamilyMart", "convenience_store"},
		{"STARBUCKS", "Starbucks", "cafe"},
		{"REHAN RESTAURANT", "Rehan Restaurant", "restaurant"},
		{"RESTORAN NASI KANDAR", "Restoran Nasi Kandar", "restaurant"},
	}
	for _, w := range wants {
		e := dict.Lookup(w.alias)
		if e == nil {
			t.Errorf("seed missing alias %q", w.alias)
			continue
		}
		if e.Canonical != w.canonical || e.Category != w.category {
			t.Errorf("alias %q → %+v, want canonical=%q category=%q",
				w.alias, e, w.canonical, w.category)
		}
	}
}

// runStep marshals in to a FileStore-backed PreviousURI, runs the
// step, and decodes the persisted output. Same shape as the
// cardstatement tests: lets each test assert on the enriched
// PageExtractionResult directly rather than the StepOutput wire form.
func runStep(t *testing.T, in pipeline.PageExtractionResult) pipeline.PageExtractionResult {
	t.Helper()
	store, previousURI := newSeededStore(t, in)
	step := &transactions.Step{Objects: store}
	out, err := step.Run(context.Background(), pipeline.StepInput{
		JobID:       "job_x",
		Page:        1,
		PreviousURI: previousURI,
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	body, err := store.Get(context.Background(), out.ResultURI)
	if err != nil {
		t.Fatalf("read result: %v", err)
	}
	var got pipeline.PageExtractionResult
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	return got
}

func newSeededStore(t *testing.T, in pipeline.PageExtractionResult) (object.Store, string) {
	t.Helper()
	store := object.NewFileStore(t.TempDir())
	body, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal seed: %v", err)
	}
	uri, err := store.PutBytes(context.Background(), "previous/normalize_card_statement/v1/result.json", body, "application/json")
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	return store, uri
}

func endsWith(s, suffix string) bool {
	return len(s) >= len(suffix) && s[len(s)-len(suffix):] == suffix
}
