package cardstatement_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/keix/lady-glass/internal/object"
	"github.com/keix/lady-glass/internal/pipeline"
	"github.com/keix/lady-glass/internal/stage/normalize/cardstatement"
)

func TestStep_DropsPhantomScheduleRowsMatchingIssuerName(t *testing.T) {
	// PayPay page 3 in the real-world job_1782486811_375c7b fixture:
	// the お支払い予定表 future-payment table is misclassified as a
	// transactions list; every row's merchant equals the page's
	// issuer_name. v1 normaliser drops all six.
	in := pipeline.PageExtractionResult{
		DocumentType: pipeline.DocumentTypeCreditCardStatement,
		Fields: map[string]any{
			"issuer_name": "PayPay Card",
		},
		Transactions: []pipeline.Transaction{
			{Date: "2026/4", Merchant: "PayPay Card", Amount: "4,180"},
			{Date: "2026/5", Merchant: "PayPay Card", Amount: "0"},
			{Date: "2026/6", Merchant: "PayPay Card", Amount: "0"},
			{Date: "2026/7", Merchant: "PayPay Card", Amount: "0"},
			{Date: "2026/8", Merchant: "PayPay Card", Amount: "0"},
			{Date: "2026/9", Merchant: "PayPay Card", Amount: "0"},
		},
	}

	out := runStep(t, in)
	if len(out.Transactions) != 0 {
		t.Fatalf("phantom rows survived: %+v", out.Transactions)
	}
	// Fields and document type are forwarded verbatim — the stage
	// only narrows transactions, never strips metadata.
	if out.Fields["issuer_name"] != "PayPay Card" {
		t.Fatalf("issuer_name lost: %v", out.Fields)
	}
	if out.DocumentType != pipeline.DocumentTypeCreditCardStatement {
		t.Fatalf("document_type lost: %q", out.DocumentType)
	}
}

func TestStep_KeepsLegitimateMerchantRows(t *testing.T) {
	// PayPay page 1 from the same fixture: real purchases at real
	// merchants. None of them share the issuer name. All three must
	// pass through.
	in := pipeline.PageExtractionResult{
		DocumentType: pipeline.DocumentTypeCreditCardStatement,
		Fields: map[string]any{
			"issuer_name": "PayPayカード株式会社",
		},
		Transactions: []pipeline.Transaction{
			{Date: "2026/3/1", Merchant: "御影クラッセ", Amount: "1,620"},
			{Date: "2026/3/1", Merchant: "麺蔵", Amount: "1,180"},
			{Date: "2026/3/8", Merchant: "麺蔵", Amount: "1,380"},
		},
	}

	out := runStep(t, in)
	if len(out.Transactions) != 3 {
		t.Fatalf("dropped legitimate rows: %+v", out.Transactions)
	}
}

func TestStep_DropsZeroAmountRowsEvenWithoutIssuerMatch(t *testing.T) {
	// Future-schedule rows that do NOT match the issuer name still get
	// dropped on the amount=0 rule. Common shapes covered: "0",
	// "0円", "" (omitted), "0.00".
	in := pipeline.PageExtractionResult{
		Fields: map[string]any{"issuer_name": "SomeBank"},
		Transactions: []pipeline.Transaction{
			{Date: "2026/4", Merchant: "Future Plan A", Amount: "0"},
			{Date: "2026/5", Merchant: "Future Plan B", Amount: "0円"},
			{Date: "2026/6", Merchant: "Future Plan C", Amount: ""},
			{Date: "2026/7", Merchant: "Future Plan D", Amount: "0.00"},
			{Date: "2026/8", Merchant: "Real Purchase", Amount: "1,234"},
		},
	}

	out := runStep(t, in)
	if len(out.Transactions) != 1 || out.Transactions[0].Merchant != "Real Purchase" {
		t.Fatalf("expected only the 1,234 row to survive, got %+v", out.Transactions)
	}
}

func TestStep_MissingIssuerFieldOnlyAppliesAmountRule(t *testing.T) {
	// Without an issuer_name to compare against, the phantom rule is
	// inert. The amount rule still fires so a misclassified zero row
	// is dropped.
	in := pipeline.PageExtractionResult{
		Fields: map[string]any{},
		Transactions: []pipeline.Transaction{
			{Date: "2026/3/1", Merchant: "御影クラッセ", Amount: "1,620"},
			{Date: "2026/3/1", Merchant: "phantom-but-no-issuer", Amount: "0"},
		},
	}

	out := runStep(t, in)
	if len(out.Transactions) != 1 || out.Transactions[0].Merchant != "御影クラッセ" {
		t.Fatalf("expected to keep only the non-zero row, got %+v", out.Transactions)
	}
}

func TestStep_IssuerComparisonHandlesKabushikiKaisha(t *testing.T) {
	// "PayPay" (merchant) vs "PayPay株式会社" (fields.issuer_name)
	// should still drop — operators commonly print issuer with or
	// without the corporate suffix on the same page.
	in := pipeline.PageExtractionResult{
		Fields: map[string]any{"issuer_name": "PayPay株式会社"},
		Transactions: []pipeline.Transaction{
			{Date: "2026/4", Merchant: "PayPay", Amount: "4,180"},
			{Date: "2026/4", Merchant: "御影クラッセ", Amount: "1,620"},
		},
	}

	out := runStep(t, in)
	if len(out.Transactions) != 1 || out.Transactions[0].Merchant != "御影クラッセ" {
		t.Fatalf("kabushiki kaisha stripping failed: %+v", out.Transactions)
	}
}

func TestStep_WritesResultUnderCanonicalKey(t *testing.T) {
	store, previousURI := newSeededStore(t, pipeline.PageExtractionResult{
		Transactions: []pipeline.Transaction{
			{Date: "2026/3/1", Merchant: "御影クラッセ", Amount: "1,620"},
		},
	})

	step := &cardstatement.Step{Objects: store}
	out, err := step.Run(context.Background(), pipeline.StepInput{
		JobID:       "job_xyz",
		Page:        7,
		PreviousURI: previousURI,
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	wantKey := "jobs/job_xyz/pages/000007/normalize_card_statement/v1/result.json"
	// FileStore returns file:// URIs; assert the path suffix matches
	// the SPEC-required per-stage layout.
	if !endsWith(out.ResultURI, wantKey) {
		t.Fatalf("result_uri = %q, want suffix %q", out.ResultURI, wantKey)
	}
	if out.Stage != "normalize_card_statement" || out.Version != "v1" {
		t.Fatalf("StepOutput stage/version wrong: %+v", out)
	}
}

func TestStep_RejectsEmptyPreviousURI(t *testing.T) {
	step := &cardstatement.Step{Objects: object.NewFileStore(t.TempDir())}
	_, err := step.Run(context.Background(), pipeline.StepInput{
		JobID: "job_x",
		Page:  1,
	})
	if err == nil {
		t.Fatal("expected error for empty previous_uri")
	}
}

// runStep marshals in to a FileStore-backed PreviousURI, runs the
// step, and decodes the persisted output. Lets each test assert on
// the normalised PageExtractionResult directly rather than navigating
// the StepOutput wire shape.
func runStep(t *testing.T, in pipeline.PageExtractionResult) pipeline.PageExtractionResult {
	t.Helper()
	store, previousURI := newSeededStore(t, in)
	step := &cardstatement.Step{Objects: store}
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
	uri, err := store.PutBytes(context.Background(), "previous/gemini/v1/result.json", body, "application/json")
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	return store, uri
}

func endsWith(s, suffix string) bool {
	return len(s) >= len(suffix) && s[len(s)-len(suffix):] == suffix
}
