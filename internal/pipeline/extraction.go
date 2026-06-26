package pipeline

// PageExtractionResult is the typed contract for what every Lady Glass
// extraction stage emits for one page. It is the schema the Gemini
// prompt (and any future provider) is bound to produce; the merged
// document embeds these verbatim (see workflow.MergedPage.Result).
//
// Two design choices that make this work across document types
// (receipts, credit-card statements, invoices, bills):
//
//   - The strict fields cover the line-item / transaction shape that
//     the aggregate command actually queries. These are the values the
//     prompt must enforce.
//
//   - Fields stays as a free-form bag for document-type-specific
//     headline values (statement total, due date, issuer name, account
//     number, etc.) that the aggregate command does not need to walk.
//
// Currency values are kept as strings ("1,012,127") instead of parsed
// numerics so the source representation is preserved losslessly; the
// API layer parses on demand for aggregate.
type PageExtractionResult struct {
	// Text is the full transcribed text of the page. Always present;
	// used for audit, search, and downstream stages that need raw OCR.
	Text string `json:"text"`

	// DocumentType is the model's best-guess classification, narrowed
	// to one of the labels listed in the prompt. An unknown document
	// MUST be reported as "other" rather than left empty so consumers
	// can branch reliably.
	DocumentType DocumentType `json:"document_type"`

	// Fields holds document-type-specific metadata as free-form
	// key/value pairs. Example keys for a credit-card statement:
	// "issuer", "statement_date", "payment_due_month",
	// "total_amount_due_jpy". For a receipt: "store_name",
	// "subtotal_jpy", "tax_jpy". The aggregate command does NOT walk
	// this map — anything aggregable goes into Transactions.
	Fields map[string]any `json:"fields,omitempty"`

	// Transactions is the line-item list. For a credit-card statement
	// every row in the usage table becomes one Transaction. For a
	// receipt every purchased item becomes one. For documents with
	// no line items (e.g. a one-page contract) this is empty / omitted.
	Transactions []Transaction `json:"transactions,omitempty"`
}

// DocumentType classifies the page so consumers can branch on it
// without re-reading Text. The set is intentionally small in v0; new
// values land here when a new document family ships through the
// pipeline.
type DocumentType string

const (
	DocumentTypeReceipt             DocumentType = "receipt"
	DocumentTypeCreditCardStatement DocumentType = "credit_card_statement"
	DocumentTypeInvoice             DocumentType = "invoice"
	DocumentTypeBill                DocumentType = "bill"
	DocumentTypeOther               DocumentType = "other"
)

// Transaction is the smallest aggregable unit. It maps to one row in
// a credit-card statement's usage table, or one purchased item on a
// receipt.
type Transaction struct {
	// Date is the transaction date as it appears in the source
	// document, with format preserved verbatim (e.g. "2026/06/22",
	// "26/06/22", "Jun 22, 2026"). Parsing into a normalised form is
	// a downstream concern.
	Date string `json:"date"`

	// Merchant is the vendor / store / payee name as printed.
	// Required: a row without a merchant is not a Transaction.
	Merchant string `json:"merchant"`

	// Description is optional further context (e.g. item name on a
	// receipt, "Coffee", or a memo on a card statement).
	Description string `json:"description,omitempty"`

	// Amount is the charge in the primary currency (Currency below).
	// Stored as the original printed string to preserve source
	// fidelity ("1,680", "100,000", "5,950"). The API parses to a
	// numeric on demand for aggregate.
	Amount string `json:"amount"`

	// Currency is the ISO 4217 code (or "JPY") for Amount above.
	// Defaults to "JPY" when absent.
	Currency string `json:"currency,omitempty"`

	// ForeignAmount / ForeignCurrency / ExchangeRate / ExchangeDate
	// describe the original transaction when it was settled in a
	// foreign currency and converted to the primary currency.
	// All four are populated together or none of them.
	ForeignAmount   string `json:"foreign_amount,omitempty"`
	ForeignCurrency string `json:"foreign_currency,omitempty"`
	ExchangeRate    string `json:"exchange_rate,omitempty"`
	ExchangeDate    string `json:"exchange_date,omitempty"`

	// Category is an optional tag the model MAY emit (e.g. "groceries",
	// "transport") or that a later normalisation stage attaches. Empty
	// in v0 since the prompt does not ask for it.
	Category string `json:"category,omitempty"`
}
