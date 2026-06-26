package lineocr

// The line_ocr stage is intentionally a Mock-only scaffold in v0.
//
// Multimodal Gemini in the next stage performs OCR alongside structured
// extraction in a single call, so a separate pre-processing OCR Step is
// not on the v0 critical path. lineocr.Mock therefore stays as the
// only implementation, kept in the chain to keep the multi-stage
// Executor wiring exercised end-to-end.
//
// If document-level OCR pre-processing becomes useful (e.g., a real
// Cloud Vision integration), implement a real stage.Step here and swap
// lineocr.Mock for it in cmd/line-ocr-lambda. No other code changes
// are needed — the Step interface is the only seam.
