package gemini

// Real ai.Step implementation backed by Gemini.
//
// Phase 6 fills this in with a Google AI Studio-backed client that
// reads page images and emits structured extraction in a single
// multimodal call. The Mock in mock.go satisfies ai.Step in the
// meantime so the rest of the pipeline (executor, queue, handler)
// can be exercised without external credentials.
