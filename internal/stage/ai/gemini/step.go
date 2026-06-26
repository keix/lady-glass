package gemini

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/keix/lady-glass/internal/object"
	"github.com/keix/lady-glass/internal/pipeline"
)

// DefaultPrompt instructs Gemini to read a document page image and
// return structured JSON. Step uses it when its Prompt field is empty.
const DefaultPrompt = `You are extracting structured data from a single document page image.
Return a JSON object with the following shape:
  {
    "text": "<the full transcribed text of the page>",
    "fields": { ... any identifiable key/value pairs ... },
    "tables": [ ... any tables present, as arrays of rows ... ]
  }
Respond with valid JSON only. Do not wrap it in markdown or explanatory prose.`

// Step is the production stage.Step backed by Gemini via Google AI Studio.
// It fetches the page image from Objects, asks Client for multimodal
// extraction, and persists the JSON result back to Objects.
type Step struct {
	Client  Client
	Objects object.Store

	// Prompt overrides the instructions sent to Gemini. Empty means use
	// DefaultPrompt.
	Prompt string
}

func (s *Step) Name() string    { return "gemini" }
func (s *Step) Version() string { return "v1" }

func (s *Step) Run(ctx context.Context, in pipeline.StepInput) (pipeline.StepOutput, error) {
	imageURI := in.InputURI
	if imageURI == "" {
		imageURI = in.PreviousURI
	}
	if imageURI == "" {
		return pipeline.StepOutput{}, fmt.Errorf("gemini: no input or previous URI on StepInput")
	}

	image, err := s.Objects.Get(ctx, imageURI)
	if err != nil {
		return pipeline.StepOutput{}, fmt.Errorf("gemini: fetch image %q: %w", imageURI, err)
	}

	prompt := s.Prompt
	if prompt == "" {
		prompt = DefaultPrompt
	}

	out, err := s.Client.Extract(ctx, ExtractInput{
		Image:     image,
		ImageMIME: mimeFromURI(imageURI),
		Prompt:    prompt,
	})
	if err != nil {
		return pipeline.StepOutput{}, fmt.Errorf("gemini: extract: %w", err)
	}

	key := fmt.Sprintf("jobs/%s/pages/%06d/gemini/v1/result.json", in.JobID, in.Page)
	resultURI, err := s.Objects.PutBytes(ctx, key, []byte(out.JSON), "application/json")
	if err != nil {
		return pipeline.StepOutput{}, fmt.Errorf("gemini: persist result: %w", err)
	}

	usage := &pipeline.Usage{
		Provider:     "google_ai_studio",
		Model:        out.Usage.Model,
		InputTokens:  out.Usage.InputTokens,
		OutputTokens: out.Usage.OutputTokens,
	}

	return pipeline.StepOutput{
		JobID:     in.JobID,
		Page:      in.Page,
		Stage:     s.Name(),
		Version:   s.Version(),
		ResultURI: resultURI,
		JSONURI:   resultURI,
		Usage:     usage,
	}, nil
}

// mimeFromURI picks a content type for the Gemini request body from the
// image URI's extension. Unknown extensions fall back to image/png.
func mimeFromURI(uri string) string {
	switch strings.ToLower(filepath.Ext(uri)) {
	case ".png":
		return "image/png"
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".webp":
		return "image/webp"
	case ".pdf":
		return "application/pdf"
	default:
		return "image/png"
	}
}
