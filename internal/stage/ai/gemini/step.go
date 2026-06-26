package gemini

import (
	"context"
	"embed"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/keix/lady-glass/internal/object"
	"github.com/keix/lady-glass/internal/pipeline"
)

// promptsFS embeds every prompt profile shipped with the Gemini stage.
// Files live next to step.go under prompts/<profile_id>.txt; resolvePrompt
// looks them up by id at runtime. The empty profile id ("") resolves to
// "default" so callers that do not set StepInput.PromptProfileID get a
// sensible baseline.
//
//go:embed prompts/*.txt
var promptsFS embed.FS

// DefaultPromptProfileID is the profile id used when StepInput's own
// PromptProfileID is empty.
const DefaultPromptProfileID = "default"

// resolvePrompt loads the prompt body for the given profile id from the
// embedded filesystem. Unknown profiles surface as a typed error so the
// Step's Run can wrap them.
func resolvePrompt(profileID string) (string, error) {
	if profileID == "" {
		profileID = DefaultPromptProfileID
	}
	body, err := promptsFS.ReadFile("prompts/" + profileID + ".txt")
	if err != nil {
		return "", fmt.Errorf("gemini: prompt profile %q not found: %w", profileID, err)
	}
	return string(body), nil
}

// Step is the production stage.Step backed by Gemini via Google AI Studio.
// It fetches the page image from Objects, asks Client for multimodal
// extraction, and persists the JSON result back to Objects.
type Step struct {
	Client  Client
	Objects object.Store

	// Prompt overrides the instructions sent to Gemini. When non-empty,
	// it wins over StepInput.PromptProfileID. Intended for tests and
	// one-off hardcoded uses; production callers leave it empty and let
	// the prompt come from the profile file.
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
		prompt, err = resolvePrompt(in.PromptProfileID)
		if err != nil {
			return pipeline.StepOutput{}, err
		}
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
