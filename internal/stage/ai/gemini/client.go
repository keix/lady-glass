package gemini

import (
	"context"

	"google.golang.org/genai"
)

// Client is the narrow extraction surface gemini.Step depends on. Tests
// substitute a fake; SDKClient below is the real wrapper backed by the
// google.golang.org/genai SDK.
type Client interface {
	Extract(ctx context.Context, in ExtractInput) (ExtractOutput, error)
}

type ExtractInput struct {
	Image     []byte
	ImageMIME string
	Prompt    string
}

type ExtractOutput struct {
	JSON  string
	Usage UsageInfo
}

type UsageInfo struct {
	Model        string
	InputTokens  int
	OutputTokens int
}

// SDKClient is a Client backed by google.golang.org/genai configured for
// the Gemini API (Google AI Studio) backend.
type SDKClient struct {
	Client *genai.Client
	Model  string
}

// NewSDKClient constructs an SDKClient authenticated by apiKey against the
// public Gemini API. model is the Gemini model name passed to every
// GenerateContent call (e.g. "gemini-2.5-flash").
func NewSDKClient(ctx context.Context, apiKey, model string) (*SDKClient, error) {
	client, err := genai.NewClient(ctx, &genai.ClientConfig{
		APIKey:  apiKey,
		Backend: genai.BackendGeminiAPI,
	})
	if err != nil {
		return nil, err
	}
	return &SDKClient{Client: client, Model: model}, nil
}

func (c *SDKClient) Extract(ctx context.Context, in ExtractInput) (ExtractOutput, error) {
	contents := []*genai.Content{{
		Role: "user",
		Parts: []*genai.Part{
			{Text: in.Prompt},
			{InlineData: &genai.Blob{Data: in.Image, MIMEType: in.ImageMIME}},
		},
	}}
	cfg := &genai.GenerateContentConfig{
		ResponseMIMEType: "application/json",
	}

	resp, err := c.Client.Models.GenerateContent(ctx, c.Model, contents, cfg)
	if err != nil {
		return ExtractOutput{}, err
	}

	usage := UsageInfo{Model: c.Model}
	if resp.UsageMetadata != nil {
		usage.InputTokens = int(resp.UsageMetadata.PromptTokenCount)
		usage.OutputTokens = int(resp.UsageMetadata.CandidatesTokenCount)
	}

	return ExtractOutput{
		JSON:  resp.Text(),
		Usage: usage,
	}, nil
}
