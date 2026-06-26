package gemini_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/keix/lady-glass/internal/stage/ai/gemini"
	"github.com/keix/lady-glass/internal/object"
	"github.com/keix/lady-glass/internal/pipeline"
)

type fakeClient struct {
	captured gemini.ExtractInput
	response gemini.ExtractOutput
	err      error
}

func (f *fakeClient) Extract(_ context.Context, in gemini.ExtractInput) (gemini.ExtractOutput, error) {
	f.captured = in
	if f.err != nil {
		return gemini.ExtractOutput{}, f.err
	}
	return f.response, nil
}

func TestStep_RunReadsImageCallsClientAndPersistsJSON(t *testing.T) {
	ctx := context.Background()
	store := object.NewFileStore(t.TempDir())

	imageURI, err := store.PutBytes(ctx, "jobs/j1/pages/000001/input.png", []byte("fakepngbytes"), "image/png")
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	fake := &fakeClient{response: gemini.ExtractOutput{
		JSON:  `{"text":"hello","fields":{"date":"2026-06-25"}}`,
		Usage: gemini.UsageInfo{Model: "gemini-2.5-flash", InputTokens: 100, OutputTokens: 50},
	}}

	step := &gemini.Step{Client: fake, Objects: store}

	out, err := step.Run(ctx, pipeline.StepInput{
		JobID:    "j1",
		Page:     1,
		InputURI: imageURI,
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	if string(fake.captured.Image) != "fakepngbytes" {
		t.Fatalf("captured image = %q", fake.captured.Image)
	}
	if fake.captured.ImageMIME != "image/png" {
		t.Fatalf("captured mime = %q, want image/png", fake.captured.ImageMIME)
	}
	if !strings.Contains(fake.captured.Prompt, "JSON") {
		t.Fatalf("prompt does not mention JSON: %q", fake.captured.Prompt)
	}

	body, err := store.Get(ctx, out.ResultURI)
	if err != nil {
		t.Fatalf("get result: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if decoded["text"] != "hello" {
		t.Fatalf("result text = %v", decoded["text"])
	}

	if out.Usage == nil {
		t.Fatal("Usage is nil")
	}
	if out.Usage.Provider != "google_ai_studio" {
		t.Fatalf("usage provider = %q", out.Usage.Provider)
	}
	if out.Usage.Model != "gemini-2.5-flash" {
		t.Fatalf("usage model = %q", out.Usage.Model)
	}
	if out.Usage.InputTokens != 100 || out.Usage.OutputTokens != 50 {
		t.Fatalf("usage tokens = (%d, %d)", out.Usage.InputTokens, out.Usage.OutputTokens)
	}
}

func TestStep_CustomPromptOverridesDefault(t *testing.T) {
	ctx := context.Background()
	store := object.NewFileStore(t.TempDir())
	imageURI, _ := store.PutBytes(ctx, "img.png", []byte("x"), "image/png")

	fake := &fakeClient{response: gemini.ExtractOutput{JSON: "{}"}}
	step := &gemini.Step{Client: fake, Objects: store, Prompt: "extract receipts only"}

	_, err := step.Run(ctx, pipeline.StepInput{JobID: "j", Page: 1, InputURI: imageURI})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if fake.captured.Prompt != "extract receipts only" {
		t.Fatalf("prompt = %q, want custom prompt", fake.captured.Prompt)
	}
}

func TestStep_PreviousURIIsUsedWhenInputURIEmpty(t *testing.T) {
	ctx := context.Background()
	store := object.NewFileStore(t.TempDir())
	imageURI, _ := store.PutBytes(ctx, "prev.png", []byte("fallback"), "image/png")

	fake := &fakeClient{response: gemini.ExtractOutput{JSON: `{"ok":true}`}}
	step := &gemini.Step{Client: fake, Objects: store}

	_, err := step.Run(ctx, pipeline.StepInput{JobID: "j", Page: 1, PreviousURI: imageURI})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if string(fake.captured.Image) != "fallback" {
		t.Fatalf("expected PreviousURI image to be used, got %q", fake.captured.Image)
	}
}

func TestStep_MissingImageURI_IsAnError(t *testing.T) {
	step := &gemini.Step{
		Client:  &fakeClient{},
		Objects: object.NewFileStore(t.TempDir()),
	}

	_, err := step.Run(context.Background(), pipeline.StepInput{JobID: "j", Page: 1})
	if err == nil {
		t.Fatal("expected error for missing image URI, got nil")
	}
}

func TestStep_ClientErrorIsWrapped(t *testing.T) {
	ctx := context.Background()
	store := object.NewFileStore(t.TempDir())
	imageURI, _ := store.PutBytes(ctx, "img.png", []byte("x"), "image/png")

	fake := &fakeClient{err: errors.New("simulated client failure")}
	step := &gemini.Step{Client: fake, Objects: store}

	_, err := step.Run(ctx, pipeline.StepInput{JobID: "j", Page: 1, InputURI: imageURI})
	if err == nil {
		t.Fatal("expected error from client, got nil")
	}
	if !strings.Contains(err.Error(), "simulated client failure") {
		t.Fatalf("error %q does not include underlying error", err)
	}
}
