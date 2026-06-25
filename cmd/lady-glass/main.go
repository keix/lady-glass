package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/keix/lady-glass/internal/ai/gemini"
	"github.com/keix/lady-glass/internal/ai/lineocr"
	"github.com/keix/lady-glass/internal/executor"
	"github.com/keix/lady-glass/internal/object"
	"github.com/keix/lady-glass/internal/pipeline"
	"github.com/keix/lady-glass/internal/queue"
	"github.com/keix/lady-glass/internal/store"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}

	ctx := context.Background()

	switch os.Args[1] {
	case "dev":
		runDev(ctx)
	case "gemini":
		if len(os.Args) < 3 {
			usage()
		}
		runGemini(ctx, os.Args[2])
	default:
		usage()
	}
}

func usage() {
	fmt.Println("usage:")
	fmt.Println("  lady-glass dev               run the mock line_ocr → gemini chain locally")
	fmt.Println("  lady-glass gemini <file>     smoke-test the real Gemini Step against a local image or PDF")
	os.Exit(1)
}

// runDev wires the local mock chain end-to-end with in-memory store/queue
// and FileStore objects. No AWS, no API keys — proves the pipeline plumbing.
func runDev(ctx context.Context) {
	objects := object.NewFileStore("./out")
	st := store.NewMemoryStore()
	q := queue.NewMemoryQueue()

	lineCalls := 0
	geminiCalls := 0

	lineStep := &lineocr.Mock{Objects: objects, Calls: &lineCalls}
	geminiStep := &gemini.Mock{Objects: objects, Calls: &geminiCalls}

	lineExecutor := &executor.Executor{
		Step: lineStep,
		NextStage: &pipeline.StageSpec{
			Name:      "gemini",
			Version:   "v1",
			QueueName: "gemini",
		},
		Store: st,
		Queue: q,
	}

	geminiExecutor := &executor.Executor{Step: geminiStep, Store: st, Queue: q}

	input := pipeline.StepInput{
		JobID:    "job_local_001",
		Page:     1,
		InputURI: "file://testdata/page-1.png",
	}

	if err := lineExecutor.Execute(ctx, input); err != nil {
		log.Fatal(err)
	}

	msg, ok := q.Pop("gemini")
	if !ok {
		log.Fatal("gemini message was not enqueued")
	}

	if err := geminiExecutor.Execute(ctx, msg); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Lady Glass dev chain completed")
	fmt.Printf("line_ocr calls: %d\n", lineCalls)
	fmt.Printf("gemini calls:   %d\n", geminiCalls)
	fmt.Println("output: ./out")
}

// runGemini runs the real gemini.Step against a single local file. It
// stages the file into ./out via FileStore so the Step's normal URI-based
// flow works unchanged. The API key is loaded from .env (if present) or
// from process env.
func runGemini(ctx context.Context, filePath string) {
	if err := loadDotenv(".env"); err != nil {
		log.Fatalf("load .env: %v", err)
	}

	apiKey := firstNonEmpty(
		os.Getenv("LADY_GLASS_GEMINI_API_KEY"),
		os.Getenv("GEMINI_API_KEY"),
		os.Getenv("GOOGLE_API_KEY"),
	)
	if apiKey == "" {
		log.Fatal("no Gemini API key found; set LADY_GLASS_GEMINI_API_KEY (or GEMINI_API_KEY) in .env or env")
	}

	model := firstNonEmpty(
		os.Getenv("LADY_GLASS_GEMINI_MODEL"),
		os.Getenv("GEMINI_MODEL"),
		"gemini-2.5-flash",
	)

	absPath, err := filepath.Abs(filePath)
	if err != nil {
		log.Fatalf("resolve path: %v", err)
	}
	data, err := os.ReadFile(absPath)
	if err != nil {
		log.Fatalf("read file: %v", err)
	}

	ext := filepath.Ext(absPath)
	if ext == "" {
		log.Fatalf("file %q has no extension; cannot infer MIME type", absPath)
	}

	objects := object.NewFileStore("./out")
	inputKey := fmt.Sprintf("jobs/smoke/pages/000001/input%s", ext)
	inputURI, err := objects.PutBytes(ctx, inputKey, data, contentTypeForExt(ext))
	if err != nil {
		log.Fatalf("stage input: %v", err)
	}

	sdkClient, err := gemini.NewSDKClient(ctx, apiKey, model)
	if err != nil {
		log.Fatalf("init gemini client: %v", err)
	}
	step := &gemini.Step{Client: sdkClient, Objects: objects}

	fmt.Printf("model:    %s\n", model)
	fmt.Printf("file:     %s (%d bytes)\n", absPath, len(data))
	fmt.Printf("running...\n\n")

	out, err := step.Run(ctx, pipeline.StepInput{
		JobID:    "smoke",
		Page:     1,
		InputURI: inputURI,
	})
	if err != nil {
		log.Fatalf("step run: %v", err)
	}

	fmt.Printf("result_uri: %s\n", out.ResultURI)
	if out.Usage != nil {
		fmt.Printf("tokens:     in=%d out=%d\n", out.Usage.InputTokens, out.Usage.OutputTokens)
	}
	fmt.Println()

	body, err := objects.Get(ctx, out.ResultURI)
	if err != nil {
		log.Fatalf("read result: %v", err)
	}

	var pretty bytes.Buffer
	if err := json.Indent(&pretty, body, "", "  "); err == nil {
		fmt.Println(pretty.String())
	} else {
		fmt.Println(string(body))
	}
}

// loadDotenv reads KEY=VALUE pairs from path (if it exists) into os.Environ,
// without overwriting variables already set in the process environment.
// Supports quoted values ("..." and '...') and # comments.
func loadDotenv(path string) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		eq := strings.IndexByte(line, '=')
		if eq < 0 {
			continue
		}
		key := strings.TrimSpace(line[:eq])
		val := strings.TrimSpace(line[eq+1:])
		if len(val) >= 2 {
			if (val[0] == '"' && val[len(val)-1] == '"') || (val[0] == '\'' && val[len(val)-1] == '\'') {
				val = val[1 : len(val)-1]
			}
		}
		if os.Getenv(key) == "" {
			os.Setenv(key, val)
		}
	}
	return sc.Err()
}

func firstNonEmpty(vs ...string) string {
	for _, v := range vs {
		if v != "" {
			return v
		}
	}
	return ""
}

// contentTypeForExt picks a content type when staging the smoke-test input
// into FileStore. gemini.Step's own mimeFromURI handles the read-side
// detection from the resulting URI; this just makes the staged file
// self-describing.
func contentTypeForExt(ext string) string {
	switch strings.ToLower(ext) {
	case ".png":
		return "image/png"
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".webp":
		return "image/webp"
	case ".pdf":
		return "application/pdf"
	default:
		return "application/octet-stream"
	}
}
