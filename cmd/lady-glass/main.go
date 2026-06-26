package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/keix/lady-glass/internal/api"
	"github.com/keix/lady-glass/internal/client"
	"github.com/keix/lady-glass/internal/executor"
	"github.com/keix/lady-glass/internal/object"
	"github.com/keix/lady-glass/internal/pipeline"
	"github.com/keix/lady-glass/internal/queue"
	"github.com/keix/lady-glass/internal/stage/ai/gemini"
	"github.com/keix/lady-glass/internal/stage/ai/lineocr"
	"github.com/keix/lady-glass/internal/store"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}

	ctx := context.Background()
	rest := os.Args[2:]

	switch os.Args[1] {
	case "dev":
		runDev(ctx)
	case "gemini":
		if len(rest) < 1 {
			usage()
		}
		runGemini(ctx, rest[0])
	case "submit":
		runSubmit(ctx, rest)
	case "status":
		runStatus(ctx, rest)
	case "result":
		runResult(ctx, rest)
	case "aggregate":
		runAggregate(ctx, rest)
	default:
		usage()
	}
}

func usage() {
	fmt.Println("usage:")
	fmt.Println("  lady-glass dev                              run the local mock chain")
	fmt.Println("  lady-glass gemini <file>                    smoke-test real Gemini against a local file")
	fmt.Println("  lady-glass submit <file> [--json]           upload + start a job")
	fmt.Println("  lady-glass status <job_id> [--json]         poll job status")
	fmt.Println("  lady-glass result <job_id>                  fetch merged extraction (JSON)")
	fmt.Println("  lady-glass aggregate <job_id> --merchant X [--json]   merchant rollup")
	fmt.Println()
	fmt.Println("Cloud commands read LADY_GLASS_API_URL and LADY_GLASS_API_TOKEN from .env or env.")
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

// --- Cloud commands --------------------------------------------------

// newAPIClient builds a client.Client from .env / process env. Used by
// every cloud subcommand. Exits with a clear message if either env var
// is missing.
func newAPIClient() *client.Client {
	if err := loadDotenv(".env"); err != nil {
		log.Fatalf("load .env: %v", err)
	}
	baseURL := firstNonEmpty(os.Getenv("LADY_GLASS_API_URL"))
	token := firstNonEmpty(os.Getenv("LADY_GLASS_API_TOKEN"))
	if baseURL == "" || token == "" {
		log.Fatal("LADY_GLASS_API_URL and LADY_GLASS_API_TOKEN must be set in .env or env")
	}
	return client.New(baseURL, token)
}

// runSubmit uploads a local file and starts the SFn workflow.
func runSubmit(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("submit", flag.ExitOnError)
	jsonOut := fs.Bool("json", false, "emit JSON output")
	_ = fs.Parse(args)
	if fs.NArg() < 1 {
		log.Fatal("usage: lady-glass submit <file> [--json]")
	}
	path := fs.Arg(0)

	c := newAPIClient()
	filename := filepath.Base(path)
	contentType := contentTypeForExt(filepath.Ext(filename))

	created, err := c.CreateJob(ctx, api.CreateJobRequest{
		Filename:    filename,
		ContentType: contentType,
	})
	if err != nil {
		log.Fatalf("create job: %v", err)
	}

	if err := c.UploadFile(ctx, created.UploadURL, path, contentType); err != nil {
		log.Fatalf("upload: %v", err)
	}

	started, err := c.StartJob(ctx, created.JobID)
	if err != nil {
		log.Fatalf("start job: %v", err)
	}

	if *jsonOut {
		printJSON(map[string]any{
			"job_id":        started.JobID,
			"execution_arn": started.ExecutionARN,
		})
		return
	}
	fmt.Printf("created job: %s\n", created.JobID)
	fmt.Printf("uploaded:    %s\n", filename)
	fmt.Printf("started:     %s\n", started.ExecutionARN)
}

// runStatus prints the current job status snapshot.
func runStatus(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	jsonOut := fs.Bool("json", false, "emit JSON output")
	_ = fs.Parse(args)
	if fs.NArg() < 1 {
		log.Fatal("usage: lady-glass status <job_id> [--json]")
	}
	jobID := fs.Arg(0)

	c := newAPIClient()
	out, err := c.GetJobStatus(ctx, jobID)
	if err != nil {
		log.Fatalf("status: %v", err)
	}

	if *jsonOut {
		printJSON(out)
		return
	}
	fmt.Printf("job:     %s\n", out.JobID)
	fmt.Printf("status:  %s\n", out.Status)
	fmt.Printf("pages:   %d (succeeded=%d failed=%d pending=%d)\n",
		out.PageCount, out.SucceededCount, out.FailedCount, out.PendingCount)
	if out.InputURI != "" {
		fmt.Printf("input:   %s\n", out.InputURI)
	}
	if out.ResultURI != "" {
		fmt.Printf("result:  %s\n", out.ResultURI)
	}
	if out.Error != "" {
		fmt.Printf("error:   %s\n", out.Error)
	}
	if out.UpdatedAt != "" {
		fmt.Printf("updated: %s\n", out.UpdatedAt)
	}
}

// runResult fetches the typed merged result and pretty-prints it.
func runResult(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("result", flag.ExitOnError)
	_ = fs.Parse(args)
	if fs.NArg() < 1 {
		log.Fatal("usage: lady-glass result <job_id>")
	}
	jobID := fs.Arg(0)

	c := newAPIClient()
	out, err := c.GetJobResult(ctx, jobID)
	if err != nil {
		if client.IsCode(err, api.ErrCodeJobNotReady) {
			fmt.Println("job is not ready yet — wait and retry")
			os.Exit(2)
		}
		log.Fatalf("result: %v", err)
	}
	printJSON(out)
}

// runAggregate filters transactions by merchant and prints the rollup.
func runAggregate(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("aggregate", flag.ExitOnError)
	merchant := fs.String("merchant", "", "exact-match merchant filter")
	jsonOut := fs.Bool("json", false, "emit JSON output")
	_ = fs.Parse(args)
	if fs.NArg() < 1 || *merchant == "" {
		log.Fatal("usage: lady-glass aggregate <job_id> --merchant <name> [--json]")
	}
	jobID := fs.Arg(0)

	c := newAPIClient()
	out, err := c.AggregateJob(ctx, jobID, api.AggregateRequest{Merchant: *merchant})
	if err != nil {
		var apiErr *client.Error
		if errors.As(err, &apiErr) && apiErr.Code == api.ErrCodeJobNotReady {
			fmt.Println("job is not ready yet — wait and retry")
			os.Exit(2)
		}
		log.Fatalf("aggregate: %v", err)
	}

	if *jsonOut {
		printJSON(out)
		return
	}
	fmt.Printf("merchant: %s\n", out.Merchant)
	fmt.Printf("count:    %d\n", out.Count)
	fmt.Printf("total:    %s 円\n", formatThousands(out.TotalJPY))
	if len(out.Transactions) > 0 {
		fmt.Println()
		for _, tx := range out.Transactions {
			fmt.Printf("  %s  p%d  %s  %s\n", tx.Date, tx.Page, tx.Merchant, tx.Amount)
		}
	}
}

// --- output helpers --------------------------------------------------

func printJSON(v any) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		log.Fatalf("encode: %v", err)
	}
	fmt.Print(buf.String())
}

// formatThousands inserts commas every three digits — "1234567" → "1,234,567".
func formatThousands(n int) string {
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}
	var b strings.Builder
	first := len(s) % 3
	if first > 0 {
		b.WriteString(s[:first])
		if len(s) > first {
			b.WriteByte(',')
		}
	}
	for i := first; i < len(s); i += 3 {
		b.WriteString(s[i : i+3])
		if i+3 < len(s) {
			b.WriteByte(',')
		}
	}
	return b.String()
}
