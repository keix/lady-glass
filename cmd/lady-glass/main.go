package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/keix/lady-glass/internal/ai/gemini"
	"github.com/keix/lady-glass/internal/ai/lineocr"
	"github.com/keix/lady-glass/internal/executor"
	"github.com/keix/lady-glass/internal/object"
	"github.com/keix/lady-glass/internal/pipeline"
	"github.com/keix/lady-glass/internal/queue"
	"github.com/keix/lady-glass/internal/store"
)

func main() {
	if len(os.Args) < 2 || os.Args[1] != "dev" {
		fmt.Println("usage: lady-glass dev")
		os.Exit(1)
	}

	ctx := context.Background()

	objects := object.NewFileStore("./out")
	st := store.NewMemoryStore()
	q := queue.NewMemoryQueue()

	lineCalls := 0
	geminiCalls := 0

	lineStep := &lineocr.Mock{
		Objects: objects,
		Calls:   &lineCalls,
	}

	geminiStep := &gemini.Mock{
		Objects: objects,
		Calls:   &geminiCalls,
	}

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

	geminiExecutor := &executor.Executor{
		Step:  geminiStep,
		Store: st,
		Queue: q,
	}

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
