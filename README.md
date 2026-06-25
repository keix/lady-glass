# Lady Glass
Lady Glass is a cloud OCR pipeline written in Go.

## Why Lady Glass
I met a Hong Kong woman in Kuala Lumpur who wore distinctive glasses.

After spending more than I should have, I later found myself reading PDFs, receipts, and card statements more carefully than usual.

At some point, I realized this was a job for AI, not for me.

Lady Glass is a pair of glasses for documents — her name was Miu.

## Architecture
Lady Glass uses Step Functions for document-level orchestration and SQS + Lambda for page-level AI execution. DynamoDB is the control plane. S3 is the data plane.

```mermaid
flowchart TB
    User([API / CLI]) --> StartExec[StartExecution]

    subgraph SFN["Step Functions"]
        direction TB
        StartExec --> RenderPages
        RenderPages --> SubmitPages
        SubmitPages --> WaitLoop[Wait]
        WaitLoop --> CheckPages
        CheckPages --> Choice{job status?}
        Choice -- pending --> WaitLoop
        Choice -- failed --> MarkFailed[MarkJobFailed]
        Choice -- succeeded --> Merge
        Merge --> MarkSucceeded[MarkJobSucceeded]
    end

    subgraph CHAIN["SQS + Lambda"]
        direction LR
        Q1[(stage-1-queue)] --> L1[stage-1 Lambda]
        L1 -- enqueue next stage --> Q2[(stage-2-queue)]
        Q2 --> L2[stage-2 Lambda]
    end

    subgraph DATA["Data plane"]
        direction LR
        S3[(S3 — images, stage results, merged output)]
        DDB[(DynamoDB — stage state, idempotency, events)]
    end

    SubmitPages -. one message per page .-> Q1
    CheckPages -. read status .-> DDB
    Merge -. read stage state .-> DDB
    Merge -. read result objects .-> S3
    Merge -. write merged result .-> S3

    L1 --- S3
    L1 --- DDB
    L2 --- S3
    L2 --- DDB
```

Step Functions owns the document workflow. SQS and Lambda own the per-page AI stage chain. They meet at DynamoDB, the control plane, and S3, the data plane.

| Layer          | Owns                                                             |
| -------------- | ---------------------------------------------------------------- |
| Step Functions | Per-document workflow: start, render, submit, wait, check, merge |
| SQS + Lambda   | Per-page AI stage chain: one queue + one Lambda per stage        |
| DynamoDB       | Stage state, idempotency keys, events — the control plane        |
| S3             | Page images, stage results, merged output — the data plane       |

### Every AI operation in Lady Glass is a stage
A stage is intentionally small: it receives one input, writes one output, and may enqueue the next stage.

### Why split this way
* **AI providers have different bottlenecks.** Each stage owns its own queue, so each Lambda sets its own reserved concurrency — a low-throughput provider cannot starve a high-throughput one.
* **Idempotency belongs at the stage level.** `job_id + page + stage + version` is the key. A redelivered SQS message, a Lambda retry, or a Step Functions re-execution all collapse to the same "succeeded → skip" path in DynamoDB.
* **Step Functions does not chain AI steps.** Page-level retry and ack stay inside SQS so workflow state transitions don't multiply with page count, and so external API limits don't leak into the workflow.
* **CheckPages is read-only.** It polls DynamoDB and either keeps waiting, merges, or fails the job. No work happens inside the workflow itself beyond orchestration.

## Core Concepts
Lady Glass is built around small, retry-safe stages.

A stage receives one page, performs one piece of work, stores its result, and optionally enqueues the next stage.

Examples of stages:

```text
tesseract_ocr
gemini_ocr_extract
gemini_extract
line_ocr
```

Each stage is identified by:

```text
job_id + page + stage + version
```

For example:

```text
job_123:page:000017:gemini_extract:v1
```

This key is used for idempotency. If the same message is delivered again after the stage has already succeeded, Lady Glass skips the external API call and continues from the stored result.

Stage outputs are stored as artifacts. DynamoDB stores only the state and pointers to those artifacts.

```text
DynamoDB:
  status
  idempotency key
  stage name
  version
  result URI
  timestamps

S3:
  page images
  OCR text
  AI responses
  structured JSON
  merged output
```

In local development, the same model is used with a file-based object store and an in-memory state store.

## Local Development
Lady Glass can run locally without AWS.

```bash
nix develop
go run ./cmd/lady-glass dev
```

The local runner uses mock AI stages and writes artifacts to `out/`.

## Current Status
Lady Glass is currently a work in progress.

The local mock pipeline is implemented. Cloud adapters and real AI stages will be added incrementally.

## License
Lady Glass is licensed under the MIT License.
