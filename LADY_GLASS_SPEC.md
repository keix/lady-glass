# Lady Glass — Stage Specification

Version: v1  
Status: **draft (not yet ratified)**

This document defines the **stage contract** of Lady Glass. Any
implementation that violates these clauses is not a Lady Glass stage.
It holds only what cannot change without changing what Lady Glass
*is*; a clause changing implies a new spec version.

The keywords MUST, MUST NOT, SHOULD, SHOULD NOT, and MAY in this
document are to be interpreted as described in RFC 2119.

## S1. Shape

A stage MUST consist of:

- exactly one inbound SQS queue,
- exactly one Lambda function bound to that queue via an AWS Lambda
  Event Source Mapping with batch size 1,
- the Executor (`internal/executor`) holding exactly one Step
  implementation,
- optionally a NextStage descriptor pointing at the next stage's
  queue name and `(Name, Version)`.

A stage SHOULD set Lambda reserved concurrency to bound the 
underlying provider's rate.

## S2. Identity

A stage is identified by the tuple `(Name, Version)`. The same Name
with a different Version IS a different stage. Stage Versions MUST
be monotonically advanced by the operator when the Step's externally
observable behaviour changes.

## S3. Idempotency key

For every `(jobID, page)` and every stage, the tuple

```
jobID + page + Name + Version
```

is the stage's *idempotency key*. The formatted form is defined by
`pipeline.StageKey` and MUST NOT vary across implementations.

`page = 0` collapses the key to the job-level form for stages that
do not operate per page (e.g. Merge).

## S4. State transitions

A StageRecord's `Status` MUST be one of `queued | running |
succeeded | failed`. The allowed transitions are:

```
(no record) → running     via MarkRunning
running     → succeeded   via MarkSucceeded
running     → failed      via MarkFailed
failed      → running     via MarkRunning (retry)
running     → running     via MarkRunning (retry of an interrupted run)
succeeded   → succeeded   (idempotent re-enqueue path; Step.Run NOT called)
```

`succeeded` is the ONLY terminal short-circuit. `running` and
`failed` both fall through to a new `Step.Run` on the next delivery
of the same stage key.

A future revision MAY add an `attempts` counter or a lease TTL on
`running` to detect stuck executions; until then, the upstream
backstop is SQS `MaxReceiveCount` plus a DLQ.

## S5. Skip semantics

A stage MUST skip `Step.Run` if and only if the StageRecord for its
OWN idempotency key has `Status=succeeded`.

The skip is stage-local and message-local:

- A stage MUST NOT consult upstream or downstream stage records to
  decide whether to skip.
- A stage MUST NOT skip work intended for a different stage.

The skip absorbs SQS redelivery of the same message, Lambda
re-invocation after a non-fatal crash, and Step Functions retries
that re-trigger SubmitPages.

## S6. Next-stage enqueue

If `NextStage` is set, every `Execute` that reaches the end of its
branch — whether after a successful `Step.Run` or via the
succeeded-skip path — MUST attempt to enqueue a
`pipeline.StepInput` to the NextStage queue.

The enqueued StepInput MUST include `JobID`, `Page`, the next
stage's `Name` and `Version`, the original `InputURI`, and the
previous stage's result as `PreviousURI`.

If the enqueue fails, `Execute` MUST return error so that the
inbound SQS message is not acked. The redelivery sees
`Status=succeeded` (per S5), skips Step.Run, and retries only the
enqueue.

## S7. Composition

The stage contract MUST NOT change with chain depth. Adding stage
`N+1` to a chain MUST cost only:

1. one SQS queue,
2. one Lambda running the same Executor with a different Step and
   NextStage,
3. one entry appended to the ChainSpec.

No existing stage's code may need to change. No code branch may
inspect chain length. Each stage MUST remain independently
idempotent against its own key.

## S8. Per-stage isolation

Each stage's SQS queue, Lambda function, and StageRecord rows MUST
be independently addressable. No code path may aggregate across
stages except:

- CheckPages, which is read-only and reads the *final* stage's
  records only;
- Merge, which reads each succeeded stage record's `ResultURI` to
  assemble the merged document.

Per-provider rate limits MUST be enforced inside the stage's own
Lambda (via reserved concurrency or an in-Step rate limiter),
never across stages.

---

## Conformance map (informative)

The current implementation enforces this spec at the following
points. References here are informative; the spec clauses are
normative.

| Clause | Where enforced |
|--------|----------------|
| S1     | `infra/cdk/stack.go` (queue + Lambda + ESM bindings) |
| S2     | `internal/stage/ai/*/*.Name()`, `*.Version()` on each Step |
| S3     | `internal/pipeline/idempotency.go` (`StageKey`) |
| S4     | `internal/store/store.go` (Store contract doc) + `internal/store/{memory,dynamodb}.go` |
| S5     | `internal/executor/executor.go` (`succeeded` short-circuit) |
| S6     | `internal/executor/executor.go` (`enqueueNext`) |
| S7     | `internal/stage/step.go` (Step interface) + `internal/pipeline/types.go` (ChainSpec) |
| S8     | `infra/cdk/stack.go` (one queue + one Lambda per stage), `internal/workflow/{check_pages,merge}.go` |

---

## Versioning

The first published version of this spec is v1. A new spec version
is required when:

- a clause is added, removed, or relaxed;
- a state transition in S4 is added or removed;
- the skip rule in S5 is generalised (e.g. cross-stage skip);
- the StageKey shape in S3 changes its format string;
- the StepInput contract in S6 gains required fields.

Bumping Step versions (per stage, per S2) does NOT bump the spec
version; only changes to the spec's clauses do.
