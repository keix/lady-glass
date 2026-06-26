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

## S9. Retention

Lady Glass is a workflow plane, not a system of record. Every
StageRecord and JobRecord, and every per-page or merged artifact in
the object store, MUST be treated as transient execution state with
a bounded retention window.

A conforming implementation MUST:

- attach a per-item TTL attribute to every DynamoDB row it writes
  (`expires_at`, unix-epoch seconds), driven by a single retention
  window the operator can configure;
- filter rows whose `expires_at` is in the past at read time
  (`GetStage`, `GetJob`, `ListStagesByJob`), so the eventual-
  consistency of DynamoDB's own TTL reaper is not observable
  through the Store contract;
- expire object-store artifacts on the same window via the bucket's
  native lifecycle mechanism;
- keep the DynamoDB TTL window and the object-store lifecycle
  window in lockstep — the row and the artifact MUST become
  unreadable in the same operator-visible time bound.

The retention window MAY slide forward on any `PutItem` against the
row: an active job's rows are kept alive while it is still being
worked on, and the clock starts from the row's last transition
(typically `Merge` for success and `MarkJobFailed` for failure).

The retention contract is intentionally separate from idempotency
(S5). Once a row expires, the idempotency guarantee on its key
also lapses: a re-enqueue of the same `(jobID, page, stage,
version)` after expiry MUST be treated as a new execution.

A future revision MAY make the retention window negotiable per
job (e.g. a "preserve until" header on `POST /jobs`).

## S10. Chain binding

A job is bound to the chain it was born with.

At job creation, the operator's chain registry resolves a logical
chain identifier (e.g. `card-statement-v1`) to an ordered list of
`(stage_name, stage_version, queue_name)` tuples. This resolved
list MUST be persisted on the JobRecord at creation time and MUST
NOT be re-resolved on any subsequent read.

A conforming implementation MUST:

- expose a registry that maps each chain ID to an immutable
  `ChainSpec` (an ordered list of `StageSpec`s);
- write `chain_id` and the resolved `ChainSpec` onto the JobRecord
  in the same write that creates the job — partial population is
  not a legal intermediate state;
- derive `first_queue`, `final_stage`, and `final_version` (the
  values SubmitPages, CheckPages, and Merge consume from the SFN
  task input) from the JobRecord's frozen `ChainSpec`, NOT from
  any per-Lambda environment variable or the registry's current
  contents.

The contract operates on two layers:

- *Read layer* (API status / result / aggregate, SFN task input
  projection): the JobRecord's frozen chain MUST drive every read
  query against `ListStagesByJob`. Changing the registry, or even
  rotating through a deploy, MUST NOT change what an existing
  job's status / result calls return.
- *Compute layer* (per-page stage execution): the frozen
  `ChainSpec` MUST ride on every SQS message the workflow emits.
  SubmitPages writes the chain onto the page-0 StepInput; each
  consuming Lambda's Executor enqueues to `Chain[ChainIdx+1]`
  rather than to any per-Lambda environment variable. A stage at
  position N is terminal for this job iff N+1 == len(Chain). Read
  and compute layers therefore agree on routing: the same frozen
  list answers both "which stage record do reads consult" and
  "which queue does the next hop land in".

Implementations MAY retain an env-driven fallback (`NextStage`
configured at Lambda startup) for messages enqueued before the
on-message chain shipped — the fallback covers the SQS retention
window during a rolling deploy. Messages carrying a Chain MUST
take precedence over the fallback.

Re-running the registry against the same chain ID after a code
release that changed its definition is therefore safe for new
jobs and inert for existing ones: the existing jobs continue to
see their birth-time chain on every read AND on every compute
hop, and the new jobs pick up the new chain on the next createJob.

A future revision MAY make the chain ID a request parameter on
POST /jobs so a single deployment can host multiple chains
concurrently. The persistence contract above already supports
this — only the API surface needs to opt in.

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
| S9     | `internal/store/dynamodb.go` (`RetentionDays`, `expires_at` attribute, read-time filter), `infra/cdk/stack.go` (`TimeToLiveAttribute` + bucket `LifecycleRules`) |
| S10    | `internal/chain/` (Registry + Resolve), `internal/store/store.go` (`JobRecord.ChainID`, `JobRecord.Chain`), `internal/api/handler.go` (createJob freeze, startJob/status projection) |

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
