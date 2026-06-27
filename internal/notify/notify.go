// Package notify defines the post-commit observer contract (SPEC §S11).
// A Notifier reports a job's terminal state — succeeded or failed — to
// the outside world AFTER Lady Glass has already committed the
// transition. Notifier failures do not roll back the JobRecord; the
// SFN orchestrator retries the observer alone and, when retries are
// exhausted, ends the execution without disturbing the committed
// state.
//
// The contract has two endpoints, one per terminal state. Both are
// required: an external system that treats the succeeded notification
// as a proxy for "no longer processing" would otherwise leave failed
// jobs stuck in its own processing state. Providing both endpoints
// closes the terminal-state boundary completely.
package notify

import (
	"context"
	"time"
)

// JobSucceeded is the payload NotifySucceeded carries. The fields are
// the minimum an external system needs to act on the success: the
// job's identifier, the chain it was bound to (§S10), the merged
// result URI, the page count, and the wall-clock moment Merge
// committed.
type JobSucceeded struct {
	JobID       string
	ChainID     string
	ResultURI   string
	PageCount   int
	SucceededAt time.Time
}

// JobFailed is the payload NotifyFailed carries. Error is the message
// MarkJobFailed wrote onto the JobRecord; FailedAt is the JobRecord's
// last UpdatedAt at the moment MarkJobFailed committed.
type JobFailed struct {
	JobID     string
	ChainID   string
	PageCount int
	Error     string
	FailedAt  time.Time
}

// Notifier is the post-commit observer interface. Every conforming
// implementation MUST provide both endpoints — see SPEC §S11.
type Notifier interface {
	NotifySucceeded(ctx context.Context, job JobSucceeded) error
	NotifyFailed(ctx context.Context, job JobFailed) error
}

// NoOp is the silent default. It returns nil on every call —
// the boundary exists, but nothing is on the other side. Useful
// for deployments without an external subscriber and for tests
// that want the workflow to traverse NotifyCompletion without
// any side effect.
type NoOp struct{}

func (NoOp) NotifySucceeded(_ context.Context, _ JobSucceeded) error { return nil }
func (NoOp) NotifyFailed(_ context.Context, _ JobFailed) error       { return nil }
