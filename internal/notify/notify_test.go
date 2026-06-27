package notify_test

import (
	"context"
	"testing"

	"github.com/keix/lady-glass/internal/notify"
)

func TestNoOp_SucceededReturnsNil(t *testing.T) {
	if err := (notify.NoOp{}).NotifySucceeded(context.Background(), notify.JobSucceeded{
		JobID: "j",
	}); err != nil {
		t.Fatalf("NoOp.NotifySucceeded returned %v, want nil", err)
	}
}

func TestNoOp_FailedReturnsNil(t *testing.T) {
	if err := (notify.NoOp{}).NotifyFailed(context.Background(), notify.JobFailed{
		JobID: "j",
		Error: "anything",
	}); err != nil {
		t.Fatalf("NoOp.NotifyFailed returned %v, want nil", err)
	}
}
