package chain_test

import (
	"testing"

	"github.com/keix/lady-glass/internal/chain"
)

func TestResolve_ReturnsDefaultChainWhenIDEmpty(t *testing.T) {
	spec, err := chain.Resolve("")
	if err != nil {
		t.Fatalf("resolve empty: %v", err)
	}
	if spec.ID != chain.DefaultChainID {
		t.Fatalf("spec.ID = %q, want %q", spec.ID, chain.DefaultChainID)
	}
	if len(spec.Stages) == 0 {
		t.Fatal("default chain has zero stages")
	}
}

func TestResolve_ReturnsCardStatementV1AsTheShippedDefault(t *testing.T) {
	spec, err := chain.Resolve("card-statement-v1")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	// Shape pin: the chain has exactly three stages and the queue names
	// are the logical names the CDK wires the SQS URLs to. Changing
	// either side without updating the other breaks new jobs at
	// SubmitPages.
	if len(spec.Stages) != 3 {
		t.Fatalf("stages = %d, want 3: %+v", len(spec.Stages), spec.Stages)
	}
	if spec.Stages[0].Name != "gemini" || spec.Stages[0].Version != "v1" {
		t.Fatalf("stage[0] = %+v, want gemini/v1", spec.Stages[0])
	}
	if spec.Stages[1].Name != "normalize_card_statement" || spec.Stages[1].Version != "v1" {
		t.Fatalf("stage[1] = %+v, want normalize_card_statement/v1", spec.Stages[1])
	}
	if spec.Stages[2].Name != "enrich_transactions" || spec.Stages[2].Version != "v1" {
		t.Fatalf("stage[2] = %+v, want enrich_transactions/v1", spec.Stages[2])
	}
	if spec.Stages[2].QueueName != "enrich_transactions" {
		t.Fatalf("stage[2].QueueName = %q, want %q", spec.Stages[2].QueueName, "enrich_transactions")
	}
}

func TestResolve_RejectsUnknownChainID(t *testing.T) {
	if _, err := chain.Resolve("does-not-exist"); err == nil {
		t.Fatal("expected error for unknown chain id, got nil")
	}
}

func TestResolve_ReturnsADefensiveCopyOfStages(t *testing.T) {
	// Mutating the returned slice must not poison the registry — a
	// later Resolve call must see the original shape.
	first, _ := chain.Resolve(chain.DefaultChainID)
	originalName := first.Stages[0].Name
	first.Stages[0].Name = "mutated_by_test"

	second, err := chain.Resolve(chain.DefaultChainID)
	if err != nil {
		t.Fatalf("second resolve: %v", err)
	}
	if second.Stages[0].Name != originalName {
		t.Fatalf("registry leaked mutation: stage[0].Name = %q", second.Stages[0].Name)
	}
}
