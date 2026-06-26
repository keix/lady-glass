// Package chain is the registry of stage chains a Lady Glass deployment
// knows how to run. Each entry maps a logical chain ID (e.g.
// "card-statement-v1") to the ordered list of (stage, version, queue)
// tuples a job born on that chain will traverse.
//
// SPEC §S10 puts the chain registry at the foundation of the
// chain-binding contract: at job creation the API resolves a chain ID
// here and freezes the resulting ChainSpec onto the JobRecord. After
// that point, only the JobRecord's frozen copy is read — the registry
// is never consulted again for an existing job, so changes to the
// registry (a new entry added in a code release, an existing entry
// re-versioned) are inert for jobs that were already in flight.
//
// In v0 the registry is a Go constant. A future revision may move it
// into DDB so chains can be added or rotated without a code release;
// SPEC §S10 names the registry by its contract rather than its storage
// shape so that migration is a non-breaking change.
package chain

import (
	"fmt"

	"github.com/keix/lady-glass/internal/pipeline"
)

// DefaultChainID is the chain createJob uses when the request does not
// name one explicitly. The selection of the default is an operator
// concern; a deployment with multiple document types may keep the
// default as the most common one and let the API expose chain_id when
// callers need an alternative.
const DefaultChainID = "card-statement-v1"

// registry is the immutable in-process chain catalog. The map's values
// are themselves slices — callers MUST treat the returned ChainSpec as
// read-only. Resolve copies the slice so a caller mutating the returned
// stages cannot poison the registry.
var registry = map[string]pipeline.ChainSpec{
	DefaultChainID: {
		ID: DefaultChainID,
		Stages: []pipeline.StageSpec{
			{Name: "gemini", Version: "v1", QueueName: "gemini"},
			{Name: "normalize_card_statement", Version: "v1", QueueName: "normalize_card_statement"},
		},
	},
}

// Resolve returns the ChainSpec for id. An empty id maps to
// DefaultChainID so the common case ("just use whatever the operator
// configured as the default") is a single Resolve("") call. An unknown
// id is a hard error so a typo at job creation surfaces immediately
// instead of producing a job whose chain field is silently empty.
func Resolve(id string) (pipeline.ChainSpec, error) {
	if id == "" {
		id = DefaultChainID
	}
	spec, ok := registry[id]
	if !ok {
		return pipeline.ChainSpec{}, fmt.Errorf("chain: %q is not in the registry", id)
	}
	// Defensive copy so a caller cannot mutate the registry through
	// the returned slice.
	stages := make([]pipeline.StageSpec, len(spec.Stages))
	copy(stages, spec.Stages)
	return pipeline.ChainSpec{ID: spec.ID, Stages: stages}, nil
}
