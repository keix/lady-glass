package pipeline

type StepInput struct {
	JobID           string            `json:"job_id"`
	DocumentID      string            `json:"document_id,omitempty"`
	Page            int               `json:"page,omitempty"`
	Stage           string            `json:"stage"`
	Version         string            `json:"version"`
	InputURI        string            `json:"input_uri,omitempty"`
	PreviousURI     string            `json:"previous_uri,omitempty"`
	PromptProfileID string            `json:"prompt_profile_id,omitempty"`
	Metadata        map[string]string `json:"metadata,omitempty"`

	// Chain and ChainIdx carry the job's frozen ChainSpec through the
	// SQS hops (SPEC §S10). SubmitPages projects the chain into the
	// per-page StepInput at ChainIdx=0; each Executor enqueues the
	// next message with ChainIdx incremented. A stage at index N
	// enqueues to Chain[N+1].QueueName; when N+1 == len(Chain) the
	// stage is terminal for this job. Messages without Chain (legacy
	// or out-of-band tests) fall back to the Executor's env-driven
	// NextStage so old in-flight messages continue to drain after a
	// deploy.
	Chain    []StageSpec `json:"chain,omitempty"`
	ChainIdx int         `json:"chain_idx,omitempty"`
}

type StepOutput struct {
	JobID     string            `json:"job_id"`
	Page      int               `json:"page,omitempty"`
	Stage     string            `json:"stage"`
	Version   string            `json:"version"`
	ResultURI string            `json:"result_uri"`
	TextURI   string            `json:"text_uri,omitempty"`
	JSONURI   string            `json:"json_uri,omitempty"`
	Usage     *Usage            `json:"usage,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

type Usage struct {
	Provider     string `json:"provider,omitempty"`
	Model        string `json:"model,omitempty"`
	InputTokens  int    `json:"input_tokens,omitempty"`
	OutputTokens int    `json:"output_tokens,omitempty"`
}

type StageSpec struct {
	Name      string `json:"name"`
	Version   string `json:"version"`
	QueueName string `json:"queue_name"`
}

type ChainSpec struct {
	ID     string      `json:"id"`
	Stages []StageSpec `json:"stages"`
}
