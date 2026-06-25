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
