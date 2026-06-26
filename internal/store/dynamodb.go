package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/keix/lady-glass/internal/pipeline"
)

// DynamoClient is the subset of the AWS DynamoDB client used by DynamoStore.
type DynamoClient interface {
	GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	Query(ctx context.Context, params *dynamodb.QueryInput, optFns ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
}

// DynamoStore implements store.Store on top of a single-table DynamoDB
// layout. Stage items live under PK="JOB#<id>", SK="PAGE#<page>#STAGE#...".
//
// v0 writes use PutItem (last-writer-wins). The "skip if succeeded"
// guarantee is enforced by Executor reading the record via GetStage before
// running the step. With Lambda reserved concurrency holding the worker
// fleet small, the GetStage→MarkRunning race is acceptable for now; a
// conditional-update lease can be layered in later (see design §8).
//
// RetentionDays (when non-zero) drives DynamoDB's per-item TTL via the
// expires_at attribute (see SPEC §S9). Every Put computes
// expires_at = now + RetentionDays * 86400 so a quiet job ages out
// after the configured window from its last activity; an active job's
// expiry slides forward on every transition. Get / List also filter
// out rows whose expires_at has already passed, because DDB's TTL
// reaper runs asynchronously (lag of up to 48h is documented) and the
// API's "the row is gone" semantic must be strict.
type DynamoStore struct {
	Client DynamoClient
	Table  string

	// RetentionDays is the TTL window in days. Zero disables the TTL
	// attribute (legacy / test path); set to e.g. 14 in production.
	RetentionDays int

	// Now is the time source for expires_at computation and read-time
	// filtering. Defaults to time.Now when nil.
	Now func() time.Time
}

func NewDynamoStore(client DynamoClient, table string) *DynamoStore {
	return &DynamoStore{Client: client, Table: table}
}

func (s *DynamoStore) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// expiresAt returns the unix-epoch second at which a row written now
// should expire, or 0 when retention is disabled.
func (s *DynamoStore) expiresAt() int64 {
	if s.RetentionDays <= 0 {
		return 0
	}
	return s.now().Add(time.Duration(s.RetentionDays) * 24 * time.Hour).Unix()
}

// expired reports whether the given expires_at (unix-epoch seconds) is
// already in the past relative to the store's clock. Zero is treated as
// "never expires" (legacy rows, in-memory paths).
func (s *DynamoStore) expired(expiresAt int64) bool {
	if expiresAt == 0 {
		return false
	}
	return expiresAt <= s.now().Unix()
}

func (s *DynamoStore) GetStage(ctx context.Context, jobID string, page int, stage, version string) (*StageRecord, error) {
	out, err := s.Client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(s.Table),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: jobPK(jobID)},
			"sk": &types.AttributeValueMemberS{Value: stageSK(page, stage, version)},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("store: get stage: %w", err)
	}
	if out.Item == nil {
		return nil, nil
	}

	var item stageItem
	if err := attributevalue.UnmarshalMap(out.Item, &item); err != nil {
		return nil, fmt.Errorf("store: unmarshal stage: %w", err)
	}
	if s.expired(item.ExpiresAt) {
		return nil, nil
	}
	rec := item.toRecord()
	return &rec, nil
}

func (s *DynamoStore) MarkRunning(ctx context.Context, in pipeline.StepInput) error {
	return s.putStage(ctx, stageItem{
		PK:             jobPK(in.JobID),
		SK:             stageSK(in.Page, in.Stage, in.Version),
		JobID:          in.JobID,
		Page:           in.Page,
		Stage:          in.Stage,
		Version:        in.Version,
		Status:         string(StageStatusRunning),
		IdempotencyKey: pipeline.StageKey(in.JobID, in.Page, in.Stage, in.Version),
		InputURI:       in.InputURI,
		UpdatedAt:      nowRFC3339(),
		ExpiresAt:      s.expiresAt(),
	})
}

func (s *DynamoStore) MarkSucceeded(ctx context.Context, out pipeline.StepOutput, nextStage string) error {
	return s.putStage(ctx, stageItem{
		PK:             jobPK(out.JobID),
		SK:             stageSK(out.Page, out.Stage, out.Version),
		JobID:          out.JobID,
		Page:           out.Page,
		Stage:          out.Stage,
		Version:        out.Version,
		Status:         string(StageStatusSucceeded),
		IdempotencyKey: pipeline.StageKey(out.JobID, out.Page, out.Stage, out.Version),
		ResultURI:      out.ResultURI,
		NextStage:      nextStage,
		UpdatedAt:      nowRFC3339(),
		ExpiresAt:      s.expiresAt(),
	})
}

func (s *DynamoStore) MarkFailed(ctx context.Context, in pipeline.StepInput, runErr error) error {
	return s.putStage(ctx, stageItem{
		PK:             jobPK(in.JobID),
		SK:             stageSK(in.Page, in.Stage, in.Version),
		JobID:          in.JobID,
		Page:           in.Page,
		Stage:          in.Stage,
		Version:        in.Version,
		Status:         string(StageStatusFailed),
		IdempotencyKey: pipeline.StageKey(in.JobID, in.Page, in.Stage, in.Version),
		InputURI:       in.InputURI,
		Error:          runErr.Error(),
		UpdatedAt:      nowRFC3339(),
		ExpiresAt:      s.expiresAt(),
	})
}

func (s *DynamoStore) putStage(ctx context.Context, item stageItem) error {
	av, err := attributevalue.MarshalMap(item)
	if err != nil {
		return fmt.Errorf("store: marshal stage: %w", err)
	}
	if _, err := s.Client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.Table),
		Item:      av,
	}); err != nil {
		return fmt.Errorf("store: put stage: %w", err)
	}
	return nil
}

func (s *DynamoStore) GetJob(ctx context.Context, jobID string) (*JobRecord, error) {
	out, err := s.Client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(s.Table),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: jobPK(jobID)},
			"sk": &types.AttributeValueMemberS{Value: jobSK()},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("store: get job: %w", err)
	}
	if out.Item == nil {
		return nil, nil
	}

	var item jobItem
	if err := attributevalue.UnmarshalMap(out.Item, &item); err != nil {
		return nil, fmt.Errorf("store: unmarshal job: %w", err)
	}
	if s.expired(item.ExpiresAt) {
		return nil, nil
	}
	rec := item.toRecord()
	return &rec, nil
}

func (s *DynamoStore) PutJob(ctx context.Context, rec JobRecord) error {
	chainJSON, err := encodeChain(rec.Chain)
	if err != nil {
		return fmt.Errorf("store: encode chain: %w", err)
	}
	item := jobItem{
		PK:        jobPK(rec.JobID),
		SK:        jobSK(),
		JobID:     rec.JobID,
		Status:    string(rec.Status),
		InputURI:  rec.InputURI,
		ResultURI: rec.ResultURI,
		PageCount: rec.PageCount,
		Mode:      rec.Mode,
		ChainID:   rec.ChainID,
		ChainJSON: chainJSON,
		Error:     rec.Error,
		UpdatedAt: nowRFC3339(),
		ExpiresAt: s.expiresAt(),
	}

	av, err := attributevalue.MarshalMap(item)
	if err != nil {
		return fmt.Errorf("store: marshal job: %w", err)
	}
	if _, err := s.Client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.Table),
		Item:      av,
	}); err != nil {
		return fmt.Errorf("store: put job: %w", err)
	}
	return nil
}

// ListStagesByJob queries the single-table layout using PK=JOB#<id> and
// SK begins_with PAGE# and matches the (stage, version) suffix client-side.
// For v0 the result fits in a single Query response; pagination via
// LastEvaluatedKey is deferred until a job needs more pages than a single
// 1MB Query page can carry.
func (s *DynamoStore) ListStagesByJob(ctx context.Context, jobID string, stage string, version string) ([]StageRecord, error) {
	out, err := s.Client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(s.Table),
		KeyConditionExpression: aws.String("#pk = :pk AND begins_with(#sk, :sk_prefix)"),
		ExpressionAttributeNames: map[string]string{
			"#pk": "pk",
			"#sk": "sk",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk":        &types.AttributeValueMemberS{Value: jobPK(jobID)},
			":sk_prefix": &types.AttributeValueMemberS{Value: "PAGE#"},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("store: list stages: %w", err)
	}

	suffix := fmt.Sprintf("#STAGE#%s#%s", stage, version)

	out2 := make([]StageRecord, 0, len(out.Items))
	for _, raw := range out.Items {
		var item stageItem
		if err := attributevalue.UnmarshalMap(raw, &item); err != nil {
			return nil, fmt.Errorf("store: unmarshal stage: %w", err)
		}
		if !endsWith(item.SK, suffix) {
			continue
		}
		if s.expired(item.ExpiresAt) {
			continue
		}
		out2 = append(out2, item.toRecord())
	}
	return out2, nil
}

func endsWith(s, suffix string) bool {
	return len(s) >= len(suffix) && s[len(s)-len(suffix):] == suffix
}

// stageItem is the DynamoDB representation of a StageRecord. Field tags are
// the persisted attribute names; PK/SK are partition and sort keys.
type stageItem struct {
	PK string `dynamodbav:"pk"`
	SK string `dynamodbav:"sk"`

	JobID   string `dynamodbav:"job_id"`
	Page    int    `dynamodbav:"page,omitempty"`
	Stage   string `dynamodbav:"stage"`
	Version string `dynamodbav:"version"`

	Status string `dynamodbav:"status"`

	IdempotencyKey string `dynamodbav:"idempotency_key"`

	InputURI  string `dynamodbav:"input_uri,omitempty"`
	ResultURI string `dynamodbav:"result_uri,omitempty"`
	NextStage string `dynamodbav:"next_stage,omitempty"`
	Error     string `dynamodbav:"error,omitempty"`

	UpdatedAt string `dynamodbav:"updated_at"`
	// ExpiresAt is the per-item TTL attribute (DDB TableProps wire
	// expires_at as the TimeToLiveAttribute). Unix-epoch seconds.
	ExpiresAt int64 `dynamodbav:"expires_at,omitempty"`
}

func (i stageItem) toRecord() StageRecord {
	return StageRecord{
		JobID:          i.JobID,
		Page:           i.Page,
		Stage:          i.Stage,
		Version:        i.Version,
		Status:         StageStatus(i.Status),
		IdempotencyKey: i.IdempotencyKey,
		InputURI:       i.InputURI,
		ResultURI:      i.ResultURI,
		NextStage:      i.NextStage,
		Error:          i.Error,
		ExpiresAt:      i.ExpiresAt,
	}
}

// jobItem is the DynamoDB representation of a JobRecord. PK=JOB#<id>,
// SK=META so it lives in the same partition as the job's stage rows for
// single-query retrieval.
type jobItem struct {
	PK string `dynamodbav:"pk"`
	SK string `dynamodbav:"sk"`

	JobID     string `dynamodbav:"job_id"`
	Status    string `dynamodbav:"status"`
	InputURI  string `dynamodbav:"input_uri,omitempty"`
	ResultURI string `dynamodbav:"result_uri,omitempty"`
	PageCount int    `dynamodbav:"page_count,omitempty"`
	Mode      string `dynamodbav:"mode,omitempty"`
	// ChainID and ChainJSON together implement SPEC §S10's frozen
	// chain contract. ChainJSON is a JSON-encoded []pipeline.StageSpec
	// kept as a string attribute so the schema does not bake in DDB's
	// List<Map> shape — a future move into a typed DDB list is an
	// implementation choice, not a wire change. Empty strings on
	// legacy rows resolve to chain.DefaultChainID at read time.
	ChainID   string `dynamodbav:"chain_id,omitempty"`
	ChainJSON string `dynamodbav:"chain_json,omitempty"`
	Error     string `dynamodbav:"error,omitempty"`

	UpdatedAt string `dynamodbav:"updated_at"`
	ExpiresAt int64  `dynamodbav:"expires_at,omitempty"`
}

func (i jobItem) toRecord() JobRecord {
	chain, _ := decodeChain(i.ChainJSON)
	return JobRecord{
		JobID:     i.JobID,
		Status:    JobStatus(i.Status),
		InputURI:  i.InputURI,
		ResultURI: i.ResultURI,
		PageCount: i.PageCount,
		Mode:      i.Mode,
		ChainID:   i.ChainID,
		Chain:     chain,
		Error:     i.Error,
		UpdatedAt: i.UpdatedAt,
		ExpiresAt: i.ExpiresAt,
	}
}

// encodeChain serialises the frozen ChainSpec list for storage. Empty /
// nil chains map to the empty string so the omitempty tag suppresses
// the attribute entirely on legacy rows.
func encodeChain(chain []pipeline.StageSpec) (string, error) {
	if len(chain) == 0 {
		return "", nil
	}
	body, err := json.Marshal(chain)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

// decodeChain is the inverse. A malformed string (manual edit, schema
// drift) returns the empty slice rather than poisoning the read path;
// callers can detect this via the parallel ChainID being non-empty and
// fall back to chain.Resolve(rec.ChainID).
func decodeChain(s string) ([]pipeline.StageSpec, error) {
	if s == "" {
		return nil, nil
	}
	var chain []pipeline.StageSpec
	if err := json.Unmarshal([]byte(s), &chain); err != nil {
		return nil, err
	}
	return chain, nil
}

func jobPK(jobID string) string {
	return "JOB#" + jobID
}

func jobSK() string {
	return "META"
}

func stageSK(page int, stage, version string) string {
	if page > 0 {
		return fmt.Sprintf("PAGE#%06d#STAGE#%s#%s", page, stage, version)
	}
	return fmt.Sprintf("STAGE#%s#%s", stage, version)
}

func nowRFC3339() string {
	return time.Now().UTC().Format(time.RFC3339)
}
