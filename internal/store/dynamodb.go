package store

import (
	"context"
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
}

// DynamoStore implements store.Store on top of a single-table DynamoDB
// layout. Stage items live under PK="JOB#<id>", SK="PAGE#<page>#STAGE#...".
//
// v0 writes use PutItem (last-writer-wins). The "skip if succeeded"
// guarantee is enforced by Executor reading the record via GetStage before
// running the step. With Lambda reserved concurrency holding the worker
// fleet small, the GetStage→MarkRunning race is acceptable for now; a
// conditional-update lease can be layered in later (see design §8).
type DynamoStore struct {
	Client DynamoClient
	Table  string
}

func NewDynamoStore(client DynamoClient, table string) *DynamoStore {
	return &DynamoStore{Client: client, Table: table}
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
	}
}

func jobPK(jobID string) string {
	return "JOB#" + jobID
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
