package main

import (
	"path/filepath"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsapigatewayv2"
	awsapigatewayv2integrations "github.com/aws/aws-cdk-go/awscdk/v2/awsapigatewayv2integrations"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsdynamodb"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsiam"
	"github.com/aws/aws-cdk-go/awscdk/v2/awslambda"
	awslambdasources "github.com/aws/aws-cdk-go/awscdk/v2/awslambdaeventsources"
	"github.com/aws/aws-cdk-go/awscdk/v2/awslogs"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3"
	"github.com/aws/aws-cdk-go/awscdk/v2/awssqs"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsssm"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsstepfunctions"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
)

// LadyGlassStackProps wraps awscdk.StackProps so callers can extend it
// later without changing the constructor signature.
type LadyGlassStackProps struct {
	awscdk.StackProps
}

// stageRuntimeProps tunes the per-stage runtime knobs that the spec
// (SPEC.md §S1, §S8) makes mutable per stage. Every SQS-triggered
// stage MUST supply these — the values are how the stage matches its
// underlying provider's rate limit.
type stageRuntimeProps struct {
	// ReservedConcurrency caps the concurrent Lambda invocations for
	// this stage. Zero means "no override" (the account-wide pool).
	// SPEC.md §S1 SHOULD: set this to bound the provider's rate.
	ReservedConcurrency int

	// BatchSize is the number of SQS messages each Lambda invocation
	// processes. SPEC.md §S1 fixes this at 1 for v1 of the spec; any
	// value other than 1 implies a spec version bump.
	BatchSize int

	// MaxConcurrency caps the SQS → Lambda in-flight messages. AWS
	// guidance is MaxConcurrency >= ReservedConcurrency so the ESM
	// never sits idle while the Lambda has capacity.
	MaxConcurrency int
}

// NewLadyGlassStack defines every resource Lady Glass needs to run:
// the artifact bucket, the control-plane table, the per-page SQS queue
// + DLQ, the five Lambda functions, the SQS → gemini-lambda Event
// Source Mapping, and the Step Functions state machine assembled from
// infra/state_machine.asl.json with ARN substitution.
func NewLadyGlassStack(scope constructs.Construct, id string, props *LadyGlassStackProps) awscdk.Stack {
	var sprops awscdk.StackProps
	if props != nil {
		sprops = props.StackProps
	}
	stack := awscdk.NewStack(scope, &id, &sprops)

	// SPEC §S9 retention window. Single knob keeps DDB TTL, the S3
	// lifecycle rule, and the Lambdas' DynamoStore.RetentionDays
	// configuration in lockstep. Bump the constant to change all
	// three at once.
	retentionDays := jsii.String("14")

	// --- Data plane ------------------------------------------------------

	bucket := awss3.NewBucket(stack, jsii.String("ArtifactBucket"), &awss3.BucketProps{
		// Server-side encryption with the S3-managed key — fine for v0.
		Encryption:        awss3.BucketEncryption_S3_MANAGED,
		BlockPublicAccess: awss3.BlockPublicAccess_BLOCK_ALL(),
		Versioned:         jsii.Bool(true),
		// In v0 stack teardowns we keep the data — operator can drain
		// manually before destroy.
		RemovalPolicy: awscdk.RemovalPolicy_RETAIN,
		// SPEC §S9: per-job artifacts (source PDFs, per-page splits,
		// per-page result JSONs, merged result JSONs) are execution
		// state, not records of record. Expire them after 14 days so
		// the bucket does not grow unboundedly. Noncurrent versions
		// (from Versioned: true) expire one day later so a same-day
		// overwrite stays recoverable for ~24h. The DDB row TTL is
		// kept in lockstep at 14 days so DDB and S3 retire the job's
		// state together.
		LifecycleRules: &[]*awss3.LifecycleRule{
			{
				Id:                          jsii.String("expire-job-artifacts"),
				Enabled:                     jsii.Bool(true),
				Expiration:                  awscdk.Duration_Days(jsii.Number(14)),
				NoncurrentVersionExpiration: awscdk.Duration_Days(jsii.Number(15)),
			},
		},
	})

	// The permanent bucket is the durable side of the split the Kowloon
	// integration (docs/kowloon-integration.md §5) introduces: ArchiveResult
	// copies the flattened transactions.v1 document, the raw PDF, and a
	// per-job manifest here, and IndexKowloon writes its sidecar here. It
	// deliberately has NO lifecycle expiry — unlike the 14-day artifact
	// bucket, this is the record of record, the source Kowloon rebuilds
	// its index from. Versioned so an accidental overwrite stays
	// recoverable; RETAIN so a stack teardown never deletes durable data.
	permanentBucket := awss3.NewBucket(stack, jsii.String("PermanentBucket"), &awss3.BucketProps{
		Encryption:        awss3.BucketEncryption_S3_MANAGED,
		BlockPublicAccess: awss3.BlockPublicAccess_BLOCK_ALL(),
		Versioned:         jsii.Bool(true),
		RemovalPolicy:     awscdk.RemovalPolicy_RETAIN,
	})

	// --- Control plane ---------------------------------------------------

	table := awsdynamodb.NewTable(stack, jsii.String("StateTable"), &awsdynamodb.TableProps{
		PartitionKey: &awsdynamodb.Attribute{
			Name: jsii.String("pk"),
			Type: awsdynamodb.AttributeType_STRING,
		},
		SortKey: &awsdynamodb.Attribute{
			Name: jsii.String("sk"),
			Type: awsdynamodb.AttributeType_STRING,
		},
		// Pay per request keeps v0 cost predictable at low job rates;
		// switch to PROVISIONED with autoscaling once steady-state load
		// is measurable.
		BillingMode:   awsdynamodb.BillingMode_PAY_PER_REQUEST,
		RemovalPolicy: awscdk.RemovalPolicy_RETAIN,
		// SPEC §S9: per-item TTL drives DDB to delete expired job /
		// stage rows. DynamoStore writes the unix-epoch second under
		// "expires_at" on every Put (see internal/store/dynamodb.go
		// RetentionDays). DDB's TTL reaper is asynchronous (up to ~48h
		// lag is documented); the store layer also filters expired
		// rows at read time so the lag is invisible to callers.
		TimeToLiveAttribute: jsii.String("expires_at"),
	})

	// --- Stage queues + DLQs --------------------------------------------

	geminiDLQ := awssqs.NewQueue(stack, jsii.String("GeminiDLQ"), &awssqs.QueueProps{
		RetentionPeriod: awscdk.Duration_Days(jsii.Number(14)),
	})

	geminiQueue := awssqs.NewQueue(stack, jsii.String("GeminiQueue"), &awssqs.QueueProps{
		// Visibility timeout must be ≥ Lambda timeout × ~1.5 (AWS
		// guidance) so a slow Gemini call cannot end up with the same
		// message redelivered to a second worker mid-flight. Lambda
		// timeout is 300s below, so 600s gives a 2× margin.
		VisibilityTimeout: awscdk.Duration_Seconds(jsii.Number(600)),
		DeadLetterQueue: &awssqs.DeadLetterQueue{
			Queue:           geminiDLQ,
			MaxReceiveCount: jsii.Number(5),
		},
	})

	normalizeDLQ := awssqs.NewQueue(stack, jsii.String("NormalizeCardStatementDLQ"), &awssqs.QueueProps{
		RetentionPeriod: awscdk.Duration_Days(jsii.Number(14)),
	})

	// The normaliser is pure compute (S3 read + S3 write + DDB update)
	// and finishes in well under a second per page, so a short VT would
	// suit the workload. But Lambda's ESM requires VisibilityTimeout >=
	// Function Timeout, and makeLambda hardcodes every Lambda at 300s,
	// so 600s is the minimum that also satisfies the AWS guidance of
	// VT >= LT × ~1.5 — same rule as the Gemini queue.
	normalizeQueue := awssqs.NewQueue(stack, jsii.String("NormalizeCardStatementQueue"), &awssqs.QueueProps{
		VisibilityTimeout: awscdk.Duration_Seconds(jsii.Number(600)),
		DeadLetterQueue: &awssqs.DeadLetterQueue{
			Queue:           normalizeDLQ,
			MaxReceiveCount: jsii.Number(5),
		},
	})

	enrichDLQ := awssqs.NewQueue(stack, jsii.String("EnrichTransactionsDLQ"), &awssqs.QueueProps{
		RetentionPeriod: awscdk.Duration_Days(jsii.Number(14)),
	})

	// enrich_transactions is the terminal per-page stage — pure compute
	// (S3 read + dictionary lookup + S3 write), same profile as the
	// normaliser, so the same 600s VT (Lambda timeout 300s × 2 margin,
	// and the ESM's VT >= function-timeout floor) applies.
	enrichQueue := awssqs.NewQueue(stack, jsii.String("EnrichTransactionsQueue"), &awssqs.QueueProps{
		VisibilityTimeout: awscdk.Duration_Seconds(jsii.Number(600)),
		DeadLetterQueue: &awssqs.DeadLetterQueue{
			Queue:           enrichDLQ,
			MaxReceiveCount: jsii.Number(5),
		},
	})

	// --- Secrets / config -----------------------------------------------

	// Operator creates this parameter once before the first deploy:
	//   aws ssm put-parameter --type String --name /lady-glass/gemini-api-key --value AIzaSy...
	// SSM SecureString would be tighter; the v0 trade-off is that a
	// plain String parameter can be referenced by name from CDK and
	// injected directly into Lambda env without code changes. Move to
	// Secrets Manager later if the operator needs rotation or audit.
	geminiAPIKey := awsssm.StringParameter_ValueForStringParameter(
		stack,
		jsii.String("/lady-glass/gemini-api-key"),
		nil,
	)

	// --- Lambda factory --------------------------------------------------

	// All Go Lambdas use the provided.al2023 runtime with an arm64
	// "bootstrap" binary. build-lambdas.sh produces each binary at
	// ./bin/<name>/bootstrap; Code_FromAsset zips and uploads the
	// directory.
	makeLambda := func(id, cmdDir string, env *map[string]*string) awslambda.Function {
		assetPath := filepath.Join("bin", cmdDir)
		return awslambda.NewFunction(stack, jsii.String(id), &awslambda.FunctionProps{
			Runtime:      awslambda.Runtime_PROVIDED_AL2023(),
			Architecture: awslambda.Architecture_ARM_64(),
			Handler:      jsii.String("bootstrap"),
			Code:         awslambda.Code_FromAsset(jsii.String(assetPath), nil),
			MemorySize:   jsii.Number(512),
			// 300s gives the Gemini multimodal call (which can run 30-60s
			// on dense PDFs) enough headroom to complete and to retry
			// inside the SDK if it returns 503/504. The non-Gemini
			// workflow Lambdas finish in <1s so they pay nothing for the
			// generous timeout — billing is on actual duration.
			Timeout: awscdk.Duration_Seconds(jsii.Number(300)),
			// Drop CloudWatch Logs after a week so log storage cannot
			// quietly accumulate; the default is infinite retention.
			// A week is enough for "yesterday's run failed, why" without
			// hoarding a year of stack traces.
			LogRetention: awslogs.RetentionDays_ONE_WEEK,
			Environment:  env,
		})
	}

	// addStage wires an SQS-triggered stage in one place: it builds
	// the Lambda via makeLambda, applies the per-stage reserved
	// concurrency (when set), and attaches the Event Source Mapping
	// with the spec-required batch size + the per-stage MaxConcurrency.
	// Returns the Function so the caller can wire grants.
	addStage := func(id, cmdDir string, queue awssqs.IQueue, env *map[string]*string, rt stageRuntimeProps) awslambda.Function {
		fn := makeLambda(id, cmdDir, env)

		if rt.ReservedConcurrency > 0 {
			fn.Node().DefaultChild().(awslambda.CfnFunction).
				AddPropertyOverride(
					jsii.String("ReservedConcurrentExecutions"),
					jsii.Number(float64(rt.ReservedConcurrency)),
				)
		}

		fn.AddEventSource(awslambdasources.NewSqsEventSource(queue, &awslambdasources.SqsEventSourceProps{
			BatchSize:               jsii.Number(float64(rt.BatchSize)),
			MaxConcurrency:          jsii.Number(float64(rt.MaxConcurrency)),
			ReportBatchItemFailures: jsii.Bool(false),
		}))

		return fn
	}

	// --- Stage Lambda: gemini --------------------------------------------

	// Reserved concurrency 10 matches the Google AI Studio free-tier
	// envelope for gemini-2.5-flash; adjust per the deployed key's
	// quota. Each future stage gets its own stageRuntimeProps value.
	geminiLambda := addStage("GeminiLambda", "gemini-lambda", geminiQueue, &map[string]*string{
		"LADY_GLASS_TABLE":                table.TableName(),
		"LADY_GLASS_BUCKET":               bucket.BucketName(),
		"LADY_GLASS_GEMINI_API_KEY":       geminiAPIKey,
		"LADY_GLASS_RETENTION_DAYS":       retentionDays,
		"LADY_GLASS_NEXT_STAGE_NAME":      jsii.String("normalize_card_statement"),
		"LADY_GLASS_NEXT_STAGE_VERSION":   jsii.String("v1"),
		"LADY_GLASS_NEXT_QUEUE_NAME":      jsii.String("normalize_card_statement"),
		"LADY_GLASS_NEXT_QUEUE_URL":       normalizeQueue.QueueUrl(),
	}, stageRuntimeProps{
		ReservedConcurrency: 10,
		BatchSize:           1,
		MaxConcurrency:      10,
	})

	bucket.GrantReadWrite(geminiLambda, nil)
	table.GrantReadWriteData(geminiLambda)
	normalizeQueue.GrantSendMessages(geminiLambda)

	// --- Stage Lambda: normalize_card_statement -------------------------

	// Pure-compute post-processor: no provider call, no rate limit.
	// Concurrency is bounded by the SQS in-flight rather than a quota,
	// and 25 is comfortable headroom for a 25-page statement to drain
	// in parallel.
	normalizeLambda := addStage("NormalizeCardStatementLambda", "normalize-card-statement-lambda", normalizeQueue, &map[string]*string{
		"LADY_GLASS_TABLE":              table.TableName(),
		"LADY_GLASS_BUCKET":             bucket.BucketName(),
		"LADY_GLASS_RETENTION_DAYS":     retentionDays,
		"LADY_GLASS_NEXT_STAGE_NAME":    jsii.String("enrich_transactions"),
		"LADY_GLASS_NEXT_STAGE_VERSION": jsii.String("v1"),
		"LADY_GLASS_NEXT_QUEUE_NAME":    jsii.String("enrich_transactions"),
		"LADY_GLASS_NEXT_QUEUE_URL":     enrichQueue.QueueUrl(),
	}, stageRuntimeProps{
		BatchSize:      1,
		MaxConcurrency: 25,
	})

	bucket.GrantReadWrite(normalizeLambda, nil)
	table.GrantReadWriteData(normalizeLambda)
	enrichQueue.GrantSendMessages(normalizeLambda)

	// --- Stage Lambda: enrich_transactions ------------------------------

	// Terminal per-page stage: attaches MerchantNormalized / Category /
	// Country from the embedded merchants dictionary (rules only, no
	// provider call). No next-stage env — the SQS chain ends here and the
	// SFN Merge → ArchiveResult → IndexKowloon steps take over. Same
	// concurrency headroom as the normaliser.
	enrichLambda := addStage("EnrichTransactionsLambda", "enrich-transactions-lambda", enrichQueue, &map[string]*string{
		"LADY_GLASS_TABLE":          table.TableName(),
		"LADY_GLASS_BUCKET":         bucket.BucketName(),
		"LADY_GLASS_RETENTION_DAYS": retentionDays,
	}, stageRuntimeProps{
		BatchSize:      1,
		MaxConcurrency: 25,
	})

	bucket.GrantReadWrite(enrichLambda, nil)
	table.GrantReadWriteData(enrichLambda)

	// --- Workflow Lambdas ------------------------------------------------

	submitPages := makeLambda("SubmitPagesLambda", "submit-pages-lambda", &map[string]*string{
		"LADY_GLASS_TABLE":          table.TableName(),
		"LADY_GLASS_QUEUE_GEMINI":   geminiQueue.QueueUrl(),
		"LADY_GLASS_RETENTION_DAYS": retentionDays,
	})
	table.GrantReadWriteData(submitPages)
	geminiQueue.GrantSendMessages(submitPages)

	// check-pages is read-only; the read-time filter on DynamoStore
	// already excludes expired rows regardless of the reader's
	// RetentionDays, so this Lambda does not need the env.
	checkPages := makeLambda("CheckPagesLambda", "check-pages-lambda", &map[string]*string{
		"LADY_GLASS_TABLE": table.TableName(),
	})
	table.GrantReadData(checkPages)

	merge := makeLambda("MergeLambda", "merge-lambda", &map[string]*string{
		"LADY_GLASS_TABLE":          table.TableName(),
		"LADY_GLASS_BUCKET":         bucket.BucketName(),
		"LADY_GLASS_RETENTION_DAYS": retentionDays,
	})
	table.GrantReadWriteData(merge)
	bucket.GrantReadWrite(merge, nil)

	markFailed := makeLambda("MarkJobFailedLambda", "mark-job-failed-lambda", &map[string]*string{
		"LADY_GLASS_TABLE":          table.TableName(),
		"LADY_GLASS_RETENTION_DAYS": retentionDays,
	})
	table.GrantReadWriteData(markFailed)

	renderPages := makeLambda("RenderPagesLambda", "render-pages-lambda", &map[string]*string{
		"LADY_GLASS_BUCKET": bucket.BucketName(),
	})
	bucket.GrantReadWrite(renderPages, nil)

	// SPEC §S11: the post-commit observer. Runs after both Merge
	// (succeeded) and MarkJobFailed (failed); reads JobRecord to
	// dispatch by Status. Has no write grant — observers MUST NOT
	// mutate the JobRecord — and ships with notify.NoOp as the
	// default sink until an external subscriber lands.
	notifyCompletion := makeLambda("NotifyCompletionLambda", "notify-completion-lambda", &map[string]*string{
		"LADY_GLASS_TABLE":          table.TableName(),
		"LADY_GLASS_RETENTION_DAYS": retentionDays,
	})
	table.GrantReadData(notifyCompletion)

	// --- Kowloon integration Lambdas (docs/kowloon-integration.md §5-6) ---

	// tenantID is the single customer scope Lady Glass runs under today.
	// The archive key partitions on it (tenant=<id>/...) and Kowloon uses
	// it to scope search results. There is no per-job tenant model yet, so
	// it is a stack-level constant threaded into the ArchiveResult /
	// IndexKowloon SFN states via DefinitionSubstitutions. When a real
	// multi-tenant model lands, this moves to the job record and the ASL
	// reads it from execution input instead of this substitution.
	tenantID := jsii.String("keix")

	// KOWLOON_BASE_URL is the private Kowloon front door. Operator
	// provisions it once before the first deploy:
	//   aws ssm put-parameter --type String --name /lady-glass/kowloon-base-url --value https://kowloon.internal
	// KOWLOON_API_KEY is intentionally NOT wired yet — v0 Kowloon has no
	// front-door auth (§9); the client sends no X-Api-Key when it is empty.
	kowloonBaseURL := awsssm.StringParameter_ValueForStringParameter(
		stack,
		jsii.String("/lady-glass/kowloon-base-url"),
		nil,
	)

	// ArchiveResult reads the merged result from the artifact (stage)
	// bucket and writes the flattened archive + manifest to the permanent
	// bucket. Read-only on DynamoDB (GetJob), read on the stage bucket,
	// read-write on the permanent bucket.
	archiveResult := makeLambda("ArchiveResultLambda", "archive-result-lambda", &map[string]*string{
		"LADY_GLASS_TABLE":            table.TableName(),
		"LADY_GLASS_BUCKET":           bucket.BucketName(),
		"LADY_GLASS_PERMANENT_BUCKET": permanentBucket.BucketName(),
	})
	table.GrantReadData(archiveResult)
	bucket.GrantRead(archiveResult, nil)
	permanentBucket.GrantReadWrite(archiveResult, nil)

	// IndexKowloon reads the manifest and writes the sidecar in the
	// permanent bucket, and makes the one HTTP call to Kowloon. Read-only
	// on DynamoDB (GetJob); the stage bucket is not touched here.
	indexKowloon := makeLambda("IndexKowloonLambda", "index-kowloon-lambda", &map[string]*string{
		"LADY_GLASS_TABLE":            table.TableName(),
		"LADY_GLASS_PERMANENT_BUCKET": permanentBucket.BucketName(),
		"KOWLOON_BASE_URL":            kowloonBaseURL,
	})
	table.GrantReadData(indexKowloon)
	permanentBucket.GrantReadWrite(indexKowloon, nil)

	// --- Step Functions state machine ------------------------------------

	// The ASL ships in the repo at ../state_machine.asl.json. Loading it
	// via DefinitionBody_FromFile lets us keep one canonical
	// representation: humans (and CI tools like awslint / aws-stepfunctions-local)
	// read the JSON directly, CDK substitutes the ARN placeholders.
	stateMachine := awsstepfunctions.NewStateMachine(stack, jsii.String("LadyGlassWorkflow"), &awsstepfunctions.StateMachineProps{
		StateMachineType: awsstepfunctions.StateMachineType_STANDARD,
		DefinitionBody: awsstepfunctions.DefinitionBody_FromFile(
			jsii.String("../state_machine.asl.json"),
			nil,
		),
		DefinitionSubstitutions: &map[string]*string{
			"SubmitPagesLambdaArn":      submitPages.FunctionArn(),
			"CheckPagesLambdaArn":       checkPages.FunctionArn(),
			"MergeLambdaArn":            merge.FunctionArn(),
			"MarkJobFailedLambdaArn":    markFailed.FunctionArn(),
			"RenderPagesLambdaArn":      renderPages.FunctionArn(),
			"NotifyCompletionLambdaArn": notifyCompletion.FunctionArn(),
			"ArchiveResultLambdaArn":    archiveResult.FunctionArn(),
			"IndexKowloonLambdaArn":     indexKowloon.FunctionArn(),
			"TenantID":                  tenantID,
		},
	})

	// Allow SFN to invoke the task Lambdas (CDK auto-creates the role,
	// but the grants make the intent explicit and keep the principle
	// of least privilege auditable in the synth output).
	submitPages.GrantInvoke(stateMachine)
	checkPages.GrantInvoke(stateMachine)
	merge.GrantInvoke(stateMachine)
	markFailed.GrantInvoke(stateMachine)
	renderPages.GrantInvoke(stateMachine)
	notifyCompletion.GrantInvoke(stateMachine)
	archiveResult.GrantInvoke(stateMachine)
	indexKowloon.GrantInvoke(stateMachine)

	// --- API Lambda + HTTP API -------------------------------------------

	// Shared API key. Operator provisions out-of-band before deploy:
	//   aws ssm put-parameter --type String --name /lady-glass/api-key \
	//       --value "$(openssl rand -hex 32)"
	apiKey := awsssm.StringParameter_ValueForStringParameter(
		stack,
		jsii.String("/lady-glass/api-key"),
		nil,
	)

	// FIRST_QUEUE / FINAL_STAGE / FINAL_VERSION used to live here as
	// chain config. They are now resolved per-job via
	// internal/chain.Resolve() at createJob time and frozen onto the
	// JobRecord (SPEC §S10), so the API Lambda no longer needs to
	// know the chain shape ahead of time.
	apiLambda := makeLambda("ApiLambda", "api-lambda", &map[string]*string{
		"LADY_GLASS_TABLE":              table.TableName(),
		"LADY_GLASS_BUCKET":             bucket.BucketName(),
		"LADY_GLASS_STATE_MACHINE_ARN":  stateMachine.StateMachineArn(),
		"LADY_GLASS_API_KEY":            apiKey,
		"LADY_GLASS_UPLOAD_EXPIRES_MIN": jsii.String("15"),
		"LADY_GLASS_RETENTION_DAYS":     retentionDays,
	})
	table.GrantReadWriteData(apiLambda)
	bucket.GrantReadWrite(apiLambda, nil)
	stateMachine.GrantStartExecution(apiLambda)
	// Presigned PUT URL signing requires the issuing role to itself
	// hold s3:PutObject on the target; GrantReadWrite covers it but
	// be explicit so a future grant trim does not silently break
	// presign.
	apiLambda.AddToRolePolicy(awsiam.NewPolicyStatement(&awsiam.PolicyStatementProps{
		Actions:   jsii.Strings("s3:PutObject"),
		Resources: jsii.Strings(*bucket.BucketArn() + "/*"),
	}))

	httpApi := awsapigatewayv2.NewHttpApi(stack, jsii.String("LadyGlassApi"), &awsapigatewayv2.HttpApiProps{
		ApiName:     jsii.String("LadyGlassApi"),
		Description: jsii.String("HTTP API in front of the Lady Glass document workflow."),
	})

	httpApi.AddRoutes(&awsapigatewayv2.AddRoutesOptions{
		Path: jsii.String("/{proxy+}"),
		Methods: &[]awsapigatewayv2.HttpMethod{
			awsapigatewayv2.HttpMethod_ANY,
		},
		Integration: awsapigatewayv2integrations.NewHttpLambdaIntegration(
			jsii.String("ApiLambdaIntegration"),
			apiLambda,
			nil,
		),
	})

	// --- Operator-visible outputs ----------------------------------------

	awscdk.NewCfnOutput(stack, jsii.String("TableName"), &awscdk.CfnOutputProps{
		Value: table.TableName(),
	})
	awscdk.NewCfnOutput(stack, jsii.String("BucketName"), &awscdk.CfnOutputProps{
		Value: bucket.BucketName(),
	})
	awscdk.NewCfnOutput(stack, jsii.String("PermanentBucketName"), &awscdk.CfnOutputProps{
		Value: permanentBucket.BucketName(),
	})
	awscdk.NewCfnOutput(stack, jsii.String("GeminiQueueUrl"), &awscdk.CfnOutputProps{
		Value: geminiQueue.QueueUrl(),
	})
	awscdk.NewCfnOutput(stack, jsii.String("StateMachineArn"), &awscdk.CfnOutputProps{
		Value: stateMachine.StateMachineArn(),
	})
	awscdk.NewCfnOutput(stack, jsii.String("ApiUrl"), &awscdk.CfnOutputProps{
		Value: httpApi.Url(),
	})

	return stack
}
