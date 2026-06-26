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

	// --- Data plane ------------------------------------------------------

	bucket := awss3.NewBucket(stack, jsii.String("ArtifactBucket"), &awss3.BucketProps{
		// Server-side encryption with the S3-managed key — fine for v0.
		Encryption:        awss3.BucketEncryption_S3_MANAGED,
		BlockPublicAccess: awss3.BlockPublicAccess_BLOCK_ALL(),
		Versioned:         jsii.Bool(true),
		// In v0 stack teardowns we keep the data — operator can drain
		// manually before destroy.
		RemovalPolicy: awscdk.RemovalPolicy_RETAIN,
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
	})

	// --- Stage queue + DLQ ----------------------------------------------

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
		"LADY_GLASS_TABLE":          table.TableName(),
		"LADY_GLASS_BUCKET":         bucket.BucketName(),
		"LADY_GLASS_GEMINI_API_KEY": geminiAPIKey,
	}, stageRuntimeProps{
		ReservedConcurrency: 10,
		BatchSize:           1,
		MaxConcurrency:      10,
	})

	bucket.GrantReadWrite(geminiLambda, nil)
	table.GrantReadWriteData(geminiLambda)

	// --- Workflow Lambdas ------------------------------------------------

	submitPages := makeLambda("SubmitPagesLambda", "submit-pages-lambda", &map[string]*string{
		"LADY_GLASS_TABLE":        table.TableName(),
		"LADY_GLASS_QUEUE_GEMINI": geminiQueue.QueueUrl(),
	})
	table.GrantReadWriteData(submitPages)
	geminiQueue.GrantSendMessages(submitPages)

	checkPages := makeLambda("CheckPagesLambda", "check-pages-lambda", &map[string]*string{
		"LADY_GLASS_TABLE": table.TableName(),
	})
	table.GrantReadData(checkPages)

	merge := makeLambda("MergeLambda", "merge-lambda", &map[string]*string{
		"LADY_GLASS_TABLE":  table.TableName(),
		"LADY_GLASS_BUCKET": bucket.BucketName(),
	})
	table.GrantReadWriteData(merge)
	bucket.GrantReadWrite(merge, nil)

	markFailed := makeLambda("MarkJobFailedLambda", "mark-job-failed-lambda", &map[string]*string{
		"LADY_GLASS_TABLE": table.TableName(),
	})
	table.GrantReadWriteData(markFailed)

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
			"SubmitPagesLambdaArn":   submitPages.FunctionArn(),
			"CheckPagesLambdaArn":    checkPages.FunctionArn(),
			"MergeLambdaArn":         merge.FunctionArn(),
			"MarkJobFailedLambdaArn": markFailed.FunctionArn(),
		},
	})

	// Allow SFN to invoke the task Lambdas (CDK auto-creates the role,
	// but the grants make the intent explicit and keep the principle
	// of least privilege auditable in the synth output).
	submitPages.GrantInvoke(stateMachine)
	checkPages.GrantInvoke(stateMachine)
	merge.GrantInvoke(stateMachine)
	markFailed.GrantInvoke(stateMachine)

	// --- API Lambda + HTTP API -------------------------------------------

	// Shared API key. Operator provisions out-of-band before deploy:
	//   aws ssm put-parameter --type String --name /lady-glass/api-key \
	//       --value "$(openssl rand -hex 32)"
	apiKey := awsssm.StringParameter_ValueForStringParameter(
		stack,
		jsii.String("/lady-glass/api-key"),
		nil,
	)

	apiLambda := makeLambda("ApiLambda", "api-lambda", &map[string]*string{
		"LADY_GLASS_TABLE":             table.TableName(),
		"LADY_GLASS_BUCKET":            bucket.BucketName(),
		"LADY_GLASS_STATE_MACHINE_ARN": stateMachine.StateMachineArn(),
		"LADY_GLASS_API_KEY":           apiKey,
		"LADY_GLASS_FIRST_QUEUE":       jsii.String("gemini"),
		"LADY_GLASS_FINAL_STAGE":       jsii.String("gemini"),
		"LADY_GLASS_FINAL_VERSION":     jsii.String("v1"),
		"LADY_GLASS_UPLOAD_EXPIRES_MIN": jsii.String("15"),
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
