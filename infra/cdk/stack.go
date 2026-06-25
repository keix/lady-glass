package main

import (
	"path/filepath"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsdynamodb"
	"github.com/aws/aws-cdk-go/awscdk/v2/awslambda"
	awslambdasources "github.com/aws/aws-cdk-go/awscdk/v2/awslambdaeventsources"
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
		// Visibility timeout must comfortably exceed the Lambda timeout;
		// the executor's "succeeded → skip" path handles redelivery
		// either way, so a generous value here only costs latency on
		// genuinely stuck messages.
		VisibilityTimeout: awscdk.Duration_Seconds(jsii.Number(120)),
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
			Timeout:      awscdk.Duration_Seconds(jsii.Number(60)),
			Environment:  env,
		})
	}

	// --- Stage Lambda: gemini --------------------------------------------

	geminiLambda := makeLambda("GeminiLambda", "gemini-lambda", &map[string]*string{
		"LADY_GLASS_TABLE":          table.TableName(),
		"LADY_GLASS_BUCKET":         bucket.BucketName(),
		"LADY_GLASS_GEMINI_API_KEY": geminiAPIKey,
	})
	// Cap concurrent Gemini calls so the upstream API limits stay
	// inside the per-stage Lambda, exactly as design §10 calls out.
	geminiLambda.Node().DefaultChild().(awslambda.CfnFunction).
		AddPropertyOverride(jsii.String("ReservedConcurrentExecutions"), jsii.Number(10))

	bucket.GrantReadWrite(geminiLambda, nil)
	table.GrantReadWriteData(geminiLambda)

	// SQS → gemini-lambda
	geminiLambda.AddEventSource(awslambdasources.NewSqsEventSource(geminiQueue, &awslambdasources.SqsEventSourceProps{
		BatchSize:               jsii.Number(1),
		MaxConcurrency:          jsii.Number(10),
		ReportBatchItemFailures: jsii.Bool(false),
	}))

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

	return stack
}
