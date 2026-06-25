// CDK entry point for the Lady Glass infrastructure.
//
// Build & synth:
//
//	cd infra/cdk
//	./build-lambdas.sh        # cross-compile every Go Lambda to bin/*/bootstrap
//	cdk synth                 # emit CloudFormation under cdk.out/
//	cdk deploy                # apply
//
// Required SSM parameter (created out-of-band before the first deploy):
//
//	/lady-glass/gemini-api-key   the Google AI Studio API key, plain String
package main

import (
	"os"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/jsii-runtime-go"
)

func main() {
	defer jsii.Close()

	app := awscdk.NewApp(nil)

	NewLadyGlassStack(app, "LadyGlassStack", &LadyGlassStackProps{
		StackProps: awscdk.StackProps{
			Env: env(),
		},
	})

	app.Synth(nil)
}

// env reads the standard CDK_DEFAULT_* env vars set by the CDK CLI so the
// stack synthesises against the caller's account / region. Leave them
// unset and the stack becomes env-agnostic (cross-region / cross-account
// deploys), which is the default we want for v0.
func env() *awscdk.Environment {
	account := os.Getenv("CDK_DEFAULT_ACCOUNT")
	region := os.Getenv("CDK_DEFAULT_REGION")
	if account == "" || region == "" {
		return nil
	}
	return &awscdk.Environment{
		Account: jsii.String(account),
		Region:  jsii.String(region),
	}
}
