#!/usr/bin/env bash
#
# End-to-end smoke test against the deployed LadyGlassStack.
#
#   ./scripts/smoke-sfn.sh [path/to/document.pdf]
#
# Defaults to testdata/private/smbc.pdf if no argument is given. Steps:
#
#   1. Read BucketName / TableName / StateMachineArn from the
#      LadyGlassStack CloudFormation outputs.
#   2. Upload the PDF to s3://<bucket>/jobs/<job_id>/input.pdf.
#   3. Start a Step Functions execution whose input drives the workflow
#      end-to-end (SubmitPages → Wait → CheckPages → Merge).
#   4. Poll the execution until it leaves RUNNING (or hits a 5 min
#      timeout).
#   5. On SUCCEEDED, fetch the merged result from S3 and pretty-print
#      a preview. On any other terminal state, print the SFn error /
#      cause and dump the JobRecord from DynamoDB for the post-mortem.
#
set -euo pipefail

STACK="${LADY_GLASS_STACK:-LadyGlassStack}"
REGION="${AWS_REGION:-ap-northeast-1}"
PDF_PATH="${1:-testdata/private/smbc.pdf}"

if [[ ! -f "$PDF_PATH" ]]; then
    echo "error: $PDF_PATH not found" >&2
    exit 1
fi

echo "stack:  $STACK"
echo "region: $REGION"
echo "pdf:    $PDF_PATH"
echo

# --- discover outputs -----------------------------------------------------

outputs=$(aws cloudformation describe-stacks \
    --stack-name "$STACK" \
    --region "$REGION" \
    --query 'Stacks[0].Outputs')

bucket=$(echo "$outputs" | jq -r '.[] | select(.OutputKey=="BucketName").OutputValue')
table=$(echo "$outputs" | jq -r '.[] | select(.OutputKey=="TableName").OutputValue')
sm_arn=$(echo "$outputs" | jq -r '.[] | select(.OutputKey=="StateMachineArn").OutputValue')

if [[ -z "$bucket" || -z "$table" || -z "$sm_arn" || "$bucket" == "null" ]]; then
    echo "error: could not discover stack outputs (got bucket=$bucket table=$table sm_arn=$sm_arn)" >&2
    exit 1
fi

echo "bucket: $bucket"
echo "table:  $table"
echo "sm:     $sm_arn"
echo

# --- upload input ---------------------------------------------------------

job_id="sfn-smoke-$(date -u +%Y%m%d-%H%M%S)"
s3_key="jobs/$job_id/input.pdf"
input_uri="s3://$bucket/$s3_key"

echo "uploading → $input_uri"
aws s3 cp "$PDF_PATH" "$input_uri" --region "$REGION" --only-show-errors
echo

# --- start execution ------------------------------------------------------

# Send the PDF itself as the only "page" — Gemini multimodal reads the
# whole PDF in one call, which is the simplest way to exercise the
# chain without a separate render step.
exec_input=$(jq -n \
    --arg job_id "$job_id" \
    --arg input_uri "$input_uri" \
    '{
        job_id:        $job_id,
        input_uri:     $input_uri,
        pages:         [$input_uri],
        first_queue:   "gemini",
        final_stage:   "gemini",
        final_version: "v1"
    }')

exec_arn=$(aws stepfunctions start-execution \
    --state-machine-arn "$sm_arn" \
    --input "$exec_input" \
    --region "$REGION" \
    --query 'executionArn' \
    --output text)

echo "execution: $exec_arn"
echo

# --- poll -----------------------------------------------------------------

echo "polling (timeout 5 min)..."
status="RUNNING"
for i in $(seq 1 30); do
    status=$(aws stepfunctions describe-execution \
        --execution-arn "$exec_arn" \
        --region "$REGION" \
        --query 'status' \
        --output text)
    printf "[%02d] %s\n" "$i" "$status"
    if [[ "$status" != "RUNNING" ]]; then
        break
    fi
    sleep 10
done

echo

# --- report ---------------------------------------------------------------

if [[ "$status" == "SUCCEEDED" ]]; then
    echo "=== SUCCEEDED ==="
    output=$(aws stepfunctions describe-execution \
        --execution-arn "$exec_arn" \
        --region "$REGION" \
        --query 'output' \
        --output text)

    echo "Merge output:"
    echo "$output" | jq '.'

    merged_uri=$(echo "$output" | jq -r '.merged_result_uri // empty')
    if [[ -n "$merged_uri" ]]; then
        merged_key="${merged_uri#s3://*/}"
        merged_key="${merged_key#*/}"
        merged_key="${merged_uri#s3://$bucket/}"
        echo
        echo "Merged result preview (jobs/$job_id/...):"
        # MergedPage.Result is embedded as json.RawMessage and arrives
        # parsed (not as a JSON string), so jq sees a regular object —
        # no fromjson needed.
        aws s3 cp "$merged_uri" - --region "$REGION" 2>/dev/null \
            | jq '. | {job_id, page_count, pages: (.pages | map({page, result_keys: (.result | keys)}))}'
    fi
    exit 0
fi

echo "=== $status ==="
aws stepfunctions describe-execution \
    --execution-arn "$exec_arn" \
    --region "$REGION" \
    --query '{status:status,error:error,cause:cause,startDate:startDate,stopDate:stopDate}' \
    --output json

echo
echo "JobRecord in DynamoDB:"
aws dynamodb get-item \
    --table-name "$table" \
    --region "$REGION" \
    --key "{\"pk\":{\"S\":\"JOB#$job_id\"},\"sk\":{\"S\":\"META\"}}" \
    --output json 2>/dev/null || echo "(no JobRecord written)"

echo
echo "Last 5 SFn history events:"
aws stepfunctions get-execution-history \
    --execution-arn "$exec_arn" \
    --region "$REGION" \
    --reverse-order \
    --max-results 5 \
    --query 'events[].[id,type,stateEnteredEventDetails.name,taskFailedEventDetails.cause]' \
    --output table 2>/dev/null

exit 1
