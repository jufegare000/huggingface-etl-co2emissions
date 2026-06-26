#!/usr/bin/env bash
set -euo pipefail

JOB_NAME="hf-juanfgallo-experiment-data-data-recuperation-dev"
RUN_ID="20260626174739"
CONTROL_TABLE_NAME="hf-juanfgallo-experiment-data-dev-enrichment-control"
SOURCE_BUCKET="hf-juanfgallo-experiment-data-data-dev"
AWS_PROFILE_ARG=$1

aws glue start-job-run \
  --profile "$AWS_PROFILE_ARG" \
  --job-name "$JOB_NAME" \
  --arguments "{\"--run_id\":\"$RUN_ID\",\"--control_table_name\":\"$CONTROL_TABLE_NAME\",\"--source_bucket\":\"$SOURCE_BUCKET\"}"
