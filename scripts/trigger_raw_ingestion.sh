#!/usr/bin/env bash
set -euo pipefail

JOB_NAME="hf-juanfgallo-experiment-data-raw-ingestion-dev"
RUN_ID="20260626021234"
PARTITION_ID="000002"
CONTROL_TABLE_NAME="hf-juanfgallo-experiment-data-dev-enrichment-control"
AWS_PROFILE_ARG=$1

aws glue start-job-run \
  --profile "$AWS_PROFILE_ARG" \
  --job-name "$JOB_NAME" \
  --arguments "{\"--run_id\":\"$RUN_ID\",\"--partition_id\":\"$PARTITION_ID\",\"--control_table_name\":\"$CONTROL_TABLE_NAME\"}"