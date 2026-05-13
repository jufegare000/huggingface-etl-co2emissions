locals {
  aws_region = data.aws_region.current.region
  account_id = data.aws_caller_identity.current.account_id
  project_name = "hf-juanfgallo-experiment-data"

  lambda_arn_calculated = "arn:aws:lambda:${local.aws_region}:${local.account_id}:function:${local.project_name}-data-prep-${var.environment}"

  glue_job_arn_calculated = "arn:aws:glue:${local.aws_region}:${local.account_id}:job/${local.project_name}-raw-ingestion-${var.environment}"
}