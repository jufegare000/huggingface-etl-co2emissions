module "workflow" {
  source = "../../modules/workflow"

  project_name = local.project_name
  environment  = var.environment

  sfn_role_arn  = module.security_base.sfn_role_arn
  glue_job_name = "${local.project_name}-raw-ingestion-${var.environment}"
  lambda_arn    = local.lambda_arn_calculated

  output_bucket = module.s3_etl_dev.bucket_id
  hf_token_secret_name = module.hf_secrets.secret_arn
}