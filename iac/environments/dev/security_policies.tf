module "security_policies" {
  source = "../../modules/security_policies"

  project_name = local.project_name
  environment  = var.environment

  bucket_name         = module.s3_etl_dev.bucket_id
  hf_token_secret_arn = module.hf_secrets.secret_arn
  lambda_arn          = local.lambda_arn_calculated
  glue_job_arn        = local.glue_job_arn_calculated

  lambda_role_name = module.security_base.lambda_role_name
  glue_role_name   = module.security_base.glue_role_name
  sfn_role_name    = module.security_base.sfn_role_name
  dynamodb_control_table_arn = module.dynamodb_enrichment_control.table_arn
  enrichment_glue_job_arn = module.glue_gold_job.enrichment_glue_job_arn
  lambda_env_kms_key_arn = module.kms.key_arn
}

