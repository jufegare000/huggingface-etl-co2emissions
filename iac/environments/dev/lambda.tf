module "data_prep_lambda" {
  source = "../../modules/lambda"
  function_name   = "${local.project_name}-data-prep-${var.environment}"
  source_dir_path = "src"
  handler         = "data_preparation/infrastructure/in/lambda/data_preparation_job.handler"
  lambda_role_arn = module.security_base.lambda_role_arn
  kms_key_arn     = module.kms.key_arn
  environment_variables = {
    RAW_BUCKET_NAME    = module.s3_etl_dev.bucket_id
    ENVIRONMENT        = var.environment
    CONTROL_TABLE_NAME = module.dynamodb_enrichment_control.table_name
    CONTROL_TABLE_ARN  = module.dynamodb_enrichment_control.table_arn
  }
}