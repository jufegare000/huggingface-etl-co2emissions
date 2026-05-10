module "s3_etl_dev" {
  source = "../../modules/s3_etl"
  project_name = var.project_name
  environment  = "dev"
  lambda_role_arn =  module.security.lambda_role_arn
}

module "security" {
  source       = "../../modules/security"
  project_name = var.project_name
  environment  = var.environment
}


module "data_prep_lambda" {
  source           = "../../modules/lambda"
  function_name    = "${var.project_name}-data-prep-${var.environment}"
  source_file_path = "src/infrastructure/in/lambda/data_preparation_job.py"
  lambda_role_arn  = module.security.lambda_role_arn
  
  environment_variables = {
    RAW_BUCKET_NAME = module.s3_etl_dev.bucket_id
    ENVIRONMENT     = var.environment
  }
}