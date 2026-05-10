module "s3_etl_dev" {
  source = "../../modules/s3_etl"
  project_name = var.project_name
  environment  = "dev"
  lambda_role_arn =  module.security.lambda_role_arn
}

module "security" {
  source        = "../../modules/security"
  project_name  = var.project_name
  environment   = var.environment
  bucket_name   = module.s3_etl_dev.bucket_id 
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

module "glue_ingestion_job" {
  source        = "../../modules/glue-job"
  project_name  = var.project_name
  environment   = var.environment
  
  s3_bucket_id  = module.s3_etl_dev.bucket_id
  glue_role_arn = module.security.glue_role_arn
  script_path   = "scripts/raw_ingestion.py"

}
resource "aws_s3_object" "glue_script" {
  bucket = module.s3_etl_dev.bucket_id
  key    = "scripts/raw_ingestion.py"
  
  source = "../../../src/infrastructure/in/glue/raw_ingestion.py"
  etag   = filemd5("../../../src/infrastructure/in/glue/raw_ingestion.py")
}

module "hf_secrets" {
  source       = "../../modules/secrets"
  project_name = var.project_name
  environment  = var.environment
  secret_name  = "hf-token"
  secret_value = var.hf_token
}