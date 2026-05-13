resource "aws_s3_object" "glue_discovery_script" {
  bucket = module.s3_etl_dev.bucket_id
  key    = "scripts/discovery.py"

  source = "../../../src/infrastructure/in/glue/discovery.py"
  etag   = filemd5("../../../src/infrastructure/in/glue/discovery.py")
}

module "glue_discovery_job" {
  source = "../../modules/glue-discovery"

  project_name = var.project_name
  environment  = var.environment

  s3_bucket_id       = module.s3_etl_dev.bucket_id
  output_bucket_name = module.s3_etl_dev.bucket_id

  glue_role_arn        = module.security_base.glue_role_arn
  script_path          = "scripts/discovery.py"
  hf_token_secret_name = module.hf_secrets.secret_arn

  depends_on = [
    aws_s3_object.glue_discovery_script
  ]
}