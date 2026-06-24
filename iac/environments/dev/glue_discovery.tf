resource "aws_s3_object" "glue_src_zip" {
  bucket = module.s3_etl_dev.bucket_id
  key    = "glue-libs/src.zip"
  source = "../../../dist/glue_src.zip"
  etag   = filemd5("../../../dist/glue_src.zip")
}

resource "aws_s3_object" "glue_discovery_script" {
  bucket = module.s3_etl_dev.bucket_id
  key    = "scripts/discovery.py"

  source = "../../../src/infrastructure/in/glue/discovery/0_discovery_job.py"
  etag   = filemd5("../../../src/infrastructure/in/glue/discovery/0_discovery_job.py")
}

module "glue_discovery_job" {
  source = "../../modules/glue-discovery"

  project_name = local.project_name
  environment  = var.environment

  s3_bucket_id       = module.s3_etl_dev.bucket_id
  output_bucket_name = module.s3_etl_dev.bucket_id

  glue_role_arn        = module.security_base.glue_role_arn
  script_path          = "scripts/discovery.py"
  hf_token_secret_name = module.hf_secrets.secret_arn
  extra_py_files_path  = "glue-libs/src.zip"

  depends_on = [
    aws_s3_object.glue_discovery_script,
    aws_s3_object.glue_src_zip,
  ]
}