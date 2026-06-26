module "glue_ingestion_job" {
  source = "../../modules/glue-job"

  project_name = local.project_name
  environment  = var.environment

  s3_bucket_id         = module.s3_etl_dev.bucket_id
  glue_role_arn        = module.security_base.glue_role_arn
  script_path          = "scripts/raw_ingestion.py"
  hf_token_secret_name = module.hf_secrets.secret_arn
  extra_py_files_path  = "glue-libs/src.zip"

  depends_on = [
    aws_s3_object.glue_script,
    aws_s3_object.glue_src_zip,
  ]
}

resource "aws_s3_object" "glue_script" {
  bucket = module.s3_etl_dev.bucket_id
  key    = "scripts/raw_ingestion.py"

  source = "../../../src/raw_ingestion/infrastructure/into/glue/raw_ingestion.py"
  etag   = filemd5("../../../src/raw_ingestion/infrastructure/into/glue/raw_ingestion.py")
}