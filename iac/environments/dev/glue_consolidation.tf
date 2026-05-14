module "glue_gold_job" {
  source = "../../modules/glue_enrichment"

  project_name = local.project_name
  environment  = var.environment

  s3_bucket_id  = module.s3_etl_dev.bucket_id
  glue_role_arn = module.security_base.glue_role_arn
  script_path   = "scripts/gold_consolidation.py"
}

resource "aws_s3_object" "glue_gold_script" {
  bucket = module.s3_etl_dev.bucket_id
  key    = "scripts/gold_consolidation.py"

  source = "../../../src/infrastructure/in/glue/gold_consolidation.py"
  etag   = filemd5("../../../src/infrastructure/in/glue/gold_consolidation.py")
}