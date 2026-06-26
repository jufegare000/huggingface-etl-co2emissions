resource "aws_s3_object" "glue_recuperation_script" {
  bucket = module.s3_etl_dev.bucket_id
  key    = "scripts/data_recuperation.py"
  source = "../../../src/raw_ingestion/infrastructure/into/glue/data_recuperation.py"
  etag   = filemd5("../../../src/raw_ingestion/infrastructure/into/glue/data_recuperation.py")
}

resource "aws_glue_job" "data_recuperation" {
  name     = "${local.project_name}-data-recuperation-${var.environment}"
  role_arn = module.security_base.glue_role_arn

  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 60

  execution_property {
    max_concurrent_runs = 1
  }

  command {
    name            = "glueetl"
    script_location = "s3://${module.s3_etl_dev.bucket_id}/scripts/data_recuperation.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"

    "--additional-python-modules" = "huggingface_hub,requests"
    "--customer-driver-env-vars"  = "CUSTOMER_HF_TOKEN_SECRET_NAME=${module.hf_secrets.secret_arn}"
    "--extra-py-files"            = "s3://${module.s3_etl_dev.bucket_id}/glue-libs/src.zip"
  }

  depends_on = [
    aws_s3_object.glue_recuperation_script,
    aws_s3_object.glue_src_zip,
  ]
}
