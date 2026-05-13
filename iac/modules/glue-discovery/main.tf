resource "aws_glue_job" "hf_carbon_discovery" {
  name     = "${var.project_name}-hf-carbon-discovery-${var.environment}"
  role_arn = var.glue_role_arn

  worker_type       = var.worker_type
  number_of_workers = var.number_of_workers
  timeout           = var.timeout

  execution_property {
    max_concurrent_runs = var.max_concurrent_runs
  }

  command {
    name            = "glueetl"
    script_location = "s3://${var.s3_bucket_id}/${var.script_path}"
    python_version  = var.python_version
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"

    "--additional-python-modules" = var.additional_python_modules

    "--customer-driver-env-vars" = join(",", [
      "CUSTOMER_TARGET_BUCKET_NAME=${var.output_bucket_name}",
      "CUSTOMER_HF_TOKEN_SECRET_NAME=${var.hf_token_secret_name}"
    ])
  }

  tags = {
    Project     = var.project_name
    Environment = var.environment
    Component   = "hf-carbon-discovery"
  }
}