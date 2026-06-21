resource "aws_glue_job" "gold_consolidation" {
  name     = "${var.project_name}-gold-consolidation-${var.environment}"
  role_arn = var.glue_role_arn

  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 20

  execution_property {
    max_concurrent_runs = 8
  }

  command {
    name            = "glueetl"
    script_location = "s3://${var.s3_bucket_id}/scripts/gold_consolidation.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"
    "--output_bucket"                    = var.s3_bucket_id

    "--additional-python-modules" = "pandas"

  }
}