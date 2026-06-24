provider "aws" {
  region  = "us-east-1"
  profile = "expe"
}

variables {
  project_name  = "hf-etl-test"
  environment   = "test"
  s3_bucket_id  = "test-bucket-id"
  glue_role_arn = "arn:aws:iam::123456789012:role/test-glue-role"
  script_path   = "scripts/gold_consolidation.py"
}

run "validate_job_name" {
  command = plan

  assert {
    condition     = aws_glue_job.gold_consolidation.name == "hf-etl-test-gold-consolidation-test"
    error_message = "Gold consolidation Glue job name does not match expected pattern {project_name}-gold-consolidation-{environment}."
  }
}

run "validate_job_type" {
  command = plan

  assert {
    condition     = aws_glue_job.gold_consolidation.command[0].name == "glueetl"
    error_message = "Job must be configured as a Spark ETL (glueetl)."
  }
}

run "validate_worker_config" {
  command = plan

  assert {
    condition     = aws_glue_job.gold_consolidation.worker_type == "G.1X"
    error_message = "Worker type must be G.1X."
  }

  assert {
    condition     = aws_glue_job.gold_consolidation.number_of_workers == 2
    error_message = "Number of workers must be 2."
  }
}

run "validate_script_location" {
  command = plan

  assert {
    condition     = aws_glue_job.gold_consolidation.command[0].script_location == "s3://test-bucket-id/scripts/gold_consolidation.py"
    error_message = "Script location does not match expected S3 path."
  }
}

run "validate_max_concurrent_runs" {
  command = plan

  assert {
    condition     = aws_glue_job.gold_consolidation.execution_property[0].max_concurrent_runs == 8
    error_message = "Max concurrent runs must be 8 for gold consolidation."
  }
}

run "validate_pandas_dependency" {
  command = plan

  assert {
    condition     = contains(keys(aws_glue_job.gold_consolidation.default_arguments), "--additional-python-modules")
    error_message = "Missing --additional-python-modules argument."
  }

  assert {
    condition     = can(regex("pandas", aws_glue_job.gold_consolidation.default_arguments["--additional-python-modules"]))
    error_message = "pandas must be listed in additional Python modules."
  }
}
