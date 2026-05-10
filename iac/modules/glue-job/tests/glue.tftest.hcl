provider "aws" {
  region  = "us-east-1"
  profile = "expe"
}

variables {
  project_name  = "hf-etl-test"
  environment   = "test"
  s3_bucket_id  = "test-bucket-id"
  glue_role_arn = "arn:aws:iam::123456789012:role/test-glue-role"
  script_path   = "scripts/raw_ingestion.py"
}

run "validate_glue_job_config" {
  command = plan

  assert {
    condition     = aws_glue_job.huggingface_ingestion.name == "hf-etl-test-raw-ingestion-test"
    error_message = "Glue Job name does not keep the spected pattern"
  }

  assert {
    condition     = aws_glue_job.huggingface_ingestion.command[0].name == "glueetl"
    error_message = "Job must be configured as a SPKAR ETL (gluetl)"
  }

  assert {
    condition     = aws_glue_job.huggingface_ingestion.worker_type == "G.1X"
    error_message = "Worker type must be G.1X."
  }

  assert {
    condition     = contains(keys(aws_glue_job.huggingface_ingestion.default_arguments), "--additional-python-modules")
    error_message = "Additional libraries are missing."
  }
}

run "validate_script_location" {
  command = plan

  assert {
    condition     = aws_glue_job.huggingface_ingestion.command[0].script_location == "s3://test-bucket-id/scripts/raw_ingestion.py"
    error_message = "S3 directory is incorrect"
  }
}