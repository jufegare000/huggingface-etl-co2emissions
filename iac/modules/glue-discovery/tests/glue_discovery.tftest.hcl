provider "aws" {
  region  = "us-east-1"
  profile = "expe"
}

variables {
  project_name         = "hf-etl-test"
  environment          = "test"
  s3_bucket_id         = "test-bucket-id"
  output_bucket_name   = "test-output-bucket"
  glue_role_arn        = "arn:aws:iam::123456789012:role/test-glue-role"
  script_path          = "scripts/discovery.py"
  hf_token_secret_name = "hf-token-secret"
}

run "validate_job_name" {
  command = plan

  assert {
    condition     = aws_glue_job.hf_carbon_discovery.name == "hf-etl-test-hf-carbon-discovery-test"
    error_message = "Discovery Glue job name does not match expected pattern {project_name}-hf-carbon-discovery-{environment}."
  }
}

run "validate_job_type" {
  command = plan

  assert {
    condition     = aws_glue_job.hf_carbon_discovery.command[0].name == "glueetl"
    error_message = "Job must be configured as a Spark ETL (glueetl)."
  }
}

run "validate_script_location" {
  command = plan

  assert {
    condition     = aws_glue_job.hf_carbon_discovery.command[0].script_location == "s3://test-bucket-id/scripts/discovery.py"
    error_message = "Script location does not match expected S3 path."
  }
}

run "validate_customer_driver_env_vars" {
  command = plan

  assert {
    condition     = contains(keys(aws_glue_job.hf_carbon_discovery.default_arguments), "--customer-driver-env-vars")
    error_message = "Missing --customer-driver-env-vars argument; bucket and HF token must be injected via driver env vars."
  }

  assert {
    condition     = can(regex("CUSTOMER_TARGET_BUCKET_NAME=test-output-bucket", aws_glue_job.hf_carbon_discovery.default_arguments["--customer-driver-env-vars"]))
    error_message = "CUSTOMER_TARGET_BUCKET_NAME must be set in customer driver env vars."
  }

  assert {
    condition     = can(regex("CUSTOMER_HF_TOKEN_SECRET_NAME=hf-token-secret", aws_glue_job.hf_carbon_discovery.default_arguments["--customer-driver-env-vars"]))
    error_message = "CUSTOMER_HF_TOKEN_SECRET_NAME must be set in customer driver env vars."
  }
}

run "validate_additional_python_modules" {
  command = plan

  assert {
    condition     = contains(keys(aws_glue_job.hf_carbon_discovery.default_arguments), "--additional-python-modules")
    error_message = "Missing --additional-python-modules argument."
  }
}

run "validate_worker_defaults" {
  command = plan

  assert {
    condition     = aws_glue_job.hf_carbon_discovery.worker_type == "G.1X"
    error_message = "Worker type must default to G.1X."
  }

  assert {
    condition     = aws_glue_job.hf_carbon_discovery.number_of_workers == 2
    error_message = "Number of workers must default to 2."
  }
}
