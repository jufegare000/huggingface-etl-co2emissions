
provider "aws" {
  region  = "us-east-1"
  profile = "expe"
}

variables {
  project_name = "hugging-face-project"
  environment  = "test"
}



run "validate_bucket_name" {
  command = plan

  assert {
    condition     = aws_s3_bucket.etl_data_bucket.bucket == "hugging-face-project-data-test"
    error_message = "Bucket name not found"
  }
}

run "validate_versioning_enabled" {
  command = plan

  assert {
    condition     = aws_s3_bucket_versioning.etl_data_bucket_versioning.versioning_configuration[0].status == "Enabled"
    error_message = "Versioning is disabled by default"
  }
}