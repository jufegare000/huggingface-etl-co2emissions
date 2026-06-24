provider "aws" {
  region  = "us-east-1"
  profile = "expe"
}

variables {
  project_name = "hf-test"
  environment  = "test"
  secret_name  = "hf-token"
  secret_value = "dummy-token-value"
}

run "validate_secret_name" {
  command = plan

  assert {
    condition     = aws_secretsmanager_secret.this.name == "hf-test-hf-token-test"
    error_message = "Secret name does not match expected pattern {project_name}-{secret_name}-{environment}."
  }
}

run "validate_secret_version_bound_to_secret" {
  command = plan

  assert {
    condition     = aws_secretsmanager_secret_version.this.secret_id == aws_secretsmanager_secret.this.id
    error_message = "Secret version must reference the secret created by this module."
  }
}
