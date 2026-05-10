provider "aws" {
  region  = "us-east-1"
  profile = "expe"
}

variables {
  project_name        = "hf-test"
  environment         = "dev"
  s3_bucket_arn       = "arn:aws:s3:::test-bucket"
  bucket_name         = "test-bucket"
  hf_token_secret_arn = "arn:aws:iam::123456789012:secret:hf-token-dummy"
}

run "validate_lambda_role_trust_policy" {
  command = plan

  assert {
    condition     = aws_iam_role.lambda_default_role.name == "hf-test-lambda-role-dev"
    error_message = "El nombre del rol de Lambda no es el esperado."
  }
}

run "validate_logging_policy_attachment" {
  command = plan

  assert {
    condition     = aws_iam_role_policy_attachment.lambda_logs.role == aws_iam_role.lambda_default_role.name
    error_message = "La política de logs no está asociada al rol de Lambda correcto."
  }

  assert {
    condition     = aws_iam_role_policy_attachment.lambda_logs.policy_arn == "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
    error_message = "El ARN de la política de logs no es el esperado."
  }
}