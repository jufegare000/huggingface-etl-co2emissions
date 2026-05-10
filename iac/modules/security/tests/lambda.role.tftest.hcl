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
  lambda_arn          = "arn:aws:lambda:us-east-1:123456789012:function:test-lambda"
  glue_job_arn        = "arn:aws:glue:us-east-1:123456789012:job/test-glue-job"
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

run "validate_sfn_role_and_policy" {
  command = plan

  assert {
    condition     = aws_iam_role.sfn_role.name == "hf-test-sfn-role-dev"
    error_message = "El nombre del rol de Step Functions es incorrecto."
  }

  assert {
    condition     = aws_iam_role_policy.sfn_policy.name == "hf-test-sfn-policy-dev"
    error_message = "La política de orquestación de Step Functions no tiene el nombre esperado."
  }
}