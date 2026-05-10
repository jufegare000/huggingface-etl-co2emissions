
provider "aws" {
  region  = "us-east-1"
  profile = "expe"
}

variables {
  project_name = "test-project"
  environment  = "test"
}

run "validate_lambda_role_trust_policy" {
  command = plan

  assert {
    condition     = aws_iam_role.lambda_default_role.name == "test-project-lambda-role-test"
    error_message = "The arn role name does not keep the conventions"
  }

  assert {
    condition     = can(regex("lambda.amazonaws.com", aws_iam_role.lambda_default_role.assume_role_policy))
    error_message = "Trust policy must allow lambda to be assumned"
  }
}

run "validate_logging_policy_attachment" {
  command = plan

  assert {
    condition     = aws_iam_role_policy_attachment.lambda_logs.policy_arn == "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
    error_message = "Role must have poliocy to put logs"
  }
}