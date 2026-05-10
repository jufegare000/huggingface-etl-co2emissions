provider "aws" {
  region  = "us-east-1"
  profile = "expe"
}

variables {
  function_name    = "test-data-prep-lambda"
  source_file_path = "src/infrastructure/in/lambda/data_preparation_job.py"
  handler          = "data_preparation_job.handler"
  lambda_role_arn  = "arn:aws:iam::123456789012:role/test-role" 
  environment_variables = {
    "DEBUG" = "true"
    "TEST"  = "unit-test"
  }
}

run "validate_lambda_config" {
  command = plan

  assert {
    condition     = aws_lambda_function.this.function_name == "test-data-prep-lambda"
    error_message = "Lambda function name does not keep with input varriable"
  }

  assert {
    condition     = aws_lambda_function.this.role == var.lambda_role_arn
    error_message = "Lambda is not using the provided ARN"
  }

  assert {
    condition     = aws_lambda_function.this.environment[0].variables["DEBUG"] == "true"
    error_message = "DEBUG environment variable has not been set."
  }

  assert {
    condition     = aws_lambda_function.this.runtime == "python3.11"
    error_message = "Lambda must run on Python 3.11 runtime."
  }
}
