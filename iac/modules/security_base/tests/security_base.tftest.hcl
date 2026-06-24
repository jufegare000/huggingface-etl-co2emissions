provider "aws" {
  region  = "us-east-1"
  profile = "expe"
}

variables {
  project_name               = "hf-test"
  environment                = "test"
  data_bucket_name           = "hf-test-data-test"
  dynamodb_control_table_arn = "arn:aws:dynamodb:us-east-1:123456789012:table/test-table"
}

run "validate_lambda_role_name" {
  command = plan

  assert {
    condition     = aws_iam_role.lambda_default_role.name == "hf-test-lambda-role-test"
    error_message = "Lambda IAM role name does not match expected pattern {project_name}-lambda-role-{environment}."
  }
}

run "validate_lambda_assume_role_policy" {
  command = plan

  assert {
    condition     = jsondecode(aws_iam_role.lambda_default_role.assume_role_policy).Statement[0].Principal.Service == "lambda.amazonaws.com"
    error_message = "Lambda role must allow lambda.amazonaws.com to assume it."
  }

  assert {
    condition     = jsondecode(aws_iam_role.lambda_default_role.assume_role_policy).Statement[0].Action == "sts:AssumeRole"
    error_message = "Lambda role assume-role policy must use sts:AssumeRole."
  }
}

run "validate_glue_role_name" {
  command = plan

  assert {
    condition     = aws_iam_role.glue_role.name == "hf-test-glue-role-test"
    error_message = "Glue IAM role name does not match expected pattern {project_name}-glue-role-{environment}."
  }
}

run "validate_glue_assume_role_policy" {
  command = plan

  assert {
    condition     = jsondecode(aws_iam_role.glue_role.assume_role_policy).Statement[0].Principal.Service == "glue.amazonaws.com"
    error_message = "Glue role must allow glue.amazonaws.com to assume it."
  }
}

run "validate_sfn_role_name" {
  command = plan

  assert {
    condition     = aws_iam_role.sfn_role.name == "hf-test-sfn-role-test"
    error_message = "Step Functions IAM role name does not match expected pattern {project_name}-sfn-role-{environment}."
  }
}

run "validate_sfn_assume_role_policy" {
  command = plan

  assert {
    condition     = jsondecode(aws_iam_role.sfn_role.assume_role_policy).Statement[0].Principal.Service == "states.amazonaws.com"
    error_message = "Step Functions role must allow states.amazonaws.com to assume it."
  }
}

run "validate_lambda_basic_execution_policy_attached" {
  command = plan

  assert {
    condition     = aws_iam_role_policy_attachment.lambda_logs.policy_arn == "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
    error_message = "Lambda role must have AWSLambdaBasicExecutionRole attached."
  }
}

run "validate_glue_service_policy_attached" {
  command = plan

  assert {
    condition     = aws_iam_role_policy_attachment.glue_service.policy_arn == "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
    error_message = "Glue role must have AWSGlueServiceRole attached."
  }
}
