provider "aws" {
  region  = "us-east-1"
  profile = "expe"
}

variables {
  project_name               = "hf-test"
  environment                = "test"
  bucket_name                = "hf-test-data-test"
  hf_token_secret_arn        = "arn:aws:secretsmanager:us-east-1:123456789012:secret:hf-token-abc123"
  lambda_arn                 = "arn:aws:lambda:us-east-1:123456789012:function:hf-test-data-prep-test"
  glue_job_arn               = "arn:aws:glue:us-east-1:123456789012:job/hf-test-raw-ingestion-test"
  enrichment_glue_job_arn    = "arn:aws:glue:us-east-1:123456789012:job/hf-test-gold-consolidation-test"
  glue_role_name             = "hf-test-glue-role-test"
  sfn_role_name              = "hf-test-sfn-role-test"
  lambda_role_name           = "hf-test-lambda-role-test"
  lambda_env_kms_key_arn     = "arn:aws:kms:us-east-1:123456789012:key/test-key-id"
  dynamodb_control_table_arn = "arn:aws:dynamodb:us-east-1:123456789012:table/hf-test-test-enrichment-control"
}

run "validate_glue_s3_policy_allows_required_actions" {
  command = plan

  assert {
    condition = contains(
      jsondecode(aws_iam_role_policy.glue_s3_access.policy).Statement[0].Action,
      "s3:GetObject"
    )
    error_message = "Glue S3 policy must allow s3:GetObject."
  }

  assert {
    condition = contains(
      jsondecode(aws_iam_role_policy.glue_s3_access.policy).Statement[0].Action,
      "s3:PutObject"
    )
    error_message = "Glue S3 policy must allow s3:PutObject."
  }
}

run "validate_glue_secrets_policy" {
  command = plan

  assert {
    condition = contains(
      jsondecode(aws_iam_role_policy.glue_secrets_policy.policy).Statement[0].Action,
      "secretsmanager:GetSecretValue"
    )
    error_message = "Glue secrets policy must allow secretsmanager:GetSecretValue."
  }

  assert {
    condition     = jsondecode(aws_iam_role_policy.glue_secrets_policy.policy).Statement[0].Resource[0] == var.hf_token_secret_arn
    error_message = "Glue secrets policy must scope resource to the HF token secret ARN."
  }
}

run "validate_sfn_policy_allows_lambda_invoke" {
  command = plan

  assert {
    condition = anytrue([
      for s in jsondecode(aws_iam_role_policy.sfn_policy.policy).Statement :
      s.Action == "lambda:InvokeFunction"
    ])
    error_message = "Step Functions policy must allow lambda:InvokeFunction."
  }
}

run "validate_sfn_policy_allows_glue_start_job" {
  command = plan

  assert {
    condition = anytrue([
      for s in jsondecode(aws_iam_role_policy.sfn_policy.policy).Statement :
      contains(tolist(s.Action), "glue:StartJobRun")
    ])
    error_message = "Step Functions policy must allow glue:StartJobRun."
  }
}

run "validate_lambda_kms_policy_actions" {
  command = plan

  assert {
    condition = contains(
      jsondecode(aws_iam_role_policy.lambda_kms_decrypt.policy).Statement[0].Action,
      "kms:Decrypt"
    )
    error_message = "Lambda KMS policy must allow kms:Decrypt."
  }

  assert {
    condition = contains(
      jsondecode(aws_iam_role_policy.lambda_kms_decrypt.policy).Statement[0].Action,
      "kms:DescribeKey"
    )
    error_message = "Lambda KMS policy must allow kms:DescribeKey."
  }
}

run "validate_lambda_dynamodb_policy_allows_batch_write" {
  command = plan

  assert {
    condition = contains(
      jsondecode(aws_iam_role_policy.lambda_dynamodb_control_policy.policy).Statement[0].Action,
      "dynamodb:BatchWriteItem"
    )
    error_message = "Lambda DynamoDB policy must allow dynamodb:BatchWriteItem."
  }
}

run "validate_glue_dynamodb_policy_allows_query" {
  command = plan

  assert {
    condition = contains(
      jsondecode(aws_iam_role_policy.glue_dynamodb_control_policy.policy).Statement[0].Action,
      "dynamodb:Query"
    )
    error_message = "Glue DynamoDB policy must allow dynamodb:Query."
  }
}
