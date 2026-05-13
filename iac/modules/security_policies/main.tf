resource "aws_iam_role_policy" "glue_s3_access" {
  name = "${var.project_name}-glue-s3-policy-${var.environment}"
  role = var.glue_role_name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
        Resource = [
          "arn:aws:s3:::${var.bucket_name}",
          "arn:aws:s3:::${var.bucket_name}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy" "glue_secrets_policy" {
  name = "${var.project_name}-glue-secrets-policy-${var.environment}"
  role = var.glue_role_name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = [var.hf_token_secret_arn]
      }
    ]
  })
}

resource "aws_iam_role_policy" "sfn_policy" {
  name = "${var.project_name}-sfn-policy-${var.environment}"
  role = var.sfn_role_name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "lambda:InvokeFunction"
        Resource = [var.lambda_arn]
      },
      {
        Effect   = "Allow"
        Action   = ["glue:StartJobRun", "glue:GetJobRun"]
        Resource = [var.glue_job_arn]
      }
    ]
  })
}

resource "aws_iam_role_policy" "lambda_kms_decrypt" {
  name = "${var.project_name}-lambda-kms-decrypt-${var.environment}"
  role = var.lambda_role_name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:DescribeKey"
        ]
        Resource = [
          var.lambda_env_kms_key_arn
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy" "glue_dynamodb_control_policy" {
  name = "${var.project_name}-glue-dynamodb-control-${var.environment}"
  role = var.glue_role_name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowGlueReadAndUpdateDynamoDbControlTable"
        Effect = "Allow"
        Action = [
          "dynamodb:GetItem",
          "dynamodb:UpdateItem",
          "dynamodb:PutItem"
        ]
        Resource = var.dynamodb_control_table_arn
      }
    ]
  })
}

resource "aws_iam_role_policy" "lambda_data_preparation_s3_policy" {
  name = "${var.project_name}-lambda-data-preparation-s3-${var.environment}"
  role = var.lambda_role_name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
        Resource = [
          "arn:aws:s3:::${var.bucket_name}",
          "arn:aws:s3:::${var.bucket_name}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy" "lambda_dynamodb_control_policy" {
  name = "${var.project_name}-lambda-dynamodb-control-${var.environment}"
  role = var.lambda_role_name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowLambdaReadAndUpdateDynamoDbControlTable"
        Effect = "Allow"
        Action = [
          "dynamodb:GetItem",
          "dynamodb:UpdateItem",
          "dynamodb:PutItem",
          "dynamodb:BatchWriteItem"
        ]
        Resource = var.dynamodb_control_table_arn
      }
    ]
  })
}
