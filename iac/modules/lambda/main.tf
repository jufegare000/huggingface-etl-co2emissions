data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/../../../${var.source_file_path}"
  output_path = "${path.module}/files/lambda_function.zip"
}

resource "aws_lambda_function" "this" {
  # checkov:skip=CKV_AWS_272: "Code signing is not required for this internal ETL process"
  filename      = data.archive_file.lambda_zip.output_path
  function_name = var.function_name
  role          = var.lambda_role_arn
  handler       = var.handler
  runtime       = "python3.11"
  kms_key_arn   = var.kms_key_arn
  environment {
    variables = var.environment_variables
  }

  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
}