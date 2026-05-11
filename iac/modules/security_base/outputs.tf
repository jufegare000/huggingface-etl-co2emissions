output "lambda_role_arn" {
  value = aws_iam_role.lambda_default_role.arn
}

output "lambda_role_name" {
  value = aws_iam_role.lambda_default_role.name
}

output "glue_role_arn" {
  value = aws_iam_role.glue_role.arn
}

output "glue_role_name" {
  value = aws_iam_role.glue_role.name
}

output "sfn_role_arn" {
  value = aws_iam_role.sfn_role.arn
}

output "sfn_role_name" {
  value = aws_iam_role.sfn_role.name
}