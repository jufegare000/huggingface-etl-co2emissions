output "key_arn" {
  value = aws_kms_key.lambda_env.arn
}

output "key_id" {
  value = aws_kms_key.lambda_env.key_id
}

output "alias_name" {
  value = aws_kms_alias.lambda_env.name
}