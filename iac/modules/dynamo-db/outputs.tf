output "table_name" {
  description = "DynamoDB control table name."
  value       = aws_dynamodb_table.this.name
}

output "table_arn" {
  description = "DynamoDB control table ARN."
  value       = aws_dynamodb_table.this.arn
}

output "table_id" {
  description = "DynamoDB control table ID."
  value       = aws_dynamodb_table.this.id
}

output "hash_key" {
  description = "DynamoDB hash key name."
  value       = var.hash_key
}

output "range_key" {
  description = "DynamoDB range key name."
  value       = var.range_key
}