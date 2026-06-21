
output "dev_bucket_name" {
  description = "hugging-face-etl-data-bucket"
  value       = module.s3_etl_dev.bucket_id
}

output "dynamodb_enrichment_control_table_name" {
  description = "DynamoDB table used to control enrichment runs and partitions."
  value       = module.dynamodb_enrichment_control.table_name
}

output "dynamodb_enrichment_control_table_arn" {
  description = "DynamoDB control table ARN."
  value       = module.dynamodb_enrichment_control.table_arn
}