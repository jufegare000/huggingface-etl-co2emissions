
output "dev_bucket_name" {
  description = "hugging-face-etl-data-bucket"
  value       = module.s3_etl_dev.bucket_id
}