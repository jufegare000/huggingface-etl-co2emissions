output "bucket_id" {
  value = aws_s3_bucket.etl_data_bucket.id
}

output "bucket_arn" {
  value = aws_s3_bucket.etl_data_bucket.arn
}