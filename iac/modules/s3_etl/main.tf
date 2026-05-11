resource "aws_s3_bucket" "etl_data_bucket" {
  bucket = "${var.project_name}-data-${var.environment}"

  tags = {
    Name        = "Data ETL Bucket"
    Environment = var.environment
    Project     = var.project_name
    ManagedBy   = "Terraform"
  }
}

resource "aws_s3_bucket_versioning" "etl_data_bucket_versioning" {
  bucket = aws_s3_bucket.etl_data_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}