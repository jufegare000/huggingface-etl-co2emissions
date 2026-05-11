resource "aws_s3_bucket_policy" "allow_lambda_read" {
  bucket = aws_s3_bucket.etl_data_bucket.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowLambdaReadAccess"
        Effect = "Allow"
        Principal = {
          AWS = var.lambda_role_arn
        }
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.etl_data_bucket.arn,
          "${aws_s3_bucket.etl_data_bucket.arn}/*"
        ]
      }
    ]
  })
}