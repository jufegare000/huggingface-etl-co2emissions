variables {
  project_name        = "hf-test"
  environment         = "dev"
  s3_bucket_arn       = "arn:aws:s3:::test-bucket"
  bucket_name         = "test-bucket"
  hf_token_secret_arn = "arn:aws:secretsmanager:us-east-1:123456789012:secret:hf-token-abc"
  
  lambda_arn          = "arn:aws:lambda:us-east-1:123456789012:function:test-lambda"
  glue_job_arn        = "arn:aws:glue:us-east-1:123456789012:job/test-glue-job"
}