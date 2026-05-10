variable "project_name" { type = string }
variable "environment" { type = string }

variable "bucket_name" {
  description = "S3 Bucket name for gluejobs permissions"
  type        = string
}

variable "hf_token_secret_arn" {
  type = string
}