variable "project_name" {
  type        = string
}

variable "environment" {
  type        = string
}

variable "sfn_role_arn" {
  description = "IAM ARN role for Step Function"
  type        = string
}

variable "lambda_arn" {
  description = "lambda ARN of data preparation"
  type        = string
}

variable "glue_job_name" {
  description = "ingest glue job name"
  type        = string
}

variable "output_bucket" {
  description = "Bucket name for output"
  type        = string
}