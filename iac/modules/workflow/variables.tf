variable "project_name" {
  type = string
}

variable "environment" {
  type = string
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

variable "hf_token_secret_name" {
  type = string
}

variable "enrichment_glue_job_name" {
  type        = string
  description = "Glue job name for enrichment / gold consolidation"
}