variable "project_name" {
  type = string
}

variable "environment" {
  type = string
}

variable "bucket_name" {
  type = string
}

variable "hf_token_secret_arn" {
  type = string
}

variable "lambda_arn" {
  type = string
}

variable "glue_job_arn" {
  type = string
}

variable "glue_role_name" {
  type = string
}

variable "sfn_role_name" {
  type = string
}

variable "lambda_role_name" {
  type = string
}

variable "lambda_env_kms_key_arn" {
  type = string
}