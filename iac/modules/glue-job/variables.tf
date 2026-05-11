variable "project_name" {
  type = string
}

variable "environment" {
  type = string
}

variable "s3_bucket_id" {
  type = string
}

variable "glue_role_arn" {
  type = string
}

variable "script_path" {
  type    = string
  default = "scripts/raw_ingestion.py"
}

variable "max_retries" {
  type    = number
  default = 0
} 

variable "hf_token_secret_name" {
  type = string
}