variable "project_name" {
  description = "Project name used as resource prefix."
  type        = string
}

variable "environment" {
  description = "Deployment environment, for example dev, staging or prod."
  type        = string
}

variable "s3_bucket_id" {
  description = "S3 bucket where the Glue discovery script is stored."
  type        = string
}

variable "output_bucket_name" {
  description = "S3 bucket where the discovery output will be written."
  type        = string
}

variable "glue_role_arn" {
  description = "IAM role ARN used by the Glue discovery job."
  type        = string
}

variable "script_path" {
  description = "S3 key of the Glue discovery script."
  type        = string
  default     = "scripts/discovery.py"
}

variable "hf_token_secret_name" {
  description = "Name or ARN of the Secrets Manager secret containing the Hugging Face token."
  type        = string
}

variable "worker_type" {
  description = "Glue worker type."
  type        = string
  default     = "G.1X"
}

variable "number_of_workers" {
  description = "Number of Glue workers."
  type        = number
  default     = 2
}

variable "timeout" {
  description = "Glue job timeout in minutes."
  type        = number
  default     = 240
}

variable "max_concurrent_runs" {
  description = "Maximum concurrent runs for this Glue job."
  type        = number
  default     = 1
}

variable "python_version" {
  description = "Python version used by the Glue job."
  type        = string
  default     = "3"
}

variable "additional_python_modules" {
  description = "Comma-separated Python modules installed by Glue."
  type        = string
  default     =  "requests,boto3,botocore"
}