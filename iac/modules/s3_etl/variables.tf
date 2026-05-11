variable "project_name" {
  description = "Name of the ETL project"
  type        = string
}

variable "lambda_role_arn" {
  description = "Lambda role ARN which has to be allowed for read operations"
  type        = string
}

variable "environment" {
  description = "(dev, qa, prod)"
  type        = string
}