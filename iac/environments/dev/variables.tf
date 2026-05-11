variable "hf_token" {
  description = "de Hugging Face Token (environment variable)"
  type        = string
  sensitive   = true
}

variable "aws_region" {
  description = "AWS deployment region"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Hugging Face Experiment Project"
  type        = string
}

variable "environment" {
  description = "(dev, qa, prod)"
  type        = string
  default     = "dev"
}