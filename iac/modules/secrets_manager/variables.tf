variable "project_name" {
  type = string
}

variable "environment" {
  type = string
}

variable "secret_name" {
  type = string
}

variable "secret_value" {
  type      = string
  sensitive = true
}