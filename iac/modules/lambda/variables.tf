variable "function_name" { type = string }
variable "source_dir_path" { type = string }
variable "handler" {
  type    = string
  default = "data_preparation_job.handler"
}
variable "environment_variables" {
  type    = map(string)
  default = {}
}
variable "lambda_role_arn" { type = string }
variable "kms_key_arn" {
  type    = string
  default = null
}
variable "runtime" {
  type    = string
  default = "python3.13"
}
variable "timeout" {
  type    = number
  default = 60
}