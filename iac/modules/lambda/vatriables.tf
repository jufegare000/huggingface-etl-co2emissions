variable "function_name" { type = string }
variable "source_file_path" { type = string }
variable "handler" { 
    type = string
    default = "data_preparation_job.handler" 
}
variable "environment_variables" {
  type    = map(string)
  default = {}
}

variable "lambda_role_arn" { type = string }