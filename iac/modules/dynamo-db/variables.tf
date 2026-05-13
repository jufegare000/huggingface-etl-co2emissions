variable "table_name" {
  description = "Name of the DynamoDB control table."
  type        = string
}

variable "hash_key" {
  description = "DynamoDB partition key name."
  type        = string
  default     = "PK"
}

variable "range_key" {
  description = "DynamoDB sort key name."
  type        = string
  default     = "SK"
}

variable "billing_mode" {
  description = "DynamoDB billing mode."
  type        = string
  default     = "PAY_PER_REQUEST"

  validation {
    condition     = contains(["PAY_PER_REQUEST", "PROVISIONED"], var.billing_mode)
    error_message = "billing_mode must be either PAY_PER_REQUEST or PROVISIONED."
  }
}

variable "point_in_time_recovery_enabled" {
  description = "Enable DynamoDB point-in-time recovery."
  type        = bool
  default     = true
}

variable "ttl_enabled" {
  description = "Enable TTL for ephemeral records such as rate windows."
  type        = bool
  default     = true
}

variable "ttl_attribute_name" {
  description = "TTL attribute name."
  type        = string
  default     = "ttl"
}

variable "deletion_protection_enabled" {
  description = "Enable deletion protection for the DynamoDB table."
  type        = bool
  default     = true
}

variable "tags" {
  description = "Tags to apply to the DynamoDB table."
  type        = map(string)
  default     = {}
}