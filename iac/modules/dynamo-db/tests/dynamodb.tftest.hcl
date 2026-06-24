provider "aws" {
  region  = "us-east-1"
  profile = "expe"
}

variables {
  table_name                     = "test-enrichment-control"
  hash_key                       = "PK"
  range_key                      = "SK"
  billing_mode                   = "PAY_PER_REQUEST"
  point_in_time_recovery_enabled = true
  ttl_enabled                    = true
  ttl_attribute_name             = "ttl"
  deletion_protection_enabled    = true
  tags                           = {}
}

run "validate_table_name" {
  command = plan

  assert {
    condition     = aws_dynamodb_table.this.name == "test-enrichment-control"
    error_message = "DynamoDB table name does not match input variable."
  }
}

run "validate_billing_mode" {
  command = plan

  assert {
    condition     = aws_dynamodb_table.this.billing_mode == "PAY_PER_REQUEST"
    error_message = "Billing mode must be PAY_PER_REQUEST."
  }
}

run "validate_point_in_time_recovery" {
  command = plan

  assert {
    condition     = aws_dynamodb_table.this.point_in_time_recovery[0].enabled == true
    error_message = "Point-in-time recovery must be enabled."
  }
}

run "validate_ttl_config" {
  command = plan

  assert {
    condition     = aws_dynamodb_table.this.ttl[0].enabled == true
    error_message = "TTL must be enabled."
  }

  assert {
    condition     = aws_dynamodb_table.this.ttl[0].attribute_name == "ttl"
    error_message = "TTL attribute name must be 'ttl'."
  }
}

run "validate_deletion_protection" {
  command = plan

  assert {
    condition     = aws_dynamodb_table.this.deletion_protection_enabled == true
    error_message = "Deletion protection must be enabled."
  }
}

run "validate_key_schema" {
  command = plan

  assert {
    condition     = aws_dynamodb_table.this.hash_key == "PK"
    error_message = "Hash key must be 'PK'."
  }

  assert {
    condition     = aws_dynamodb_table.this.range_key == "SK"
    error_message = "Range key must be 'SK'."
  }
}

run "validate_billing_mode_validation_rejects_invalid_value" {
  command = plan

  variables {
    billing_mode = "INVALID_MODE"
  }

  expect_failures = [var.billing_mode]
}
