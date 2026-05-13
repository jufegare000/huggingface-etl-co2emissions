module "dynamodb_enrichment_control" {
  source = "../../modules/dynamo-db"

  table_name = "${local.project_name}-${var.environment}-enrichment-control"

  hash_key = "PK"
  range_key = "SK"

  billing_mode                    = "PAY_PER_REQUEST"
  point_in_time_recovery_enabled  = true
  ttl_enabled                     = true
  ttl_attribute_name              = "ttl"
  deletion_protection_enabled     = true

  tags = merge(
    local.tags,
    {
      Component = "enrichment-control"
      Service   = "dynamodb"
    }
  )
}