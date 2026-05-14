module "security_base" {
  source = "../../modules/security_base"

  project_name = local.project_name
  environment  = var.environment
  data_bucket_name            = "${local.project_name}-data-${var.environment}"
  dynamodb_control_table_arn  = module.dynamodb_enrichment_control.table_arn

}