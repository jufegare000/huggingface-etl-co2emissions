module "s3_etl_dev" {
  source          = "../../modules/s3_etl"
  project_name    = local.project_name
  environment     = var.environment
  lambda_role_arn = module.security_base.lambda_role_arn
}