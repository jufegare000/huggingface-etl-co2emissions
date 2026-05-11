module "kms" {
  source = "../../modules/kms"

  project_name    = var.project_name
  environment     = var.environment
  account_id      = local.account_id
  lambda_role_arn = module.security_base.lambda_role_arn
}