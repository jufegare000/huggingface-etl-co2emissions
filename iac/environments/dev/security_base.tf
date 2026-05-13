module "security_base" {
  source = "../../modules/security_base"

  project_name = local.project_name
  environment  = var.environment
}