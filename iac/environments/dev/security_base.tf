module "security_base" {
  source = "../../modules/security_base"

  project_name = var.project_name
  environment  = var.environment
}