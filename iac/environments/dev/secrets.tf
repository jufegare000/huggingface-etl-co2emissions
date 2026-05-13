module "hf_secrets" {
  source = "../../modules/secrets"

  project_name = local.project_name
  environment  = var.environment
  secret_name  = "hf-token"
  secret_value = var.hf_token
}