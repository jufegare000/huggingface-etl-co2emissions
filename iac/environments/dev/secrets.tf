module "hf_secrets" {
  source = "../../modules/secrets"

  project_name = local.project_name
  environment  = var.environment
  secret_name  = "hf-token-jf"
  secret_value = var.hf_token
}