module "s3_etl_dev" {
  source = "../../modules/s3_etl"

  # Pasamos las variables que el módulo pide
  project_name = var.project_name
  environment  = "dev"
}
