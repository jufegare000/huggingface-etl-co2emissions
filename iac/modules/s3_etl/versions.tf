# terraform/modules/s3_etl/versions.tf

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 4.0" 
      }
  }
}