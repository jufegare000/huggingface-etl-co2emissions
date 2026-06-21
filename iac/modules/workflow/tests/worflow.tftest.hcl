provider "aws" {
  region  = "us-east-1"
  profile = "expe"
}

variables {
  project_name  = var.project_name
  environment   = "test"
  sfn_role_arn  = "arn:aws:iam::123456789012:role/test-sfn-role"
  lambda_arn    = "arn:aws:lambda:us-east-1:123456789012:function:test-lambda"
  glue_job_name = "test-glue-job"
  output_bucket = "test-bucket"
}

run "validate_state_machine_creation" {
  command = plan

  assert {
    condition     = aws_sfn_state_machine.etl_orchestrator.name == "hf-etl-orchestrator-test"
    error_message = "El nombre de la Step Function es incorrecto."
  }

  assert {
    condition     = contains(keys(jsondecode(aws_sfn_state_machine.etl_orchestrator.definition).States), "DataPreparation")
    error_message = "Falta el estado de preparación de datos."
  }

  assert {
    condition     = contains(keys(jsondecode(aws_sfn_state_machine.etl_orchestrator.definition).States), "IngestionMap")
    error_message = "Falta el estado Map para la paralelización."
  }
}