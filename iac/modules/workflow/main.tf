resource "aws_sfn_state_machine" "etl_orchestrator" {
  name     = "${var.project_name}-orchestrator-${var.environment}"
  role_arn = var.sfn_role_arn

  definition = jsonencode({
    StartAt = "DataPreparation",
    States = {
      "DataPreparation" = {
        Type       = "Task",
        Resource   = var.lambda_arn,
        ResultPath = "$",
        Next       = "IngestionMap"
      },

      "IngestionMap" = {
        Type           = "Map",
        ItemsPath      = "$.partitions",
        MaxConcurrency = 5,

        Parameters = {
          "run_id.$"             = "$.run_id",
          "control_table_name.$" = "$.control_table_name",
          "bucket_name.$"        = "$.bucket_name",
          "partition.$"          = "$$.Map.Item.Value"
        },

        Iterator = {
          StartAt = "GlueIngestion",
          States = {
            "GlueIngestion" = {
              Type     = "Task",
              Resource = "arn:aws:states:::glue:startJobRun.sync",
              Parameters = {
                "JobName" = var.glue_job_name,
                "Arguments" = {
                  "--run_id.$"             = "$.run_id",
                  "--partition_id.$"       = "$.partition.partition_id",
                  "--control_table_name.$" = "$.control_table_name",
                  "--hf_token_secret_name" = var.hf_token_secret_name
                }
              },
              End = true
            }
          }
        },

        ResultPath = "$.ingestion_results",
        Next       = "GlueEnrichment"
      },

      "GlueEnrichment" = {
        Type     = "Task",
        Resource = "arn:aws:states:::glue:startJobRun.sync",
        Parameters = {
          "JobName" = var.enrichment_glue_job_name,
          "Arguments" = {
            "--run_id.$"             = "$.run_id",
            "--control_table_name.$" = "$.control_table_name",
            "--source_bucket.$"      = "$.bucket_name",
            "--target_bucket.$"      = "$.bucket_name"
          }
        },
        End = true
      }
    }
  })
}