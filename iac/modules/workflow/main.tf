resource "aws_sfn_state_machine" "etl_orchestrator" {
  name     = "${var.project_name}-orchestrator-${var.environment}"
  role_arn = var.sfn_role_arn

  definition = jsonencode({
    StartAt = "DataPreparation",
    States = {
      "DataPreparation" = {
        Type     = "Task",
        Resource = var.lambda_arn,
        ResultPath = "$", 
        Next     = "IngestionMap"
      },
      "IngestionMap" = {
        Type        = "Map",
        ItemsPath   = "$.partitions",
        MaxConcurrency = 5,
        Parameters = {
          "partition.$"   = "$$.Map.Item.Value",
          "bucket_name.$" = "$.bucket_name"
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
                  "--partition_id.$" = "States.Format('{}', $.partition.partition_id)",
                  "--emission_min.$" = "States.Format('{}', $.partition.emission_min)",
                  "--emission_max.$" = "States.Format('{}', $.partition.emission_max)",
                  "--output_bucket.$" = "$.bucket_name"
                }
              },
              End = true
            }
          }
        },
        End = true
      }
    }
  })
}