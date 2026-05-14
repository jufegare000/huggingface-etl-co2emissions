output "enrichment_glue_job_name" {
  description = "Name of the Glue job used for enrichment / gold consolidation."
  value       = aws_glue_job.gold_consolidation.name
}

output "enrichment_glue_job_arn" {
  description = "ARN of the Glue job used for enrichment / gold consolidation."
  value       = aws_glue_job.gold_consolidation.arn
}