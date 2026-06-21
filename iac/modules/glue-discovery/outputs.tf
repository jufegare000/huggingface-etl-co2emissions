output "job_name" {
  description = "Glue discovery job name."
  value       = aws_glue_job.hf_carbon_discovery.name
}

output "job_arn" {
  description = "Glue discovery job ARN."
  value       = aws_glue_job.hf_carbon_discovery.arn
}