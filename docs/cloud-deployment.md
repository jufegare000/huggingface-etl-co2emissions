# Cloud Deployment

## AWS Services

| Service | Role |
|---------|------|
| **AWS Lambda** | Data preparation config computation (pipeline entry point) |
| **AWS Step Functions** | Pipeline orchestration — sequences Glue jobs |
| **AWS Glue** | ETL execution: raw ingestion, model enrichment, Gold consolidation |
| **Amazon S3** | Bronze, Silver, and Gold dataset storage |
| **Amazon DynamoDB** | Enrichment control table — tracks partition processing state |
| **AWS Secrets Manager** | Hugging Face API token management |
| **AWS KMS** | Encryption key management for S3 and DynamoDB |
| **Amazon CloudWatch** | Logs and monitoring for Lambda and Glue jobs |

## Deployment Flow

```text
git push
  └── make deploy-dev
        ├── terraform plan  (iac/environments/dev)
        └── terraform apply
              ├── packages src/ → Lambda zip artifact
              ├── uploads artifact to S3
              └── updates Lambda function code
```

For code-only changes (no Terraform changes), use the fast path:

```bash
make deploy-lambda-dev
```
