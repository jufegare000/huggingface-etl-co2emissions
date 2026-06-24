# Lambda – Data Preparation Job

## Overview

The Lambda function at `src/infrastructure/in/lambda/data_preparation_job.py` is the pipeline entry point. It is a **thin handler** that delegates entirely to the use case:

```python
def handler(_event, _context):
    return data_preparation_config_use_case.create_configuration_for_etl()
```

All dependencies are resolved at module load time in `config/injection/dependency_injector.py` via explicit constructor injection — no service locator or global state.

## Handler Path (Terraform)

```
infrastructure/in/lambda/data_preparation_job.handler
```

## Environment Variables

| Variable | Description |
|----------|-------------|
| `RAW_BUCKET_NAME` | S3 bucket holding raw and processed data |
| `ENVIRONMENT` | Deployment environment (e.g. `dev`) |
| `CONTROL_TABLE_NAME` | DynamoDB table name for enrichment control |
| `CONTROL_TABLE_ARN` | DynamoDB table ARN for enrichment control |

## Dependency Injection

The `dependency_injector.py` module wires all concrete implementations to their interfaces at cold-start time. This keeps the handler free of construction logic and makes each service independently testable.

```text
dependency_injector.py
  └── resolves env vars
  └── instantiates DynamoDB / S3 / SecretsManager adapters
  └── constructs service implementations
  └── builds and exposes: data_preparation_config_use_case
```
