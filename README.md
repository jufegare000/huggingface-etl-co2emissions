# huggingface-etl-co3emissions

A reproducible, fault-tolerant ETL pipeline built on AWS Lambda, AWS Glue, and PySpark to extract, enrich, and consolidate Hugging Face model metadata into a curated analytical dataset for large-scale sustainability and performance analysis.

## Overview

This project builds a production-oriented data pipeline to collect and enrich metadata from Hugging Face models that report CO2 emissions. The pipeline is designed to support reproducibility, incremental execution, fault recovery, and analytical consumption in the cloud.

The architecture follows a layered approach:

- **Bronze**: raw snapshot of Hugging Face model metadata
- **Silver Models**: enriched model-level metadata
- **Silver Datasets**: enriched dataset-level metadata
- **Gold**: curated analytical dataset ready for querying and visualization

## Objectives

- Extract Hugging Face models with reported CO2 emissions
- Enrich model metadata with model size, library, domain, training metadata, and performance information
- Enrich referenced datasets with dataset-level metadata such as dataset size
- Build a curated final dataset for analysis
- Ensure the pipeline is reproducible, fault-tolerant, and suitable for cloud deployment

## Architecture

```text
Hugging Face API
        |
        v
   Raw Ingestion (Glue)
        |
        v
 Bronze Snapshot (S3)
    /        \
   v          v
Model        Dataset
Discovery    Enrichment
(Glue)       (Glue)
   |          |
   v          v
Silver       Silver
Models       Datasets
    \        /
     v      v
 Gold Consolidation (Glue)
        |
        v
 Gold Curated Dataset (S3)
        |
        v
Analytics / Visualization
```

The **Data Preparation Lambda** sits before the pipeline. It is triggered to compute the ETL configuration (date boundaries, partition descriptors, input manifest) and pass it to AWS Step Functions, which orchestrates the Glue jobs.

## Code Architecture

The codebase follows a **hexagonal (ports and adapters) architecture** with three layers:

```text
domain/          ← interfaces, domain models, repository contracts
application/     ← service and use-case implementations (pure business logic)
infrastructure/  ← AWS adapters (Lambda handler, Glue entrypoints, DynamoDB, S3, HF client)
```

- **Domain layer** defines service and repository interfaces as abstract classes. It contains no AWS SDK calls.
- **Application layer** implements those interfaces with pure business logic.
- **Infrastructure layer** wires everything together: inbound handlers (Lambda, Glue) and outbound adapters (DynamoDB, S3, Secrets Manager, Hugging Face).

## Key Features

- Modular, testable hexagonal architecture separating domain, application, and infrastructure concerns
- Thin Lambda handler — only parses the event, resolves dependencies, and delegates to the use case
- Dependency injection via a single `dependency_injector.py` module
- Reproducible execution through parameterized jobs and versioned snapshots
- Fault tolerance with checkpoints, retries, and recoverable intermediate outputs
- Incremental processing to avoid unnecessary reprocessing
- PySpark-based transformations for scalable joins, aggregation, normalization, and curation
- Cloud-native deployment using AWS Lambda, AWS Glue, S3, DynamoDB, and Step Functions
- **Modular Infrastructure as Code (IaC)** using Terraform with native unit testing

## Data Layers

### Bronze

Raw snapshot of Hugging Face models reporting CO2 emissions.

Typical fields:
- `model_id`
- `co2_eq_emissions`
- `downloads`
- `likes`
- `pipeline_tag`
- `library_name`
- `datasets`
- `created_at`

### Silver Models

Model-level enrichment.

Typical fields:
- `model_id`
- `model_size_mb`
- `is_autotrain`
- `training_type`
- `geographical_location`
- `hardware_used`
- `performance_metrics`

### Silver Datasets

Dataset-level enrichment.

Typical fields:
- `dataset_id`
- `dataset_size`
- optional dataset metadata

### Gold

Final curated analytical dataset combining model and dataset enrichment.

Typical fields:
- `model_id`
- `datasets`
- `datasets_size`
- `co2_eq_emissions`
- `source`
- `training_type`
- `geographical_location`
- `environment`
- `performance_metrics`
- `downloads`
- `likes`
- `library_name`
- `domain`
- `size`
- `created_at`
- `auto`

## Project Structure

```text
final/
├── README.md
├── CLAUDE.md
├── Makefile                          ← Task orchestration (tests, deploys)
├── makefiles/
│   ├── tests.mk                      ← Terraform module tests (parallel)
│   └── deploys.mk                    ← Terraform plan/apply/destroy targets
├── pyproject.toml
├── requirements.txt
├── requirements-dev.txt
├── src/
│   ├── domain/
│   │   ├── shared/
│   │   │   └── models/
│   │   │       └── serializable_model.py
│   │   └── data_preparation/
│   │       ├── models/preparation/   ← boundary, manifest, partition, metadata models
│   │       ├── models/s3/            ← bucket URI model
│   │       ├── persistence/          ← DataPreparationRepository interface
│   │       ├── services/             ← service interfaces (config, dates, metadata, s3, step functions)
│   │       └── use_cases/            ← DataPreparationConfigUseCase interface
│   ├── application/
│   │   └── data_preparation/
│   │       ├── services/             ← service implementations
│   │       └── use_cases/            ← DataPreparationConfigUseCaseImplemented
│   └── infrastructure/
│       ├── in/
│       │   ├── lambda/
│       │   │   ├── data_preparation_job.py        ← Lambda handler (entrypoint)
│       │   │   └── config/injection/
│       │   │       └── dependency_injector.py     ← wires all dependencies at module load
│       │   └── glue/
│       │       ├── raw_ingestion.py
│       │       ├── discovery.py
│       │       └── gold_consolidation.py
│       └── out/
│           ├── dynamo/               ← DynamoDB client, mappers, repository, type conversion
│           ├── s3/                   ← S3 client, writer
│           ├── hugging_face/         ← HuggingFace API client
│           └── secrets_config/       ← HF token resolver via Secrets Manager
├── tests/
│   ├── application/data_preparation/services/
│   │   ├── config/boundaries/
│   │   ├── config/environment/
│   │   ├── config/lambda_function/
│   │   ├── config/partitions/
│   │   ├── dates/
│   │   ├── metadata/ai_metadata_models/
│   │   └── s3/
│   ├── domain/shared/models/
│   └── infrastructure/
│       ├── in/glue/
│       └── out/dynamo/
└── iac/
    ├── environments/
    │   └── dev/                      ← dev environment instantiating all modules
    └── modules/
        ├── lambda/                   ← packages src/ and deploys the Lambda
        ├── dynamo-db/
        ├── glue-job/
        ├── glue-discovery/
        ├── glue_enrichment/
        ├── s3_etl/
        ├── secrets/
        ├── kms/
        ├── security_base/
        ├── security_policies/
        └── workflow/
```

## Lambda: Data Preparation Job

The Lambda function at `src/infrastructure/in/lambda/data_preparation_job.py` is the pipeline entry point. It is a **thin handler** that delegates entirely to the use case:

```python
def handler(_event, _context):
    return data_preparation_config_use_case.create_configuration_for_etl()
```

All dependencies are resolved at module load time in `config/injection/dependency_injector.py` via explicit constructor injection — no service locator or global state.

### Handler path (Terraform)

```
infrastructure/in/lambda/data_preparation_job.handler
```

### Environment variables

| Variable             | Description                                   |
|----------------------|-----------------------------------------------|
| `RAW_BUCKET_NAME`    | S3 bucket holding raw and processed data      |
| `ENVIRONMENT`        | Deployment environment (e.g. `dev`)           |
| `CONTROL_TABLE_NAME` | DynamoDB table name for enrichment control    |
| `CONTROL_TABLE_ARN`  | DynamoDB table ARN for enrichment control     |

## Infrastructure as Code (IaC)

The cloud infrastructure is provisioned using a clean, modular **Terraform** architecture located in `iac/`.

- **Modules:** Reusable components (e.g., `lambda`, `dynamo-db`, `s3_etl`) containing the exact specifications for AWS resources.
- **Environments:** Environment-specific configurations (e.g., `dev`) that instantiate modules using `.tfvars` to inject correct naming conventions and variables.
- **Orchestration:** The root-level `Makefile` abstracts Terraform commands, ensuring consistent execution across tests and deployments.

The Lambda module packages the entire `src/` directory as the deployment artifact.

## Workflow

### 1. Data Preparation (Lambda)
The Lambda is invoked (e.g. by a scheduler or Step Functions) to compute the ETL configuration: date boundaries, partition descriptors, and the input manifest. The output is passed to Step Functions.

### 2. Raw Ingestion (Glue)
Extract a raw snapshot of Hugging Face models and persist it to S3.

### 3. Model Discovery (Glue)
Process model IDs incrementally and enrich them with model-level metadata.

### 4. Gold Consolidation (Glue)
Join Silver Models and Silver Datasets, apply schema normalization, and publish the Gold dataset.

## Fault Tolerance Strategy

The pipeline includes:
- Checkpointing for long-running enrichment jobs
- Retry with exponential backoff for API rate limits
- Persistence of failed records for later reprocessing
- Incremental execution based on processed entities tracked in DynamoDB
- Idempotent writes where possible

## Cloud Deployment

AWS services used:
- **AWS Lambda** — data preparation config computation
- **AWS Step Functions** — pipeline orchestration
- **AWS Glue** — ETL execution (raw ingestion, enrichment, consolidation)
- **Amazon S3** — Bronze, Silver, and Gold storage
- **Amazon DynamoDB** — enrichment control table (partition state tracking)
- **AWS Secrets Manager** — Hugging Face token management
- **AWS KMS** — encryption key management
- **Amazon CloudWatch** — logs and monitoring

## Testing Strategy

This project includes tests for both application logic and infrastructure.

### Python Unit Tests

Tests are organized to mirror the source tree and cover:
- Boundary calculation logic
- Environment variable resolution
- Lambda configuration validation
- Partition descriptor computation
- Date parsing
- AI model metadata parsing and service
- S3 URI parsing and S3 service
- DynamoDB mapper, repository, and type conversion

Run with:
```bash
pytest
```

Coverage is reported automatically (configured in `pyproject.toml`).

### Infrastructure Tests (IaC)

Native Terraform unit tests (`.tftest.hcl`) validate module configurations and resource properties in memory (via `terraform plan`) before any real resources are provisioned on AWS.

Run all module tests in parallel:
```bash
make test
```

Run a specific module:
```bash
make test mod=lambda
```

## Local Development

### Python Setup

```bash
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

Run linting:
```bash
ruff check .
black --check .
```

Run type checking:
```bash
mypy src/
```

### Running Tests (Python & IaC)

```bash
# Run Python unit tests with coverage
pytest

# Run Terraform unit tests across all modules (parallel)
make test

# Run Terraform tests for a single module
make test mod=lambda
```

### Deploying Infrastructure

```bash
# Preview changes for the dev environment
make plan-dev

# Deploy all infrastructure to dev
make deploy-dev

# Deploy only the Lambda (fast path for code changes)
make deploy-lambda-dev

# Destroy dev infrastructure
make destroy-dev
```

> **Note:** Deployment requires a `.env` file at the project root exporting `HF_TOKEN`.

## Future Improvements

- Apache Iceberg support for transactional tables
- CI/CD pipeline for automated testing and deployment
- Data quality validation layer
- Monitoring dashboards for job metrics and API failures

## Use Cases

- Sustainability analysis of open ML models
- Correlation analysis between CO2 emissions and model size
- Study of dataset usage patterns across models
- Benchmarking metadata availability and reporting quality
- Analytical consumption through Athena or dashboards

## License

MIT License

Copyright (c) 2026 Juan F. Gallo

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
