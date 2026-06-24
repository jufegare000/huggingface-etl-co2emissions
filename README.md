# huggingface-etl-co3emissions

A reproducible, fault-tolerant ETL pipeline built on AWS Lambda, AWS Glue, and PySpark to extract, enrich, and consolidate Hugging Face model metadata into a curated analytical dataset for large-scale sustainability and performance analysis.

## Overview

This project builds a production-oriented data pipeline to collect and enrich metadata from Hugging Face models that report CO2 emissions. The pipeline supports reproducibility, incremental execution, fault recovery, and analytical consumption in the cloud.

The architecture follows a layered medallion approach:

| Layer | Description |
|-------|-------------|
| **Bronze** | Raw snapshot of Hugging Face model metadata |
| **Silver Models** | Enriched model-level metadata |
| **Silver Datasets** | Enriched dataset-level metadata |
| **Gold** | Curated analytical dataset ready for querying and visualization |

## Objectives

- Extract Hugging Face models with reported CO2 emissions
- Enrich model metadata with model size, library, domain, training metadata, and performance information
- Enrich referenced datasets with dataset-level metadata such as dataset size
- Build a curated final dataset for analysis
- Ensure the pipeline is reproducible, fault-tolerant, and suitable for cloud deployment

## Documentation Index

| Document | Description |
|----------|-------------|
| [Architecture](docs/architecture.md) | Pipeline diagram and hexagonal code architecture |
| [Data Layers](docs/data-layers.md) | Bronze, Silver, and Gold schema definitions |
| [Lambda – Data Preparation](docs/lambda.md) | Handler entrypoint, env vars, and dependency injection |
| [Infrastructure as Code](docs/iac.md) | Terraform modules and environment configuration |
| [Pipeline Workflow](docs/workflow.md) | Step-by-step pipeline execution and fault tolerance |
| [Cloud Deployment](docs/cloud-deployment.md) | AWS services used and their roles |
| [Testing](docs/testing.md) | Python unit tests and Terraform IaC tests |
| [Local Development](docs/local-development.md) | Setup, linting, type checking, and deploy commands |
| [Project Structure](docs/project-structure.md) | Full directory tree |
| [Roadmap & Use Cases](docs/roadmap.md) | Future improvements and analytical use cases |

## License

MIT License — Copyright (c) 2026 Juan F. Gallo
