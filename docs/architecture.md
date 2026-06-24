# Architecture

## Pipeline Overview

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

The codebase follows a **hexagonal (ports and adapters)** architecture with three layers:

```text
domain/          ← interfaces, domain models, repository contracts
application/     ← service and use-case implementations (pure business logic)
infrastructure/  ← AWS adapters (Lambda handler, Glue entrypoints, DynamoDB, S3, HF client)
```

### Layer Responsibilities

| Layer | Responsibility |
|-------|---------------|
| **Domain** | Defines service and repository interfaces as abstract classes. Contains no AWS SDK calls. |
| **Application** | Implements domain interfaces with pure business logic. |
| **Infrastructure** | Wires everything together: inbound handlers (Lambda, Glue) and outbound adapters (DynamoDB, S3, Secrets Manager, Hugging Face). |

## Key Features

- Modular, testable hexagonal architecture separating domain, application, and infrastructure concerns
- Thin Lambda handler — only parses the event, resolves dependencies, and delegates to the use case
- Dependency injection via a single `dependency_injector.py` module
- Reproducible execution through parameterized jobs and versioned snapshots
- Fault tolerance with checkpoints, retries, and recoverable intermediate outputs
- Incremental processing to avoid unnecessary reprocessing
- PySpark-based transformations for scalable joins, aggregation, normalization, and curation
- Cloud-native deployment using AWS Lambda, AWS Glue, S3, DynamoDB, and Step Functions
- Modular Infrastructure as Code (IaC) using Terraform with native unit testing
