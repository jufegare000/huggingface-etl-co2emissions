# Pipeline Workflow

## Steps

### 1. Data Preparation (Lambda)

The Lambda is invoked (e.g. by a scheduler or Step Functions) to compute the ETL configuration: date boundaries, partition descriptors, and the input manifest. The output is passed to Step Functions.

### 2. Raw Ingestion (Glue)

Extract a raw snapshot of Hugging Face models and persist it to the Bronze layer in S3.

### 3. Model Discovery (Glue)

Process model IDs incrementally and enrich them with model-level metadata, producing the Silver Models layer.

### 4. Gold Consolidation (Glue)

Join Silver Models and Silver Datasets, apply schema normalization, and publish the Gold dataset to S3.

## Fault Tolerance Strategy

The pipeline is designed to survive partial failures and resume without full re-execution:

| Mechanism | Description |
|-----------|-------------|
| **Checkpointing** | Long-running enrichment jobs persist progress so they can resume from the last checkpoint |
| **Retry with backoff** | Exponential backoff on Hugging Face API rate-limit errors |
| **Failed record persistence** | Failed records are written separately for later reprocessing |
| **Incremental execution** | DynamoDB tracks processed entities; already-processed partitions are skipped |
| **Idempotent writes** | Outputs are written idempotently where possible to allow safe re-runs |
