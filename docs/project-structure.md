# Project Structure

The repository is organised around **bounded contexts** (domain-first). Each context owns its full vertical slice — application, domain, and infrastructure layers — rather than sharing a global layer tree.

```text
final/
├── README.md
├── CLAUDE.md
├── pyproject.toml
├── docs/                             ← Documentation index
│   ├── architecture.md
│   ├── data-layers.md
│   ├── lambda.md
│   ├── iac.md
│   ├── workflow.md
│   ├── cloud-deployment.md
│   ├── testing.md
│   ├── local-development.md
│   ├── project-structure.md
│   └── roadmap.md
├── src/
│   ├── data_discovery/               ← Glue discovery job: scrapes HF API → S3 raw CSV
│   │   ├── application/
│   │   │   └── services/discovery/   ← discovery_service.py, discovery_job_constants_enum.py
│   │   ├── domain/
│   │   └── infrastructure/
│   │       └── in/glue/discovery/
│   │           ├── 0_discovery_job.py          ← Glue entrypoint
│   │           └── config/injection/
│   │               └── data_discovery_dependency_injector.py
│   │
│   ├── data_preparation/             ← Lambda: coordinates ETL, manages partitions in DynamoDB
│   │   ├── application/
│   │   │   ├── services/
│   │   │   │   ├── config/           ← boundaries, env vars, lambda config, manifests, partitions
│   │   │   │   ├── dates/            ← date parsing implementation
│   │   │   │   ├── metadata/         ← AI model metadata parsing
│   │   │   │   ├── partitions/       ← partition service implementation
│   │   │   │   ├── s3/               ← S3 read/write, CSV column definitions
│   │   │   │   └── step_functions/   ← Step Functions trigger implementation
│   │   │   └── use_cases/
│   │   │       └── data_preparation/ ← DataPreparationConfigUseCaseImplemented
│   │   ├── domain/
│   │   │   ├── models/
│   │   │   │   ├── preparation/      ← boundary, manifest, partition, metadata value objects
│   │   │   │   └── s3/               ← BucketURI
│   │   │   ├── persistence/          ← DataPreparationRepository interface
│   │   │   ├── services/             ← service interfaces (config, dates, metadata, s3, …)
│   │   │   └── use_cases/
│   │   │       └── data_preparation/ ← DataPreparationConfigUseCase interface
│   │   └── infrastructure/
│   │       ├── in/lambda/
│   │       │   ├── data_preparation_job.py     ← Lambda handler (entrypoint)
│   │       │   └── config/injection/
│   │       │       └── dependency_injector.py  ← wires all dependencies at module load
│   │       └── out/dynamo/           ← DynamoDB client, mappers, repository, type conversion
│   │
│   ├── raw_ingestion/                ← Self-contained Glue scripts (no local imports)
│   │   └── infrastructure/
│   │       └── in/glue/
│   │           ├── raw_ingestion.py      ← Bronze ingestion: S3 raw CSV → Parquet
│   │           └── gold_consolidation.py ← Gold layer consolidation and enrichment
│   │
│   └── shared/                       ← Cross-context utilities
│       ├── domain/
│       │   ├── exceptions/           ← RateLimitError
│       │   ├── models/               ← SerializableModel base class
│       │   └── services/             ← data_discovery_columns_service
│       └── infrastructure/
│           └── out/
│               ├── s3/               ← S3 client and writer
│               ├── hugging_face/     ← HuggingFace API client
│               └── secrets_config/   ← HF token resolver via Secrets Manager
│
├── tests/
│   ├── data_preparation/
│   │   ├── application/services/     ← unit tests for all application-layer services
│   │   └── infrastructure/out/dynamo/ ← DynamoDB mapper, repository, type conversion tests
│   ├── shared/domain/models/         ← SerializableModel tests
│   └── raw_ingestion/infrastructure/in/glue/ ← Glue job test helpers
│
└── iac/
    ├── environments/
    │   └── dev/                      ← dev environment wiring all modules together
    └── modules/
        ├── lambda/                   ← packages src/ into a zip, deploys the Lambda
        ├── dynamo-db/
        ├── glue-job/                 ← raw ingestion Glue job
        ├── glue-discovery/           ← discovery Glue job
        ├── glue_enrichment/          ← gold consolidation Glue job
        ├── s3_etl/
        ├── secrets/
        ├── kms/
        ├── security_base/
        ├── security_policies/
        └── workflow/
```

## Bounded context responsibilities

| Context | Trigger | Writes to |
|---|---|---|
| `data_discovery` | Glue schedule | S3 raw CSV (discovery snapshot) |
| `data_preparation` | Lambda (Step Functions) | DynamoDB control table; triggers next Glue steps |
| `raw_ingestion` | Glue (bronze + gold jobs) | S3 bronze Parquet → S3 gold layer |
| `shared` | imported by any context | — (infrastructure adapters only) |
