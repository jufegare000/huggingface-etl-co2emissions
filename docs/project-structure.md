# Project Structure

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
