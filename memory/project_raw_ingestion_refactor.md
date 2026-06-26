---
name: project-raw-ingestion-refactor
description: raw_ingestion Glue job refactored from monolith to modular structure matching data_discovery and data_preparation patterns
metadata:
  type: project
---

The `raw_ingestion` Glue job was refactored from a single 884-line `raw_ingestion.py` into a clean layered structure.

**Why:** Follow the same modular pattern as `data_discovery` and `data_preparation` jobs — testable, loosely coupled, and with a thin Glue entrypoint.

**New structure under `src/raw_ingestion/`:**
- `domain/models/` — `PartitionConfig`, `RateBudget`, `IngestionMetrics` dataclasses
- `domain/services/` — Protocol interfaces: `HfModelFetcher`, `PartitionRepository`, `IngestionS3Writer`
- `application/services/enrichment/` — `RowEnrichmentService` (pure enrichment logic)
- `application/services/rate_limit/` — `RateLimitService` (budget/window sleep)
- `application/services/ingestion/` — `RawIngestionService` (orchestrator)
- `infrastructure/out/hf/` — `HfModelFetcherImplemented` (urllib-based HF API client)
- `infrastructure/out/dynamodb/` — `PartitionRepositoryImplemented` (DynamoDB CRUD)
- `infrastructure/out/s3/` — `IngestionS3WriterImplemented` (JSONL/JSON S3 writes + CSV reads)
- `infrastructure/in/glue/config/` — `RawIngestionJobConfig`, `load_config()` (Glue args)
- `infrastructure/in/glue/config/injection/` — `build_raw_ingestion_service()` factory
- `infrastructure/in/glue/raw_ingestion.py` — thin entrypoint (Spark/Glue context only)

**Tests:** 55 new unit tests under `tests/raw_ingestion/application/services/`

**How to apply:** The Glue Terraform module (`iac/modules/glue-job/main.tf`) does NOT yet include `extra_py_files` for `glue_src.zip`. To deploy this refactoring, the Terraform must be updated to add `--extra-py-files = "s3://.../glue-libs/src.zip"` — same pattern as the discovery job. This was left out per the "no Terraform changes unless requested" rule.

**Important pytest note:** Test directories under `tests/raw_ingestion/` must NOT have `__init__.py` files (except the pre-existing one at `tests/raw_ingestion/infrastructure/in/glue/`). Adding `__init__.py` causes pytest to shadow `src/raw_ingestion` with `tests/raw_ingestion`, breaking imports.
