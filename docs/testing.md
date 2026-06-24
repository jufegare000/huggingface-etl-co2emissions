# Testing

## Python Unit Tests

Tests are organized to mirror the source tree under `tests/` and cover:

| Area | What is tested |
|------|---------------|
| Boundary calculation | Date boundary logic |
| Environment resolution | Environment variable reading |
| Lambda configuration | Config object validation |
| Partition descriptors | Partition computation |
| Date parsing | Date utility functions |
| AI metadata | Metadata parsing and service layer |
| S3 URI | URI parsing and S3 service |
| DynamoDB | Mapper, repository, and type conversion |

### Run all tests

```bash
pytest
```

Coverage is reported automatically (configured in `pyproject.toml`).

## Infrastructure Tests (IaC)

Native Terraform unit tests (`.tftest.hcl`) validate module configurations and resource properties in memory via `terraform plan` — no real AWS resources are provisioned.

### Run all module tests in parallel

```bash
make test
```

### Run a specific module

```bash
make test mod=lambda
```

## Test Layout

```text
tests/
├── application/data_preparation/services/
│   ├── config/boundaries/
│   ├── config/environment/
│   ├── config/lambda_function/
│   ├── config/partitions/
│   ├── dates/
│   ├── metadata/ai_metadata_models/
│   └── s3/
├── domain/shared/models/
└── infrastructure/
    ├── in/glue/
    └── out/dynamo/
```
