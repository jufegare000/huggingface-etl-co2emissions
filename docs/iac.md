# Infrastructure as Code (IaC)

Cloud infrastructure is provisioned with a modular **Terraform** architecture located in `iac/`.

## Structure

```text
iac/
├── environments/
│   └── dev/          ← dev environment instantiating all modules
└── modules/
    ├── lambda/        ← packages src/ and deploys the Lambda
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

## Concepts

| Concept | Description |
|---------|-------------|
| **Modules** | Reusable components containing the exact AWS resource specifications |
| **Environments** | Environment-specific configs (e.g. `dev`) that instantiate modules via `.tfvars` |
| **Orchestration** | Root-level `Makefile` abstracts Terraform commands for consistent execution |

The Lambda module packages the entire `src/` directory as the deployment artifact.

## Makefile Targets

| Target | Action |
|--------|--------|
| `make plan-dev` | Preview changes for the dev environment |
| `make deploy-dev` | Deploy all infrastructure to dev |
| `make deploy-lambda-dev` | Deploy only the Lambda (fast path for code changes) |
| `make destroy-dev` | Destroy dev infrastructure |
| `make test` | Run all Terraform module tests in parallel |
| `make test mod=<name>` | Run tests for a specific module |

> **Note:** Deployment requires a `.env` file at the project root exporting `HF_TOKEN`.
