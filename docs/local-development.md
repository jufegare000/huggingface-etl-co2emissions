# Local Development

## Python Setup

```bash
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

## Linting and Formatting

```bash
ruff check .
black --check .
```

## Type Checking

```bash
mypy src/
```

## Running Tests

```bash
# Python unit tests with coverage
pytest

# Terraform unit tests across all modules (parallel)
make test

# Terraform tests for a single module
make test mod=lambda
```

## Deploying Infrastructure

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
