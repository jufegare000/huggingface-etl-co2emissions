# CLAUDE.md

## Project context

This repository contains AWS Lambda functions deployed through Terraform.

The current task is to refactor one existing Lambda from a monolithic implementation into a modular, testable, deployable structure without changing its functional behavior.

## Hard rules

- Do not change Terraform unless explicitly requested.
- Do not change the Lambda handler path unless explicitly requested.
- Do not change runtime behavior.
- Do not change environment variable names.
- Do not change IAM assumptions, resource names, event sources, or Terraform module interfaces.
- Do not introduce new external dependencies without explaining why.
- Prefer incremental refactoring over rewriting.
- Preserve the current Lambda input/output contract.
- Preserve current logging semantics unless there is a clear defect.
- Add or update tests for extracted logic.
- Keep the AWS Lambda entrypoint thin.

## Architecture rules

- The Lambda handler should only parse the event, initialize dependencies, call the service layer, handle top-level errors, and return the response.
- Business logic should live outside the handler.
- AWS SDK calls should be isolated in adapter/client modules.
- Data transformation logic should be isolated in pure functions where possible.
- Configuration should be read from environment variables in one place.
- Validation should be explicit and testable.
- Terraform packaging assumptions must be preserved.

## Expected refactoring sequence

1. Analyze the current Lambda and Terraform packaging.
2. Identify the active handler configured by Terraform.
3. Identify all environment variables used by the Lambda.
4. Identify external services used by the Lambda.
5. Propose a target module structure.
6. Refactor one responsibility at a time.
7. Run existing tests.
8. Add missing unit tests around extracted logic.
9. Confirm that the deployment artifact still includes the required files.