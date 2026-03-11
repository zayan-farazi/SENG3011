# CI/CD Setup

This repository uses two GitHub Actions workflows:

- `terraform-ci.yml`: runs on pull requests and non-`main` pushes for Terraform formatting, backend-free validation, OpenAPI YAML parsing, and Python quality checks; it also runs a dev-backed Terraform plan only when the `dev` environment is configured
- `terraform-deploy-dev.yml`: runs on pushes to `main` and applies Terraform to the `dev` environment

## GitHub setup

Create a GitHub environment named `dev` and add these variables:

- `AWS_ROLE_ARN`: IAM role assumed by GitHub Actions through OIDC
- `AWS_REGION`: AWS region for Terraform and the provider, for example `us-east-1`
- `TF_STATE_BUCKET`: S3 bucket name used for remote Terraform state
- `TF_STATE_KEY`: state object path, for example `dev/terraform.tfstate`
- `TF_VAR_data_bucket_name`: app bucket name for retrieval data, for example `seng3011-app-zayan-360990919154-dev`

AWS IAM setup details and example policies are in [docs/aws/README.md](/Users/zayanfarazi/Developer/uni/seng3011/docs/aws/README.md).

Add branch protection on `main` so the Terraform CI workflow must pass before merge.

## AWS setup

Create the remote-state S3 bucket before enabling the workflows.

Create an IAM role for GitHub OIDC with:

- a trust policy allowing this repository to assume the role from `token.actions.githubusercontent.com`
- permissions for the Terraform-managed API Gateway, Lambda, S3 resources, and state bucket access

Example trust policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "arn:aws:iam::<aws-account-id>:oidc-provider/token.actions.githubusercontent.com"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "token.actions.githubusercontent.com:aud": "sts.amazonaws.com"
        },
        "StringLike": {
          "token.actions.githubusercontent.com:sub": "repo:<github-owner>/<github-repo>:*"
        }
      }
    }
  ]
}
```

## Deployment flow

- Pull request or branch push touching `terraform/**` or `.github/workflows/**` runs static CI checks without requiring AWS credentials or remote backend access
- If Python project files are present, the CI workflow also runs Python linting, type checking, tests, and coverage
- If `AWS_ROLE_ARN` and `TF_STATE_BUCKET` are configured in the GitHub `dev` environment, the CI workflow also runs a Terraform plan against the dev backend
- Merge to `main` touching those paths runs the dev deploy workflow
- Terraform now packages and deploys the retrieval Lambda directly, seeds the development S3 data, and wires the two retrieval routes to API Gateway
- The Terraform state bucket stays `seng3011-tf-state-zayan-360990919154`; the application data bucket should be passed separately as `TF_VAR_data_bucket_name`

## Python quality checks

The CI workflow auto-detects a Python project by looking for:

- `pyproject.toml`
- `requirements.txt`
- `requirements-dev.txt`
- or Python source files

When detected, it runs:

- `ruff check .`
- `mypy .`
- `pytest --cov=. --cov-report=term-missing --cov-report=xml`

If no Python project exists yet, the Python quality job is skipped rather than failing the workflow.
