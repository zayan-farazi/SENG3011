# CI/CD Setup

This repository uses two GitHub Actions workflows:

- `terraform-ci.yml`: runs on pull requests and non-`main` pushes for Terraform formatting, backend-free validation, OpenAPI YAML parsing, and Python quality checks
- `terraform-deploy-dev.yml`: runs on pushes to `main` and applies Terraform to the `dev` environment

## GitHub setup

Create a GitHub environment named `dev` and add these variables:

- `AWS_ROLE_ARN`: IAM role assumed by GitHub Actions through OIDC
- `AWS_REGION`: AWS region for Terraform and the provider, for example `ap-southeast-2`
- `TF_STATE_BUCKET`: S3 bucket name used for remote Terraform state
- `TF_STATE_KEY`: state object path, for example `dev/terraform.tfstate`
- `TF_VAR_data_bucket_name`: app bucket name for application data, for example `<team>-app-<account-id>-dev`

Add this GitHub `dev` environment secret:

- `PIRATE_WEATHER_API_KEY`: Pirate Weather API key used by the ingestion Lambda

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

- Pull request or branch push touching `terraform/**`, `docs/openapi.yaml`, `scripts/**`, `models/**`, or `.github/workflows/**` runs static CI checks without requiring AWS credentials or remote backend access
- If Python project files are present, the CI workflow also runs Python linting, type checking, tests, and coverage
- CI also builds Linux Lambda zip artifacts to validate the deploy packaging path
- Merge to `main` touching those paths runs the dev deploy workflow
- Deploy builds Linux Lambda artifacts, uploads the risk model to S3, and wires the location, retrieval, ingestion, processing, `risk/location`, and watchlist routes to API Gateway
- Terraform also creates a daily EventBridge rule at `02:00 UTC` that invokes the ingestion Lambda with an empty payload so it ingests all hubs automatically
- `risk/region` stays documented only until a handler is implemented
- The Terraform state bucket and application data bucket should both be supplied from the GitHub `dev` environment for the target AWS account

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
