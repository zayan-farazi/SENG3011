# CI/CD Setup

This repository uses five GitHub Actions workflows:

- `terraform-ci.yml`: runs on pull requests and non-`main` pushes for Terraform formatting, backend-free validation, OpenAPI YAML parsing, and Python quality checks
- `terraform-deploy-dev.yml`: runs on non-`main` pushes and applies Terraform to the `dev` environment
- `terraform-deploy-staging.yml`: runs on pushes to `main` and applies Terraform to the `staging` environment
- `combined-quality-report.yml`: runs after staging completes and generates the final HTML and PDF quality reports from CI and staging artifacts
- `terraform-deploy-prod.yml`: runs manually and applies Terraform to the `prod` environment

## GitHub setup

Create GitHub environments named `staging`, `dev`, and `prod`. Each environment should have its own Terraform state key and application bucket so the stacks can coexist in the same AWS account. You can use one shared GitHub OIDC deploy role across all three environments if you prefer.

Add these variables to all three environments:

- `AWS_ROLE_ARN`: IAM role assumed by GitHub Actions through OIDC
- `AWS_REGION`: AWS region for Terraform and the provider, for example `ap-southeast-2`
- `TF_STATE_BUCKET`: S3 bucket name used for remote Terraform state
- `TF_STATE_KEY`: state object path, for example `staging/terraform.tfstate`, `dev/terraform.tfstate`, or `prod/terraform.tfstate`
- `TF_VAR_data_bucket_name`: app bucket name for application data, for example `<team>-app-<account-id>-staging`, `<team>-app-<account-id>-dev`, or `<team>-app-<account-id>-prod`

Add this secret to all three environments:

- `PIRATE_WEATHER_API_KEY`: Pirate Weather API key used by the ingestion Lambda

Update `DEV_BASE_URL` whenever the dev stack is recreated in a new AWS account or gets a new API Gateway URL, otherwise the post-merge system tests will still point at the old deployment.

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
- CI uploads raw unit and integration test artifacts including JUnit XML, coverage XML, and HTML coverage output
- Pushes to non-`main` branches touching those paths run the dev deploy workflow
- Merge to `main` touching those paths runs the staging deploy workflow
- Production deploys are manual through the prod workflow so release timing stays explicit
- Each deploy builds Linux Lambda artifacts, uploads the risk model to S3, applies Terraform with the correct `environment_name` and stage, then runs the system tests against the freshly deployed base URL from Terraform output
- Deploy builds Linux Lambda artifacts, uploads the risk model to S3, and wires the location, retrieval, ingestion, processing, `risk/location`, and watchlist routes to API Gateway
- Terraform also creates a daily EventBridge rule at `02:00 UTC` that invokes the ingestion Lambda with an empty payload so it ingests all hubs automatically
- `risk/region` stays documented only until a handler is implemented
- Generic CI stays AWS-free; environment-specific system tests now run from the deploy workflows after each successful apply
- The staging workflow uploads raw staging test artifacts including JUnit XML and metadata about the matching CI run and commit SHA
- The combined quality report workflow downloads those CI and staging artifacts, generates the final HTML summaries, converts them to PDF, and uploads the final report bundle

Recommended environment values:

- `dev`: `TF_STATE_KEY=dev/terraform.tfstate`, `TF_VAR_data_bucket_name=<team>-app-<account-id>-dev`
- `staging`: `TF_STATE_KEY=staging/terraform.tfstate`, `TF_VAR_data_bucket_name=<team>-app-<account-id>-staging`
- `prod`: `TF_STATE_KEY=prod/terraform.tfstate`, `TF_VAR_data_bucket_name=<team>-app-<account-id>-prod`

## Python quality checks

The CI workflow auto-detects a Python project by looking for:

- `pyproject.toml`
- `requirements.txt`
- `requirements-dev.txt`
- or Python source files

When detected, it runs:

- `ruff check .`
- `mypy .`
- `pytest --cov=. --cov-report=term-missing --cov-report=xml --cov-report=html`

If no Python project exists yet, the Python quality job is skipped rather than failing the workflow.

## Reporting flow

- `terraform-ci.yml` is responsible for producing raw CI artifacts only
- `terraform-deploy-staging.yml` is responsible for producing raw staging artifacts only
- `combined-quality-report.yml` is the only workflow that generates final HTML and PDF reports
- Final reports are generated from downloaded CI and staging artifacts so report rendering happens in one place
