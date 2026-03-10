# CI/CD Setup

This repository uses two GitHub Actions workflows:

- `terraform-ci.yml`: runs on pull requests and non-`main` pushes for Terraform formatting, backend-free validation, and OpenAPI YAML parsing; it also runs a dev-backed Terraform plan only when the `dev` environment is configured
- `terraform-deploy-dev.yml`: runs on pushes to `main` and applies Terraform to the `dev` environment

## GitHub setup

Create a GitHub environment named `dev` and add these variables:

- `AWS_ROLE_ARN`: IAM role assumed by GitHub Actions through OIDC
- `AWS_REGION`: AWS region for Terraform and the provider, for example `us-east-1`
- `TF_STATE_BUCKET`: S3 bucket name used for remote Terraform state
- `TF_STATE_KEY`: state object path, for example `dev/terraform.tfstate`
- `ENABLE_LAMBDA_INTEGRATIONS`: `false` until the Lambda functions exist, then `true`
- `TF_VAR_lambda_bindings`: optional JSON object containing the six Lambda bindings once the Lambdas are deployed

Example `TF_VAR_lambda_bindings` value:

```json
{
  "weather_ingest": {
    "function_name": "weather-ingest",
    "invoke_arn": "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:123456789012:function:weather-ingest/invocations"
  },
  "weather_retrieve_raw": {
    "function_name": "weather-retrieve-raw",
    "invoke_arn": "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:123456789012:function:weather-retrieve-raw/invocations"
  },
  "weather_retrieve_processed": {
    "function_name": "weather-retrieve-processed",
    "invoke_arn": "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:123456789012:function:weather-retrieve-processed/invocations"
  },
  "weather_process": {
    "function_name": "weather-process",
    "invoke_arn": "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:123456789012:function:weather-process/invocations"
  },
  "risk_region": {
    "function_name": "risk-region",
    "invoke_arn": "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:123456789012:function:risk-region/invocations"
  },
  "risk_location": {
    "function_name": "risk-location",
    "invoke_arn": "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:123456789012:function:risk-location/invocations"
  }
}
```

Add branch protection on `main` so the Terraform CI workflow must pass before merge.

## AWS setup

Create the remote-state S3 bucket before enabling the workflows.

Create an IAM role for GitHub OIDC with:

- a trust policy allowing this repository to assume the role from `token.actions.githubusercontent.com`
- permissions for the Terraform-managed API Gateway, S3 resources, and state bucket access

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
- If `AWS_ROLE_ARN` and `TF_STATE_BUCKET` are configured in the GitHub `dev` environment, the CI workflow also runs a Terraform plan against the dev backend
- Merge to `main` touching those paths runs the dev deploy workflow
- Keep `ENABLE_LAMBDA_INTEGRATIONS=false` until the six Lambda bindings are available
- After Lambda deployment, set `ENABLE_LAMBDA_INTEGRATIONS=true` and populate `TF_VAR_lambda_bindings`
