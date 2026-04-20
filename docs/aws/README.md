# AWS Deployment Setup

This repo currently deploys:

- six Lambdas: location, retrieval, ingestion, processing, analytics, and watchlist
- one HTTP API with location, retrieval, ingestion, processing, risk, and watchlist routes
- one daily EventBridge rule at `02:00 UTC` that invokes ingestion for all hubs
- one application data bucket chosen per AWS account
- one ML model artifact at `models/risk_model.joblib`
- one shared Lambda execution role managed by Terraform

## 1. Fix local AWS credentials or use GitHub OIDC

Local AWS CLI access is currently working and can be used for Terraform apply.

If you want to deploy locally, fix your CLI credentials first:

```bash
aws configure
aws sts get-caller-identity
```

If you want GitHub Actions to deploy, create an IAM role using the trust and permissions policies in this folder and then set the GitHub `staging`, `dev`, and `prod` environment variables.

## 2. Create the Terraform state bucket

Pick a globally unique bucket name. Example:

```bash
export AWS_REGION=ap-southeast-2
export TF_STATE_BUCKET=seng3011-tf-state-zayan-001
```

Create the bucket:

```bash
aws s3api create-bucket \
  --bucket "$TF_STATE_BUCKET" \
  --region "$AWS_REGION"
```

Enable versioning:

```bash
aws s3api put-bucket-versioning \
  --bucket "$TF_STATE_BUCKET" \
  --versioning-configuration Status=Enabled
```

Block public access:

```bash
aws s3api put-public-access-block \
  --bucket "$TF_STATE_BUCKET" \
  --public-access-block-configuration \
  BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
```

Enable default encryption:

```bash
aws s3api put-bucket-encryption \
  --bucket "$TF_STATE_BUCKET" \
  --server-side-encryption-configuration \
  '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
```

## 3. Create the GitHub OIDC provider and deploy role

Use:

- [github-actions-oidc-trust-policy.json](/Users/zayanfarazi/Developer/uni/seng3011/docs/aws/github-actions-oidc-trust-policy.json)
- [github-actions-terraform-policy.json](/Users/zayanfarazi/Developer/uni/seng3011/docs/aws/github-actions-terraform-policy.json)

Replace these placeholders before creating the role:

- `<aws-account-id>`
- `<tf-state-bucket-name>`
- `<app-bucket-name>`

Create the role:

```bash
aws iam create-role \
  --role-name seng3011-github-actions \
  --assume-role-policy-document file://docs/aws/github-actions-oidc-trust-policy.json
```

Create the inline policy:

```bash
aws iam put-role-policy \
  --role-name seng3011-github-actions \
  --policy-name seng3011-terraform \
  --policy-document file://docs/aws/github-actions-terraform-policy.json
```

Terraform now creates the shared Lambda execution role directly, so the GitHub deploy role must be allowed to create, update, attach policies to, and pass that role.

## 4. Configure the GitHub `staging`, `dev`, and `prod` environments

Add these variables in each GitHub environment:

- `AWS_ROLE_ARN=arn:aws:iam::<aws-account-id>:role/seng3011-github-actions`
- `AWS_REGION=ap-southeast-2`
- `TF_STATE_BUCKET=<your-state-bucket-name>`
- `TF_STATE_KEY=<environment>/terraform.tfstate`
- `TF_VAR_data_bucket_name=<your-app-bucket-name>`
- `DEV_BASE_URL=https://<api-id>.execute-api.ap-southeast-2.amazonaws.com/dev`

Examples:

- `staging`: `TF_STATE_KEY=staging/terraform.tfstate`, `TF_VAR_data_bucket_name=<team>-app-<account-id>-staging`
- `dev`: `TF_STATE_KEY=dev/terraform.tfstate`, `TF_VAR_data_bucket_name=<team>-app-<account-id>-dev`
- `prod`: `TF_STATE_KEY=prod/terraform.tfstate`, `TF_VAR_data_bucket_name=<team>-app-<account-id>-prod`

Add this GitHub secret to all three environments:

- `PIRATE_WEATHER_API_KEY=<your-pirate-weather-key>`

The shared trust policy can cover all three environments. A common release flow is:

- `dev` on non-`main` branch pushes
- `staging` on `main`
- `prod` by manual promotion

The separate GitHub environment names still let you add approval gates for `prod` if you want.

## 5. Initialize and apply Terraform locally

From [terraform](/Users/zayanfarazi/Developer/uni/seng3011/terraform):

Build the Lambda zip artifacts first:

```bash
bash ../scripts/build_lambda_artifacts.sh
```

```bash
terraform init \
  -backend-config="bucket=$TF_STATE_BUCKET" \
  -backend-config="key=dev/terraform.tfstate" \
  -backend-config="region=$AWS_REGION" \
  -backend-config="use_lockfile=true"
```

```bash
terraform apply \
  -var='data_bucket_name=<your-app-bucket-name>' \
  -var='pirate_weather_api_key=<your-pirate-weather-key>'
```

## 6. Test the live endpoints

After apply, use the Terraform outputs or call the expected URLs directly.

```bash
curl "https://<api-id>.execute-api.ap-southeast-2.amazonaws.com/dev/ese/v1/retrieve/raw/weather/H001?date=10-03-2026"
```

```bash
curl "https://<api-id>.execute-api.ap-southeast-2.amazonaws.com/dev/ese/v1/retrieve/processed/weather/H001?date=10-03-2026"
```

```bash
curl -X POST "https://<api-id>.execute-api.ap-southeast-2.amazonaws.com/dev/ese/v1/ingest/weather/H001"
```

```bash
curl -X GET "https://<api-id>.execute-api.ap-southeast-2.amazonaws.com/dev/ese/v1/risk/location/H001?date=10-03-2026"
```
