# AWS Deployment Setup

This repo currently deploys:

- one Lambda: `weather-retrieval-handler`
- one HTTP API with two live routes
- one application data bucket: `seng3011-app-zayan-360990919154-dev`
- the Lambda uses the existing IAM execution role `LabRole`

## 1. Fix local AWS credentials or use GitHub OIDC

Local AWS CLI access is currently working and can be used for Terraform apply.

If you want to deploy locally, fix your CLI credentials first:

```bash
aws configure
aws sts get-caller-identity
```

If you want GitHub Actions to deploy, create an IAM role using the trust and permissions policies in this folder and then set the GitHub `dev` environment variables.

## 2. Create the Terraform state bucket

Pick a globally unique bucket name. Example:

```bash
export AWS_REGION=us-east-1
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

Create the role:

```bash
aws iam create-role \
  --role-name seng3011-github-actions-dev \
  --assume-role-policy-document file://docs/aws/github-actions-oidc-trust-policy.json
```

Create the inline policy:

```bash
aws iam put-role-policy \
  --role-name seng3011-github-actions-dev \
  --policy-name seng3011-terraform-dev \
  --policy-document file://docs/aws/github-actions-terraform-policy.json
```

This branch intentionally does not create a dedicated Lambda execution role because the current lab identity cannot perform `iam:CreateRole`. Terraform references the existing `LabRole` instead.

## 4. Configure the GitHub `dev` environment

Add these variables in GitHub:

- `AWS_ROLE_ARN=arn:aws:iam::<aws-account-id>:role/seng3011-github-actions-dev`
- `AWS_REGION=us-east-1`
- `TF_STATE_BUCKET=<your-state-bucket-name>`
- `TF_STATE_KEY=dev/terraform.tfstate`
- `TF_VAR_data_bucket_name=seng3011-app-zayan-360990919154-dev`

## 5. Initialize and apply Terraform locally

From [terraform](/Users/zayanfarazi/Developer/uni/seng3011/terraform):

```bash
terraform init \
  -backend-config="bucket=$TF_STATE_BUCKET" \
  -backend-config="key=dev/terraform.tfstate" \
  -backend-config="region=$AWS_REGION" \
  -backend-config="use_lockfile=true"
```

```bash
terraform apply -var='data_bucket_name=seng3011-app-zayan-360990919154-dev'
```

## 6. Test the live retrieval endpoints

After apply, use the Terraform outputs or call the expected URLs directly:

```bash
curl "https://<api-id>.execute-api.us-east-1.amazonaws.com/dev/ese/v1/retrieve/raw/weather/H001?date=10-03-2026"
```

```bash
curl "https://<api-id>.execute-api.us-east-1.amazonaws.com/dev/ese/v1/retrieve/processed/weather/H001?date=10-03-2026"
```
