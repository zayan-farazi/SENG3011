# Linking Additional Lambdas to API Gateway

This repo now deploys the retrieval microservice end to end with Terraform and API Gateway HTTP API. Use the same pattern for ingestion, processing, and analytics.

## Current pattern

The live Terraform wiring is in [terraform/main.tf](/Users/zayanfarazi/Developer/uni/seng3011/terraform/main.tf):

1. Package the Lambda source with `archive_file`
2. Create or reference the Lambda execution role
3. Create the `aws_lambda_function`
4. Create an `aws_apigatewayv2_integration`
5. Create one or more `aws_apigatewayv2_route` resources
6. Add matching `aws_lambda_permission` resources so API Gateway can invoke the Lambda
7. Add output URLs for smoke testing

## Step by step for a new microservice

### 1. Add the Lambda source

Create the handler under `/Users/zayanfarazi/Developer/uni/seng3011/lambdas/<service>/handler.py`.

Keep the exported handler in the form:

```python
def lambda_handler(event, context):
    ...
```

If the Lambda needs shared constants, keep using [constants.py](/Users/zayanfarazi/Developer/uni/seng3011/constants.py).

### 2. Package it in Terraform

Add a new `archive_file` block in [terraform/main.tf](/Users/zayanfarazi/Developer/uni/seng3011/terraform/main.tf). Copy the retrieval pattern and change the service paths:

```hcl
data "archive_file" "ingest_lambda" {
  type        = "zip"
  output_path = "${path.module}/.terraform/ingest_lambda.zip"

  source {
    content  = file("${path.module}/../constants.py")
    filename = "constants.py"
  }

  source {
    content  = file("${path.module}/../lambdas/__init__.py")
    filename = "lambdas/__init__.py"
  }

  source {
    content  = file("${path.module}/../lambdas/ingest/__init__.py")
    filename = "lambdas/ingest/__init__.py"
  }

  source {
    content  = file("${path.module}/../lambdas/ingest/handler.py")
    filename = "lambdas/ingest/handler.py"
  }
}
```

If a Lambda needs extra local modules, add another `source` block for each file.

### 3. Create the Lambda resource

Add a new `aws_lambda_function` using either the existing `LabRole` or a dedicated role later.

Example:

```hcl
resource "aws_lambda_function" "ingest" {
  function_name    = "weather-ingest-handler"
  role             = data.aws_iam_role.lab_role.arn
  runtime          = "python3.12"
  handler          = "lambdas.ingest.handler.lambda_handler"
  filename         = data.archive_file.ingest_lambda.output_path
  source_code_hash = data.archive_file.ingest_lambda.output_base64sha256
  timeout          = 30

  environment {
    variables = {
      DATA_BUCKET = aws_s3_bucket.seng_3011_bkt.bucket
    }
  }
}
```

Add any service-specific environment variables here, for example API keys or object prefixes.

### 4. Add the HTTP API route definition

For HTTP API, you do not create a nested resource tree. You add route keys directly.

Add a local map for the Lambda's routes:

```hcl
locals {
  ingest_routes = {
    ingest_weather = {
      route_key    = "POST /ese/v1/ingest/weather/{hub_id}"
      path_pattern = "POST/ese/v1/ingest/weather/*"
    }
  }
}
```

Use the exact public path from the proposal in `route_key`.

Use the wildcard version in `path_pattern` for the Lambda permission ARN.

### 5. Create the API Gateway integration

Each Lambda needs an HTTP API integration:

```hcl
resource "aws_apigatewayv2_integration" "ingest" {
  api_id                 = aws_apigatewayv2_api.weather_api.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.ingest.invoke_arn
  integration_method     = "POST"
  payload_format_version = "2.0"
}
```

Keep `payload_format_version = "2.0"` so the event shape matches the current retrieval Lambda style.

### 6. Create the routes

Add one `aws_apigatewayv2_route` resource using `for_each` over the route map:

```hcl
resource "aws_apigatewayv2_route" "ingest" {
  for_each = local.ingest_routes

  api_id    = aws_apigatewayv2_api.weather_api.id
  route_key = each.value.route_key
  target    = "integrations/${aws_apigatewayv2_integration.ingest.id}"
}
```

If one Lambda serves multiple endpoints, keep them in the same local map and one integration is enough.

### 7. Add invoke permissions

API Gateway cannot call the Lambda unless you add `aws_lambda_permission`:

```hcl
resource "aws_lambda_permission" "allow_apigw_ingest" {
  for_each = local.ingest_routes

  statement_id  = "AllowHttpApiInvoke-ingest-${each.key}"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingest.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.weather_api.execution_arn}/*/${each.value.path_pattern}"
}
```

The `source_arn` must match the route. This is the main place people get wrong when a route deploys but returns `500` or `403`.

### 8. Update outputs

Add one output URL per new endpoint so you can test immediately after `terraform apply`.

Example:

```hcl
output "weather_ingest_url_example" {
  value = "https://${aws_apigatewayv2_api.weather_api.id}.execute-api.${var.aws_region}.amazonaws.com/${aws_apigatewayv2_stage.api_stage.name}/ese/v1/ingest/weather/H001"
}
```

### 9. Update the OpenAPI doc

Keep [terraform/weather-api.openapi.yaml](/Users/zayanfarazi/Developer/uni/seng3011/terraform/weather-api.openapi.yaml) aligned with the live routes.

For each new endpoint:

1. Add the path and method
2. Add path and query parameters
3. Add request body if needed
4. Add success and error responses
5. Note whether the endpoint is documented only or deployed live

### 10. Add tests before wiring the route

Before applying Terraform, add unit tests for the handler in `/Users/zayanfarazi/Developer/uni/seng3011/tests/unit`.

At minimum, cover:

- success response
- missing required input
- invalid `hub_id`
- invalid `date` where relevant
- not found behavior where relevant

### 11. Apply and smoke test

From [/Users/zayanfarazi/Developer/uni/seng3011/terraform](/Users/zayanfarazi/Developer/uni/seng3011/terraform):

```bash
terraform apply -var='data_bucket_name=seng3011-app-zayan-360990919154-dev'
```

Then test the new endpoint with `curl`.

## Route checklist for the remaining services

Use these route keys when the other Lambdas are ready:

- `POST /ese/v1/ingest/weather/{hub_id}`
- `POST /ese/v1/process/weather`
- `GET /ese/v1/risk/region`
- `GET /ese/v1/risk/location/{hub_id}`

The retrieval routes are already wired:

- `GET /ese/v1/retrieve/raw/weather/{hub_id}`
- `GET /ese/v1/retrieve/processed/weather/{hub_id}`

## Recommended order

1. Implement the Lambda handler and tests
2. Package the Lambda with `archive_file`
3. Create the Lambda resource
4. Add the HTTP API integration
5. Add the route resource
6. Add the Lambda permission
7. Add an output URL
8. Update OpenAPI
9. Run `terraform validate`
10. Run `terraform apply`
11. Smoke-test the endpoint
