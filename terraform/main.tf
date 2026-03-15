provider "aws" {
  region = var.aws_region
}

############################
# Variables
############################

variable "aws_region" {
  type        = string
  description = "AWS region for API Gateway + Lambda"
  default     = "us-east-1"
}

variable "stage_name" {
  type        = string
  description = "API Gateway stage name"
  default     = "dev"
}

variable "data_bucket_name" {
  type        = string
  description = "S3 bucket name for retrieval data and seeded development objects"
}

############################
# Locals
############################

locals {
  retrieval_lambda_name = "weather-retrieval-handler"
  seeded_hub_id         = "H001"
  seeded_date           = "10-03-2026"

  retrieval_routes = {
    retrieve_raw = {
      route_key    = "GET /ese/v1/retrieve/raw/weather/{hub_id}"
      path_pattern = "GET/ese/v1/retrieve/raw/weather/*"
    }

    retrieve_processed = {
      route_key    = "GET /ese/v1/retrieve/processed/weather/{hub_id}"
      path_pattern = "GET/ese/v1/retrieve/processed/weather/*"
    }
  }
}

############################
# Lambda package
############################

data "archive_file" "retrieval_lambda" {
  type        = "zip"
  output_path = "${path.module}/.terraform/retrieval_lambda.zip"

  source {
    content  = file("${path.module}/../constants.py")
    filename = "constants.py"
  }

  source {
    content  = file("${path.module}/../lambdas/__init__.py")
    filename = "lambdas/__init__.py"
  }

  source {
    content  = file("${path.module}/../lambdas/retrieval/__init__.py")
    filename = "lambdas/retrieval/__init__.py"
  }

  source {
    content  = file("${path.module}/../lambdas/retrieval/handler.py")
    filename = "lambdas/retrieval/handler.py"
  }
}

############################
# Existing IAM role
############################

data "aws_iam_role" "lab_role" {
  name = "LabRole"
}

############################
# S3 bucket and seed data
############################

resource "aws_s3_bucket" "seng_3011_bkt" {
  bucket = var.data_bucket_name

  tags = {
    Name        = var.data_bucket_name
    Environment = "dev"
  }
}

resource "aws_s3_object" "hubs_file" {
  bucket = aws_s3_bucket.seng_3011_bkt.id
  key    = "hubs.json"
  source = "${path.module}/../hubs.json"
  etag   = filemd5("${path.module}/../hubs.json")
}

resource "aws_s3_object" "sample_raw_weather" {
  bucket = aws_s3_bucket.seng_3011_bkt.id
  key    = "raw/weather/${local.seeded_hub_id}/${local.seeded_date}.json"
  source = "${path.module}/../tests/data/pirate_weather_raw_sample.json"
  etag   = filemd5("${path.module}/../tests/data/pirate_weather_raw_sample.json")
}

resource "aws_s3_object" "sample_processed_weather" {
  bucket = aws_s3_bucket.seng_3011_bkt.id
  key    = "processed/weather/${local.seeded_hub_id}/${local.seeded_date}.json"
  source = "${path.module}/../tests/data/processed_sample.json"
  etag   = filemd5("${path.module}/../tests/data/processed_sample.json")
}

############################
# Lambda
############################

resource "aws_lambda_function" "retrieval" {
  function_name    = local.retrieval_lambda_name
  role             = data.aws_iam_role.lab_role.arn
  runtime          = "python3.12"
  handler          = "lambdas.retrieval.handler.lambda_handler"
  filename         = data.archive_file.retrieval_lambda.output_path
  source_code_hash = data.archive_file.retrieval_lambda.output_base64sha256
  timeout          = 10

  environment {
    variables = {
      DATA_BUCKET = aws_s3_bucket.seng_3011_bkt.bucket
    }
  }

  tags = {
    Environment = "dev"
    Project     = "seng3011"
  }
}

############################
# API Gateway HTTP API
############################

resource "aws_apigatewayv2_api" "weather_api" {
  name          = "weather-supply-chain-api"
  description   = "Weather retrieval HTTP API"
  protocol_type = "HTTP"

  tags = {
    Environment = "dev"
    Project     = "seng3011"
  }
}

resource "aws_apigatewayv2_integration" "retrieval" {
  api_id                 = aws_apigatewayv2_api.weather_api.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.retrieval.invoke_arn
  integration_method     = "POST"
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_route" "retrieval" {
  for_each = local.retrieval_routes

  api_id    = aws_apigatewayv2_api.weather_api.id
  route_key = each.value.route_key
  target    = "integrations/${aws_apigatewayv2_integration.retrieval.id}"
}

resource "aws_lambda_permission" "allow_apigw" {
  for_each = local.retrieval_routes

  statement_id  = "AllowHttpApiInvoke-${each.key}"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.retrieval.function_name
  principal     = "apigateway.amazonaws.com"

  source_arn = "${aws_apigatewayv2_api.weather_api.execution_arn}/*/${each.value.path_pattern}"
}

resource "aws_apigatewayv2_stage" "api_stage" {
  api_id      = aws_apigatewayv2_api.weather_api.id
  name        = var.stage_name
  auto_deploy = true

  tags = {
    Environment = "dev"
    Project     = "seng3011"
  }
}

############################
# Outputs
############################

output "api_id" {
  value = aws_apigatewayv2_api.weather_api.id
}

output "api_stage_name" {
  value = aws_apigatewayv2_stage.api_stage.name
}

output "base_invoke_url" {
  value = "https://${aws_apigatewayv2_api.weather_api.id}.execute-api.${var.aws_region}.amazonaws.com/${aws_apigatewayv2_stage.api_stage.name}"
}

output "weather_retrieve_raw_url_example" {
  value = "https://${aws_apigatewayv2_api.weather_api.id}.execute-api.${var.aws_region}.amazonaws.com/${aws_apigatewayv2_stage.api_stage.name}/ese/v1/retrieve/raw/weather/${local.seeded_hub_id}?date=${local.seeded_date}"
}

output "weather_retrieve_processed_url_example" {
  value = "https://${aws_apigatewayv2_api.weather_api.id}.execute-api.${var.aws_region}.amazonaws.com/${aws_apigatewayv2_stage.api_stage.name}/ese/v1/retrieve/processed/weather/${local.seeded_hub_id}?date=${local.seeded_date}"
}
