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

variable "enable_lambda_integrations" {
  type        = bool
  description = "Whether to create HTTP API routes, Lambda integrations, and Lambda permissions."
  default     = false
}

variable "lambda_bindings" {
  description = "Lambda function names and invoke ARNs keyed by weather API route identifier"
  type = map(object({
    function_name = string
    invoke_arn    = string
  }))
  default = {}

  validation {
    condition = !var.enable_lambda_integrations || length(setsubtract(toset([
      "weather_ingest",
      "weather_retrieve_raw",
      "weather_retrieve_processed",
      "weather_process",
      "risk_region",
      "risk_location"
    ]), toset(keys(var.lambda_bindings)))) == 0
    error_message = "When enable_lambda_integrations is true, lambda_bindings must define weather_ingest, weather_retrieve_raw, weather_retrieve_processed, weather_process, risk_region, and risk_location."
  }
}

############################
# S3 bucket
############################

resource "aws_s3_bucket" "seng_3011_bkt" {
  bucket = "seng-3011-bkt-zayan-dev"

  tags = {
    Name        = "seng-3011-bkt"
    Environment = "dev"
  }
}

############################
# API Gateway HTTP API
############################

resource "aws_apigatewayv2_api" "weather_api" {
  name          = "weather-supply-chain-api"
  description   = "Weather and supply-chain risk HTTP API"
  protocol_type = "HTTP"

  tags = {
    Environment = "dev"
    Project     = "seng3011"
  }
}

############################
# Route definitions
############################

locals {
  routes = {
    weather_ingest = {
      route_key          = "POST /ese/v1/ingest/weather/{hub_id}"
      lambda_binding_key = "weather_ingest"
      path_pattern       = "POST/ese/v1/ingest/weather/*"
    }

    weather_retrieve_raw = {
      route_key          = "GET /ese/v1/retrieve/raw/weather/{hub_id}"
      lambda_binding_key = "weather_retrieve_raw"
      path_pattern       = "GET/ese/v1/retrieve/raw/weather/*"
    }

    weather_retrieve_processed = {
      route_key          = "GET /ese/v1/retrieve/processed/weather/{hub_id}"
      lambda_binding_key = "weather_retrieve_processed"
      path_pattern       = "GET/ese/v1/retrieve/processed/weather/*"
    }

    weather_process = {
      route_key          = "POST /ese/v1/process/weather"
      lambda_binding_key = "weather_process"
      path_pattern       = "POST/ese/v1/process/weather"
    }

    risk_region = {
      route_key          = "GET /ese/v1/risk/region"
      lambda_binding_key = "risk_region"
      path_pattern       = "GET/ese/v1/risk/region"
    }

    risk_location = {
      route_key          = "GET /ese/v1/risk/location/{hub_id}"
      lambda_binding_key = "risk_location"
      path_pattern       = "GET/ese/v1/risk/location/*"
    }
  }

  active_routes = var.enable_lambda_integrations ? local.routes : {}
}

############################
# Lambda proxy integrations
############################

resource "aws_apigatewayv2_integration" "route_integrations" {
  for_each = local.active_routes

  api_id                 = aws_apigatewayv2_api.weather_api.id
  integration_type       = "AWS_PROXY"
  integration_uri        = var.lambda_bindings[each.value.lambda_binding_key].invoke_arn
  integration_method     = "POST"
  payload_format_version = "2.0"
}

############################
# Routes
############################

resource "aws_apigatewayv2_route" "routes" {
  for_each = local.active_routes

  api_id    = aws_apigatewayv2_api.weather_api.id
  route_key = each.value.route_key
  target    = "integrations/${aws_apigatewayv2_integration.route_integrations[each.key].id}"
}

############################
# Allow API Gateway to invoke each Lambda
############################

resource "aws_lambda_permission" "allow_apigw" {
  for_each = local.active_routes

  statement_id  = "AllowHttpApiInvoke-${each.key}"
  action        = "lambda:InvokeFunction"
  function_name = var.lambda_bindings[each.value.lambda_binding_key].function_name
  principal     = "apigateway.amazonaws.com"

  source_arn = "${aws_apigatewayv2_api.weather_api.execution_arn}/*/${each.value.path_pattern}"
}

############################
# Stage
############################

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

output "weather_ingest_url_example" {
  value = "https://${aws_apigatewayv2_api.weather_api.id}.execute-api.${var.aws_region}.amazonaws.com/${aws_apigatewayv2_stage.api_stage.name}/ese/v1/ingest/weather/HUB123"
}

output "weather_retrieve_raw_url_example" {
  value = "https://${aws_apigatewayv2_api.weather_api.id}.execute-api.${var.aws_region}.amazonaws.com/${aws_apigatewayv2_stage.api_stage.name}/ese/v1/retrieve/raw/weather/HUB123?date=08-03-2026"
}

output "weather_retrieve_processed_url_example" {
  value = "https://${aws_apigatewayv2_api.weather_api.id}.execute-api.${var.aws_region}.amazonaws.com/${aws_apigatewayv2_stage.api_stage.name}/ese/v1/retrieve/processed/weather/HUB123?date=08-03-2026"
}

output "risk_region_url_example" {
  value = "https://${aws_apigatewayv2_api.weather_api.id}.execute-api.${var.aws_region}.amazonaws.com/${aws_apigatewayv2_stage.api_stage.name}/ese/v1/risk/region?region=sydney"
}

output "risk_location_url_example" {
  value = "https://${aws_apigatewayv2_api.weather_api.id}.execute-api.${var.aws_region}.amazonaws.com/${aws_apigatewayv2_stage.api_stage.name}/ese/v1/risk/location/HUB123"
}
