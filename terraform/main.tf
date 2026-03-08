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
  description = "Whether to create API Gateway methods, Lambda integrations, and Lambda permissions."
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
# API Gateway REST API
############################

resource "aws_api_gateway_rest_api" "weather_api" {
  name        = "weather-supply-chain-api"
  description = "Weather and supply-chain risk API Gateway"

  endpoint_configuration {
    types = ["REGIONAL"]
  }

  tags = {
    Environment = "dev"
    Project     = "seng3011"
  }
}

resource "aws_api_gateway_resource" "ese" {
  rest_api_id = aws_api_gateway_rest_api.weather_api.id
  parent_id   = aws_api_gateway_rest_api.weather_api.root_resource_id
  path_part   = "ese"
}

resource "aws_api_gateway_resource" "v1" {
  rest_api_id = aws_api_gateway_rest_api.weather_api.id
  parent_id   = aws_api_gateway_resource.ese.id
  path_part   = "v1"
}

resource "aws_api_gateway_resource" "ingest" {
  rest_api_id = aws_api_gateway_rest_api.weather_api.id
  parent_id   = aws_api_gateway_resource.v1.id
  path_part   = "ingest"
}

resource "aws_api_gateway_resource" "ingest_weather" {
  rest_api_id = aws_api_gateway_rest_api.weather_api.id
  parent_id   = aws_api_gateway_resource.ingest.id
  path_part   = "weather"
}

resource "aws_api_gateway_resource" "ingest_weather_hub_id" {
  rest_api_id = aws_api_gateway_rest_api.weather_api.id
  parent_id   = aws_api_gateway_resource.ingest_weather.id
  path_part   = "{hub_id}"
}

resource "aws_api_gateway_resource" "retrieve" {
  rest_api_id = aws_api_gateway_rest_api.weather_api.id
  parent_id   = aws_api_gateway_resource.v1.id
  path_part   = "retrieve"
}

resource "aws_api_gateway_resource" "retrieve_raw" {
  rest_api_id = aws_api_gateway_rest_api.weather_api.id
  parent_id   = aws_api_gateway_resource.retrieve.id
  path_part   = "raw"
}

resource "aws_api_gateway_resource" "retrieve_raw_weather" {
  rest_api_id = aws_api_gateway_rest_api.weather_api.id
  parent_id   = aws_api_gateway_resource.retrieve_raw.id
  path_part   = "weather"
}

resource "aws_api_gateway_resource" "retrieve_raw_weather_hub_id" {
  rest_api_id = aws_api_gateway_rest_api.weather_api.id
  parent_id   = aws_api_gateway_resource.retrieve_raw_weather.id
  path_part   = "{hub_id}"
}

resource "aws_api_gateway_resource" "retrieve_processed" {
  rest_api_id = aws_api_gateway_rest_api.weather_api.id
  parent_id   = aws_api_gateway_resource.retrieve.id
  path_part   = "processed"
}

resource "aws_api_gateway_resource" "retrieve_processed_weather" {
  rest_api_id = aws_api_gateway_rest_api.weather_api.id
  parent_id   = aws_api_gateway_resource.retrieve_processed.id
  path_part   = "weather"
}

resource "aws_api_gateway_resource" "retrieve_processed_weather_hub_id" {
  rest_api_id = aws_api_gateway_rest_api.weather_api.id
  parent_id   = aws_api_gateway_resource.retrieve_processed_weather.id
  path_part   = "{hub_id}"
}

resource "aws_api_gateway_resource" "process" {
  rest_api_id = aws_api_gateway_rest_api.weather_api.id
  parent_id   = aws_api_gateway_resource.v1.id
  path_part   = "process"
}

resource "aws_api_gateway_resource" "process_weather" {
  rest_api_id = aws_api_gateway_rest_api.weather_api.id
  parent_id   = aws_api_gateway_resource.process.id
  path_part   = "weather"
}

resource "aws_api_gateway_resource" "risk" {
  rest_api_id = aws_api_gateway_rest_api.weather_api.id
  parent_id   = aws_api_gateway_resource.v1.id
  path_part   = "risk"
}

resource "aws_api_gateway_resource" "risk_region" {
  rest_api_id = aws_api_gateway_rest_api.weather_api.id
  parent_id   = aws_api_gateway_resource.risk.id
  path_part   = "region"
}

resource "aws_api_gateway_resource" "risk_location" {
  rest_api_id = aws_api_gateway_rest_api.weather_api.id
  parent_id   = aws_api_gateway_resource.risk.id
  path_part   = "location"
}

resource "aws_api_gateway_resource" "risk_location_hub_id" {
  rest_api_id = aws_api_gateway_rest_api.weather_api.id
  parent_id   = aws_api_gateway_resource.risk_location.id
  path_part   = "{hub_id}"
}

############################
# Route definitions
############################

locals {
  routes = {
    weather_ingest = {
      resource_id         = aws_api_gateway_resource.ingest_weather_hub_id.id
      http_method         = "POST"
      lambda_binding_key  = "weather_ingest"
      path_pattern        = "ese/v1/ingest/weather/*"
      request_parameters  = { "method.request.path.hub_id" = true }
      validate_parameters = true
    }

    weather_retrieve_raw = {
      resource_id        = aws_api_gateway_resource.retrieve_raw_weather_hub_id.id
      http_method        = "GET"
      lambda_binding_key = "weather_retrieve_raw"
      path_pattern       = "ese/v1/retrieve/raw/weather/*"
      request_parameters = {
        "method.request.path.hub_id"      = true
        "method.request.querystring.date" = true
      }
      validate_parameters = true
    }

    weather_retrieve_processed = {
      resource_id        = aws_api_gateway_resource.retrieve_processed_weather_hub_id.id
      http_method        = "GET"
      lambda_binding_key = "weather_retrieve_processed"
      path_pattern       = "ese/v1/retrieve/processed/weather/*"
      request_parameters = {
        "method.request.path.hub_id"      = true
        "method.request.querystring.date" = true
      }
      validate_parameters = true
    }

    weather_process = {
      resource_id         = aws_api_gateway_resource.process_weather.id
      http_method         = "POST"
      lambda_binding_key  = "weather_process"
      path_pattern        = "ese/v1/process/weather"
      request_parameters  = {}
      validate_parameters = false
    }

    risk_region = {
      resource_id        = aws_api_gateway_resource.risk_region.id
      http_method        = "GET"
      lambda_binding_key = "risk_region"
      path_pattern       = "ese/v1/risk/region"
      request_parameters = {
        "method.request.querystring.region" = true
      }
      validate_parameters = true
    }

    risk_location = {
      resource_id         = aws_api_gateway_resource.risk_location_hub_id.id
      http_method         = "GET"
      lambda_binding_key  = "risk_location"
      path_pattern        = "ese/v1/risk/location/*"
      request_parameters  = { "method.request.path.hub_id" = true }
      validate_parameters = true
    }
  }

  active_routes = var.enable_lambda_integrations ? local.routes : {}
}

############################
# Request validation
############################

resource "aws_api_gateway_request_validator" "required_params" {
  rest_api_id                 = aws_api_gateway_rest_api.weather_api.id
  name                        = "required-request-params"
  validate_request_body       = false
  validate_request_parameters = true
}

############################
# Methods
############################

resource "aws_api_gateway_method" "route_methods" {
  for_each = local.active_routes

  rest_api_id          = aws_api_gateway_rest_api.weather_api.id
  resource_id          = each.value.resource_id
  http_method          = each.value.http_method
  authorization        = "NONE"
  request_parameters   = each.value.request_parameters
  request_validator_id = each.value.validate_parameters ? aws_api_gateway_request_validator.required_params.id : null
}

############################
# Lambda proxy integrations
############################

resource "aws_api_gateway_integration" "route_integrations" {
  for_each = local.active_routes

  rest_api_id             = aws_api_gateway_rest_api.weather_api.id
  resource_id             = each.value.resource_id
  http_method             = aws_api_gateway_method.route_methods[each.key].http_method
  integration_http_method = "POST"
  type                    = "AWS_PROXY"
  uri                     = var.lambda_bindings[each.value.lambda_binding_key].invoke_arn
}

############################
# Allow API Gateway to invoke each Lambda
############################

resource "aws_lambda_permission" "allow_apigw" {
  for_each = local.active_routes

  statement_id  = "AllowAPIGatewayInvoke-${each.key}"
  action        = "lambda:InvokeFunction"
  function_name = var.lambda_bindings[each.value.lambda_binding_key].function_name
  principal     = "apigateway.amazonaws.com"

  source_arn = "${aws_api_gateway_rest_api.weather_api.execution_arn}/*/${each.value.http_method}/${each.value.path_pattern}"
}

############################
# Deployment + Stage
############################

resource "aws_api_gateway_deployment" "weather_api" {
  rest_api_id = aws_api_gateway_rest_api.weather_api.id

  depends_on = [
    aws_api_gateway_integration.route_integrations
  ]

  triggers = {
    redeployment = sha1(jsonencode({
      methods = [
        for k, v in aws_api_gateway_method.route_methods :
        "${k}:${v.http_method}:${v.resource_id}:${jsonencode(v.request_parameters)}:${coalesce(v.request_validator_id, "none")}"
      ]
      integrations = [
        for k, v in aws_api_gateway_integration.route_integrations :
        "${k}:${v.uri}"
      ]
      permissions = [
        for k, v in aws_lambda_permission.allow_apigw :
        "${k}:${v.source_arn}:${v.function_name}"
      ]
    }))
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_api_gateway_stage" "api_stage" {
  rest_api_id   = aws_api_gateway_rest_api.weather_api.id
  deployment_id = aws_api_gateway_deployment.weather_api.id
  stage_name    = var.stage_name

  tags = {
    Environment = "dev"
    Project     = "seng3011"
  }
}

############################
# Outputs
############################

output "api_id" {
  value = aws_api_gateway_rest_api.weather_api.id
}

output "api_stage_name" {
  value = aws_api_gateway_stage.api_stage.stage_name
}

output "base_invoke_url" {
  value = "https://${aws_api_gateway_rest_api.weather_api.id}.execute-api.${var.aws_region}.amazonaws.com/${aws_api_gateway_stage.api_stage.stage_name}"
}

output "weather_ingest_url_example" {
  value = "https://${aws_api_gateway_rest_api.weather_api.id}.execute-api.${var.aws_region}.amazonaws.com/${aws_api_gateway_stage.api_stage.stage_name}/ese/v1/ingest/weather/HUB123"
}

output "weather_retrieve_raw_url_example" {
  value = "https://${aws_api_gateway_rest_api.weather_api.id}.execute-api.${var.aws_region}.amazonaws.com/${aws_api_gateway_stage.api_stage.stage_name}/ese/v1/retrieve/raw/weather/HUB123?date=08-03-2026"
}

output "weather_retrieve_processed_url_example" {
  value = "https://${aws_api_gateway_rest_api.weather_api.id}.execute-api.${var.aws_region}.amazonaws.com/${aws_api_gateway_stage.api_stage.stage_name}/ese/v1/retrieve/processed/weather/HUB123?date=08-03-2026"
}

output "risk_region_url_example" {
  value = "https://${aws_api_gateway_rest_api.weather_api.id}.execute-api.${var.aws_region}.amazonaws.com/${aws_api_gateway_stage.api_stage.stage_name}/ese/v1/risk/region?region=sydney"
}

output "risk_location_url_example" {
  value = "https://${aws_api_gateway_rest_api.weather_api.id}.execute-api.${var.aws_region}.amazonaws.com/${aws_api_gateway_stage.api_stage.stage_name}/ese/v1/risk/location/HUB123"
}
