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

# Lambda details supplied by teammates
variable "list_items_lambda_function_name" {
  type        = string
  description = "Lambda function name for GET /items"
}

variable "list_items_lambda_invoke_arn" {
  type        = string
  description = "Lambda invoke ARN for GET /items"
}

variable "create_item_lambda_function_name" {
  type        = string
  description = "Lambda function name for POST /items"
}

variable "create_item_lambda_invoke_arn" {
  type        = string
  description = "Lambda invoke ARN for POST /items"
}

variable "get_item_lambda_function_name" {
  type        = string
  description = "Lambda function name for GET /items/{id}"
}

variable "get_item_lambda_invoke_arn" {
  type        = string
  description = "Lambda invoke ARN for GET /items/{id}"
}

variable "update_item_lambda_function_name" {
  type        = string
  description = "Lambda function name for PUT /items/{id}"
}

variable "update_item_lambda_invoke_arn" {
  type        = string
  description = "Lambda invoke ARN for PUT /items/{id}"
}

variable "delete_item_lambda_function_name" {
  type        = string
  description = "Lambda function name for DELETE /items/{id}"
}

variable "delete_item_lambda_invoke_arn" {
  type        = string
  description = "Lambda invoke ARN for DELETE /items/{id}"
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

resource "aws_api_gateway_rest_api" "crud_api" {
  name        = "crud-api"
  description = "CRUD API Gateway for items service"

  endpoint_configuration {
    types = ["REGIONAL"]
  }

  tags = {
    Environment = "dev"
    Project     = "seng3011"
  }
}

# /items
resource "aws_api_gateway_resource" "items" {
  rest_api_id = aws_api_gateway_rest_api.crud_api.id
  parent_id   = aws_api_gateway_rest_api.crud_api.root_resource_id
  path_part   = "items"
}

# /items/{id}
resource "aws_api_gateway_resource" "item_id" {
  rest_api_id = aws_api_gateway_rest_api.crud_api.id
  parent_id   = aws_api_gateway_resource.items.id
  path_part   = "{id}"
}

############################
# Route definitions
############################

locals {
  routes = {
    list_items = {
      resource_id   = aws_api_gateway_resource.items.id
      http_method   = "GET"
      function_name = var.list_items_lambda_function_name
      invoke_arn    = var.list_items_lambda_invoke_arn
      path_pattern  = "items"
    }

    create_item = {
      resource_id   = aws_api_gateway_resource.items.id
      http_method   = "POST"
      function_name = var.create_item_lambda_function_name
      invoke_arn    = var.create_item_lambda_invoke_arn
      path_pattern  = "items"
    }

    get_item = {
      resource_id   = aws_api_gateway_resource.item_id.id
      http_method   = "GET"
      function_name = var.get_item_lambda_function_name
      invoke_arn    = var.get_item_lambda_invoke_arn
      path_pattern  = "items/*"
    }

    update_item = {
      resource_id   = aws_api_gateway_resource.item_id.id
      http_method   = "PUT"
      function_name = var.update_item_lambda_function_name
      invoke_arn    = var.update_item_lambda_invoke_arn
      path_pattern  = "items/*"
    }

    delete_item = {
      resource_id   = aws_api_gateway_resource.item_id.id
      http_method   = "DELETE"
      function_name = var.delete_item_lambda_function_name
      invoke_arn    = var.delete_item_lambda_invoke_arn
      path_pattern  = "items/*"
    }
  }
}

############################
# Methods
############################

resource "aws_api_gateway_method" "route_methods" {
  for_each = local.routes

  rest_api_id   = aws_api_gateway_rest_api.crud_api.id
  resource_id   = each.value.resource_id
  http_method   = each.value.http_method
  authorization = "NONE"
}

############################
# Lambda proxy integrations
############################

resource "aws_api_gateway_integration" "route_integrations" {
  for_each = local.routes

  rest_api_id             = aws_api_gateway_rest_api.crud_api.id
  resource_id             = each.value.resource_id
  http_method             = aws_api_gateway_method.route_methods[each.key].http_method
  integration_http_method = "POST"
  type                    = "AWS_PROXY"
  uri                     = each.value.invoke_arn
}

############################
# Allow API Gateway to invoke each Lambda
############################

resource "aws_lambda_permission" "allow_apigw" {
  for_each = local.routes

  statement_id  = "AllowAPIGatewayInvoke-${each.key}"
  action        = "lambda:InvokeFunction"
  function_name = each.value.function_name
  principal     = "apigateway.amazonaws.com"

  source_arn = "${aws_api_gateway_rest_api.crud_api.execution_arn}/*/${each.value.http_method}/${each.value.path_pattern}"
}

############################
# Deployment + Stage
############################

resource "aws_api_gateway_deployment" "crud_api" {
  rest_api_id = aws_api_gateway_rest_api.crud_api.id

  depends_on = [
    aws_api_gateway_integration.route_integrations
  ]

  triggers = {
    redeployment = sha1(jsonencode({
      methods = [
        for k, v in aws_api_gateway_method.route_methods :
        "${k}:${v.http_method}:${v.resource_id}"
      ]
      integrations = [
        for k, v in aws_api_gateway_integration.route_integrations :
        "${k}:${v.uri}"
      ]
    }))
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_api_gateway_stage" "dev" {
  rest_api_id   = aws_api_gateway_rest_api.crud_api.id
  deployment_id = aws_api_gateway_deployment.crud_api.id
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
  value = aws_api_gateway_rest_api.crud_api.id
}

output "api_stage_name" {
  value = aws_api_gateway_stage.dev.stage_name
}

output "base_invoke_url" {
  value = "https://${aws_api_gateway_rest_api.crud_api.id}.execute-api.${var.aws_region}.amazonaws.com/${aws_api_gateway_stage.dev.stage_name}"
}

output "items_url" {
  value = "https://${aws_api_gateway_rest_api.crud_api.id}.execute-api.${var.aws_region}.amazonaws.com/${aws_api_gateway_stage.dev.stage_name}/items"
}

output "item_by_id_url_example" {
  value = "https://${aws_api_gateway_rest_api.crud_api.id}.execute-api.${var.aws_region}.amazonaws.com/${aws_api_gateway_stage.dev.stage_name}/items/123"
}
