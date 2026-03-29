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
  description = "S3 bucket name for application data, model artifacts, and Lambda build artifacts"
}

variable "pirate_weather_api_key" {
  type        = string
  description = "Pirate Weather API key for the ingestion Lambda"
  default     = ""
  sensitive   = true
}

variable "risk_model_source_path" {
  type        = string
  description = "Local path to the risk model artifact uploaded to S3"
  default     = "../models/risk_model.joblib"
}

############################
# Locals
############################

locals {
  seeded_hub_id             = "H001"
  seeded_date               = "10-03-2026"
  daily_ingestion_rule_name = "weather-ingestion-daily-all-hubs"

  hub_items = jsondecode(file("${path.module}/../hubs.json"))

  retrieval_lambda_name  = "weather-retrieval-handler"
  ingestion_lambda_name  = "weather-ingestion-handler"
  processing_lambda_name = "weather-processing-handler"
  analytics_lambda_name  = "weather-analytics-handler"

  lambda_artifact_dir = "${path.module}/../build/lambdas"
  retrieval_zip_path  = "${local.lambda_artifact_dir}/retrieval.zip"
  ingestion_zip_path  = "${local.lambda_artifact_dir}/ingestion.zip"
  processing_zip_path = "${local.lambda_artifact_dir}/processing.zip"
  analytics_zip_path  = "${local.lambda_artifact_dir}/analytics.zip"
  analytics_zip_key   = "artifacts/lambdas/analytics.zip"

  model_s3_key = "models/risk_model.joblib"
  api_base_url = "https://${aws_apigatewayv2_api.weather_api.id}.execute-api.${var.aws_region}.amazonaws.com/${aws_apigatewayv2_stage.api_stage.name}"

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

  ingestion_routes = {
    ingest_weather = {
      route_key    = "POST /ese/v1/ingest/weather/{hub_id}"
      path_pattern = "POST/ese/v1/ingest/weather/*"
    }
  }

  processing_routes = {
    process_weather = {
      route_key    = "POST /ese/v1/process/weather"
      path_pattern = "POST/ese/v1/process/weather"
    }
  }

  analytics_routes = {
    risk_location = {
      route_key    = "GET /ese/v1/risk/location/{hub_id}"
      path_pattern = "GET/ese/v1/risk/location/*"
    }
  }
}

############################
# Existing IAM role
############################

data "aws_iam_role" "lab_role" {
  name = "LabRole"
}

############################
# S3 bucket, DynamoDB table and seed data
############################

resource "aws_s3_bucket" "seng_3011_bkt" {
  bucket = var.data_bucket_name

  tags = {
    Name        = var.data_bucket_name
    Environment = "dev"
  }
}

resource "aws_dynamodb_table" "locations" {
  name         = "locations"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "hub_id"

  attribute {
    name = "hub_id"
    type = "S"
  }

  attribute {
    name = "lat_lon"
    type = "S"
  }

  # GSI for lat/lon uniqueness
  global_secondary_index {
    name               = "lat-lon-index"
    hash_key           = "lat_lon"
    projection_type    = "ALL"
  }

  tags = {
    Name        = "locations"
    Environment = "dev"
  }
}

resource "aws_dynamodb_table_item" "hub_seed" {
  for_each   = local.hub_items
  table_name = aws_dynamodb_table.locations.name
  hash_key   = "hub_id"

  item = jsonencode({
    hub_id     = each.key
    lat_lon    = "${each.value.lat}:${each.value.lon}"
    name       = each.value.name
    lat        = each.value.lat
    lon        = each.value.lon
    type       = "scheduled"
    created_at = timestamp()
  })

  # Ignore changes to avoid errors if the item already exists
  lifecycle {
    ignore_changes = [item]
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

resource "aws_s3_object" "risk_model" {
  bucket = aws_s3_bucket.seng_3011_bkt.id
  key    = local.model_s3_key
  source = var.risk_model_source_path
  etag   = filemd5(var.risk_model_source_path)
}

resource "aws_s3_object" "analytics_lambda_package" {
  bucket      = aws_s3_bucket.seng_3011_bkt.id
  key         = local.analytics_zip_key
  source      = local.analytics_zip_path
  source_hash = filemd5(local.analytics_zip_path)
}

############################
# Lambda functions
############################

resource "aws_lambda_function" "retrieval" {
  function_name    = local.retrieval_lambda_name
  role             = data.aws_iam_role.lab_role.arn
  runtime          = "python3.12"
  handler          = "lambdas.retrieval.handler.lambda_handler"
  filename         = local.retrieval_zip_path
  source_code_hash = filebase64sha256(local.retrieval_zip_path)
  timeout          = 10

  environment {
    variables = {
      DATA_BUCKET  = aws_s3_bucket.seng_3011_bkt.bucket
      API_BASE_URL = local.api_base_url
    }
  }

  tracing_config {
    mode = "Active"
  }

  tags = {
    Environment = "dev"
    Project     = "seng3011"
  }
}

resource "aws_lambda_function" "ingestion" {
  function_name    = local.ingestion_lambda_name
  role             = data.aws_iam_role.lab_role.arn
  runtime          = "python3.12"
  handler          = "lambdas.ingestion.handler.lambda_handler"
  filename         = local.ingestion_zip_path
  source_code_hash = filebase64sha256(local.ingestion_zip_path)
  timeout          = 30

  environment {
    variables = {
      DATA_BUCKET  = aws_s3_bucket.seng_3011_bkt.bucket
      API_KEY      = var.pirate_weather_api_key
      API_BASE_URL = local.api_base_url
    }
  }

  tracing_config {
    mode = "Active"
  }

  tags = {
    Environment = "dev"
    Project     = "seng3011"
  }
}

resource "aws_lambda_function" "processing" {
  function_name    = local.processing_lambda_name
  role             = data.aws_iam_role.lab_role.arn
  runtime          = "python3.12"
  handler          = "lambdas.processing.handler.lambda_handler"
  filename         = local.processing_zip_path
  source_code_hash = filebase64sha256(local.processing_zip_path)
  timeout          = 30

  environment {
    variables = {
      DATA_BUCKET  = aws_s3_bucket.seng_3011_bkt.bucket
      API_BASE_URL = local.api_base_url
    }
  }

  tracing_config {
    mode = "Active"
  }

  tags = {
    Environment = "dev"
    Project     = "seng3011"
  }
}

resource "aws_lambda_function" "analytics" {
  function_name    = local.analytics_lambda_name
  role             = data.aws_iam_role.lab_role.arn
  runtime          = "python3.12"
  handler          = "lambdas.analytics.handler.lambda_handler"
  s3_bucket        = aws_s3_bucket.seng_3011_bkt.id
  s3_key           = aws_s3_object.analytics_lambda_package.key
  source_code_hash = filebase64sha256(local.analytics_zip_path)
  timeout          = 60

  environment {
    variables = {
      DATA_BUCKET    = aws_s3_bucket.seng_3011_bkt.bucket
      API_BASE_URL   = local.api_base_url
      RISK_MODEL_KEY = local.model_s3_key
    }
  }

  tracing_config {
    mode = "Active"
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
  description   = "Weather and supply-chain risk HTTP API"
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

resource "aws_apigatewayv2_integration" "ingestion" {
  api_id                 = aws_apigatewayv2_api.weather_api.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.ingestion.invoke_arn
  integration_method     = "POST"
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_integration" "processing" {
  api_id                 = aws_apigatewayv2_api.weather_api.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.processing.invoke_arn
  integration_method     = "POST"
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_integration" "analytics" {
  api_id                 = aws_apigatewayv2_api.weather_api.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.analytics.invoke_arn
  integration_method     = "POST"
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_route" "retrieval" {
  for_each = local.retrieval_routes

  api_id    = aws_apigatewayv2_api.weather_api.id
  route_key = each.value.route_key
  target    = "integrations/${aws_apigatewayv2_integration.retrieval.id}"
}

resource "aws_apigatewayv2_route" "ingestion" {
  for_each = local.ingestion_routes

  api_id    = aws_apigatewayv2_api.weather_api.id
  route_key = each.value.route_key
  target    = "integrations/${aws_apigatewayv2_integration.ingestion.id}"
}

resource "aws_apigatewayv2_route" "processing" {
  for_each = local.processing_routes

  api_id    = aws_apigatewayv2_api.weather_api.id
  route_key = each.value.route_key
  target    = "integrations/${aws_apigatewayv2_integration.processing.id}"
}

resource "aws_apigatewayv2_route" "analytics" {
  for_each = local.analytics_routes

  api_id    = aws_apigatewayv2_api.weather_api.id
  route_key = each.value.route_key
  target    = "integrations/${aws_apigatewayv2_integration.analytics.id}"
}

resource "aws_lambda_permission" "allow_apigw_retrieval" {
  for_each = local.retrieval_routes

  statement_id  = "AllowHttpApiInvoke-retrieval-${each.key}"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.retrieval.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.weather_api.execution_arn}/*/${each.value.path_pattern}"
}

resource "aws_lambda_permission" "allow_apigw_ingestion" {
  for_each = local.ingestion_routes

  statement_id  = "AllowHttpApiInvoke-ingestion-${each.key}"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingestion.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.weather_api.execution_arn}/*/${each.value.path_pattern}"
}

resource "aws_lambda_permission" "allow_apigw_processing" {
  for_each = local.processing_routes

  statement_id  = "AllowHttpApiInvoke-processing-${each.key}"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.processing.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.weather_api.execution_arn}/*/${each.value.path_pattern}"
}

resource "aws_lambda_permission" "allow_apigw_analytics" {
  for_each = local.analytics_routes

  statement_id  = "AllowHttpApiInvoke-analytics-${each.key}"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.analytics.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.weather_api.execution_arn}/*/${each.value.path_pattern}"
}

resource "aws_cloudwatch_event_rule" "daily_all_hubs_ingestion" {
  name                = local.daily_ingestion_rule_name
  description         = "Invokes the ingestion Lambda daily for all configured hubs"
  schedule_expression = "cron(0 2 * * ? *)"

  tags = {
    Environment = "dev"
    Project     = "seng3011"
  }
}

resource "aws_cloudwatch_event_target" "daily_all_hubs_ingestion" {
  rule      = aws_cloudwatch_event_rule.daily_all_hubs_ingestion.name
  target_id = "weather-ingestion-handler"
  arn       = aws_lambda_function.ingestion.arn
  input     = jsonencode({})
}

resource "aws_lambda_permission" "allow_eventbridge_ingestion" {
  statement_id  = "AllowEventBridgeInvoke-ingestion"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingestion.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily_all_hubs_ingestion.arn
}

resource "aws_lambda_permission" "allow_s3_processing" {
  statement_id  = "AllowS3Invoke-processing"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.processing.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.seng_3011_bkt.arn
}

resource "aws_lambda_permission" "allow_s3_analytics" {
  statement_id  = "AllowS3Invoke-analytics"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.analytics.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.seng_3011_bkt.arn
}

resource "aws_s3_bucket_notification" "lambda_triggers" {
  bucket = aws_s3_bucket.seng_3011_bkt.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.processing.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "raw/weather/"
    filter_suffix       = ".json"
  }

  lambda_function {
    lambda_function_arn = aws_lambda_function.analytics.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "processed/weather/"
    filter_suffix       = ".json"
  }

  depends_on = [
    aws_lambda_permission.allow_s3_processing,
    aws_lambda_permission.allow_s3_analytics,
  ]
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
  value = local.api_base_url
}

output "weather_ingest_url_example" {
  value = "${local.api_base_url}/ese/v1/ingest/weather/${local.seeded_hub_id}"
}

output "daily_all_hubs_ingestion_rule_name" {
  value = aws_cloudwatch_event_rule.daily_all_hubs_ingestion.name
}

output "weather_retrieve_raw_url_example" {
  value = "${local.api_base_url}/ese/v1/retrieve/raw/weather/${local.seeded_hub_id}?date=${local.seeded_date}"
}

output "weather_retrieve_processed_url_example" {
  value = "${local.api_base_url}/ese/v1/retrieve/processed/weather/${local.seeded_hub_id}?date=${local.seeded_date}"
}

output "weather_process_url_example" {
  value = "${local.api_base_url}/ese/v1/process/weather"
}

output "risk_location_url_example" {
  value = "${local.api_base_url}/ese/v1/risk/location/${local.seeded_hub_id}?date=${local.seeded_date}"
}
