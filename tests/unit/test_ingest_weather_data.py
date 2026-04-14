import os
import json
from unittest.mock import patch
from datetime import datetime, timezone
from tests.test_constants import TEST_BUCKET_NAME, HUB_ID_1
from constants import DATE_FORMAT, STATUS_OK, STATUS_BAD_REQUEST, STATUS_INTERNAL_SERVER_ERROR

os.environ["API_KEY"] = "test"
from lambdas.ingestion.handler import lambda_handler

@patch("lambdas.ingestion.handler.fetch_weather")
def test_lambda_handler_success_single_hub(mock_fetch, setup_s3):
    mock_fetch.return_value = '{"temperature":25}'

    event = {
        "pathParameters": {"hub_id": HUB_ID_1}
    }

    result = lambda_handler(event, None)

    assert result["statusCode"] == STATUS_OK

    today = datetime.now(timezone.utc).strftime(DATE_FORMAT)
 
    obj = setup_s3.get_object(
        Bucket=TEST_BUCKET_NAME,
        Key=f"raw/weather/{HUB_ID_1}/{today}.json"
    )

    stored_data = obj["Body"].read().decode()

    assert stored_data == '{"temperature":25}'


@patch("lambdas.ingestion.handler.fetch_weather")
def test_lambda_handler_all_hubs(mock_fetch, setup_s3):
    mock_fetch.return_value = '{"temperature":25}'

    event = {}

    result = lambda_handler(event, None)

    assert result["statusCode"] == STATUS_OK

    objects = setup_s3.list_objects_v2(
        Bucket=TEST_BUCKET_NAME,
        Prefix="raw/weather/"
    )

    assert "Contents" in objects
    assert len(objects["Contents"]) > 0


def test_lambda_handler_invalid_hub(setup_s3):
    event = {
        "pathParameters": {"hub_id": "invalid_hub"}
    }

    result = lambda_handler(event, None)

    assert result["statusCode"] == STATUS_BAD_REQUEST
    assert json.loads(result["body"])["error"] == "Invalid hub_id"

@patch.dict(os.environ, {"DATA_BUCKET": TEST_BUCKET_NAME, "API_KEY": ""})
def test_lambda_handler_missing_api_key():

    result = lambda_handler({}, None)

    assert result["statusCode"] == STATUS_INTERNAL_SERVER_ERROR
    assert json.loads(result["body"])["error"] == "Missing PirateWeather API key"

@patch.dict(os.environ, {"DATA_BUCKET": "", "API_KEY": "test"})
def test_lambda_handler_missing_bucket():

    result = lambda_handler({}, None)

    assert result["statusCode"] == STATUS_INTERNAL_SERVER_ERROR
    assert json.loads(result["body"])["error"] == "Missing DATA_BUCKET configuration"
