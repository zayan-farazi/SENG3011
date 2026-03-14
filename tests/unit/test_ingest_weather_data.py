import json
from unittest.mock import patch
import os

os.environ["DATA_BUCKET"] = "test-bucket"
os.environ["API_KEY"] = "test-key"
from lambdas.ingestion.handler import lambda_handler

@patch("lambdas.ingestion.handler.store_weather")
@patch("lambdas.ingestion.handler.fetch_weather")
@patch("lambdas.ingestion.handler.load_hubs")
def test_lambda_handler_success_single_hub(mock_load, mock_fetch, mock_store):

    fake_event = {"pathParameters": {"hubId": "hub001"}}

    mock_load.return_value = {
        "hub001": {"lat": 10, "lon": 20},
        "hub002": {"lat": 30, "lon": 40},
    }

    mock_fetch.return_value = '{"temperature":25}'

    result = lambda_handler(fake_event, None)

    mock_store.assert_called_once()
    args = mock_store.call_args[0]

    assert args[0] == "hub001"
    assert args[2] == '{"temperature":25}'
    assert result["statusCode"] == 200


@patch("lambdas.ingestion.handler.store_weather")
@patch("lambdas.ingestion.handler.fetch_weather")
@patch("lambdas.ingestion.handler.load_hubs")
def test_lambda_handler_all_hubs(mock_load, mock_fetch, mock_store):

    fake_event = {}

    mock_load.return_value = {
        "hub001": {"lat": 10, "lon": 20},
        "hub002": {"lat": 30, "lon": 40},
    }

    mock_fetch.return_value = '{"temperature":25}'

    result = lambda_handler(fake_event, None)

    assert mock_store.call_count == 2
    assert result["statusCode"] == 200


@patch("lambdas.ingestion.handler.load_hubs")
def test_lambda_handler_invalid_hub(mock_load):

    fake_event = {"pathParameters": {"hubId": "badHub"}}

    mock_load.return_value = {
        "hub001": {"lat": 10, "lon": 20}
    }

    result = lambda_handler(fake_event, None)

    assert result["statusCode"] == 400
    assert json.loads(result["body"])["error"] == "Invalid hub_id"


@patch("lambdas.ingestion.handler.bucket_name", None)
def test_lambda_handler_missing_bucket():

    result = lambda_handler({}, None)

    assert result["statusCode"] == 500


@patch("lambdas.ingestion.handler.api_key", None)
def test_lambda_handler_missing_api_key():

    result = lambda_handler({}, None)

    assert result["statusCode"] == 500
