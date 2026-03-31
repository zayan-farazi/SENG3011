import json
from unittest.mock import patch, Mock
from lambdas.retrieval.handler import lambda_handler
from tests.test_constants import TEST_BUCKET_NAME, HUB_ID_1, HUB_INVALID, DATE_1, DATE_INVALID, PROCESSED_WEATHER_DATA_FILE
from constants import STATUS_OK, STATUS_BAD_REQUEST, STATUS_NOT_FOUND, RETRIEVE_PROCESSED_WEATHER_PATH

@patch("lambdas.retrieval.handler.requests.get")
def test_processed_valid(mock_get, setup_s3):
    mock_resp = Mock()
    mock_resp.status_code = STATUS_OK
    mock_get.return_value = mock_resp
    s3 = setup_s3
    with open(PROCESSED_WEATHER_DATA_FILE, "r") as f:
        processed_data = json.load(f)

    s3.put_object(
        Bucket=TEST_BUCKET_NAME,
        Key=f"processed/weather/{HUB_ID_1}/{DATE_1}.json",
        Body=json.dumps(processed_data)
    )

    event = {
        "rawPath": RETRIEVE_PROCESSED_WEATHER_PATH,
        "pathParameters": { "hub_id": HUB_ID_1 },
        "queryStringParameters": { "date": DATE_1 }
    }

    response = lambda_handler(event, None)
    assert response["statusCode"] == STATUS_OK
    assert json.loads(response["body"]) == processed_data

def test_processed_missing_hub():
    event = {
        "rawPath": RETRIEVE_PROCESSED_WEATHER_PATH,
        "pathParameters": { },
        "queryStringParameters": { "date": DATE_1 }
    }

    response = lambda_handler(event, None)
    assert response["statusCode"] == STATUS_BAD_REQUEST
    assert json.loads(response["body"]) == {"error": "Missing hub_id"}

@patch("lambdas.retrieval.handler.requests.get")
def test_processed_invalid_hub(mock_get, setup_s3):
    mock_resp = Mock()
    mock_resp.status_code = STATUS_NOT_FOUND
    mock_get.return_value = mock_resp
    event = {
        "rawPath": RETRIEVE_PROCESSED_WEATHER_PATH,
        "pathParameters": { "hub_id": HUB_INVALID },
        "queryStringParameters": { "date": DATE_1 }
    }

    response = lambda_handler(event, None)
    assert response["statusCode"] == STATUS_BAD_REQUEST
    assert json.loads(response["body"]) == {"error": "Invalid hub_id"}

def test_processed_missing_date():
    event = {
        "rawPath": RETRIEVE_PROCESSED_WEATHER_PATH,
        "pathParameters": { "hub_id": HUB_ID_1 },
    }

    response = lambda_handler(event, None)
    assert response["statusCode"] == STATUS_BAD_REQUEST
    assert json.loads(response["body"]) == {"error": "Missing date"}

def test_processed_invalid_date():
    event = {
        "rawPath": RETRIEVE_PROCESSED_WEATHER_PATH,
        "pathParameters": { "hub_id": HUB_ID_1 },
        "queryStringParameters": { "date": DATE_INVALID }
    }

    response = lambda_handler(event, None)
    assert response["statusCode"] == STATUS_BAD_REQUEST
    assert json.loads(response["body"]) == {"error": "Invalid date format. Use DD-MM-YYYY"}

@patch("lambdas.retrieval.handler.requests.get")
def test_processed_object_not_found(mock_get, setup_s3):
    mock_resp = Mock()
    mock_resp.status_code = STATUS_OK
    mock_get.return_value = mock_resp
    event = {
        "rawPath": RETRIEVE_PROCESSED_WEATHER_PATH,
        "pathParameters": { "hub_id": HUB_ID_1 },
        "queryStringParameters": { "date": DATE_1 }
    }

    response = lambda_handler(event, None)
    assert response["statusCode"] == STATUS_NOT_FOUND
    assert json.loads(response["body"]) == {"error": "Data for hub_id and date not found"}