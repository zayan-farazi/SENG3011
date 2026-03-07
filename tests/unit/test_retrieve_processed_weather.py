import json
from lambdas.retrieval.handler import lambda_handler
from constants import *

PATH = "/ese/v1/retrieve/processed/weather"

def test_processed_valid(setup_s3):
    s3 = setup_s3
    with open(PROCESSED_WEATHER_DATA_FILE, "r") as f:
        processed_data = json.load(f)

    s3.put_object(
        Bucket=TEST_BUCKET_NAME,
        Key=f"processed/weather/{HUB_ID_1}/{DATE_1}.json",
        Body=json.dumps(processed_data)
    )

    event = {
        "rawPath": PATH,
        "pathParameters": { "hub_id": HUB_ID_1 },
        "queryStringParameters": { "date": DATE_1 }
    }

    response = lambda_handler(event, None)
    assert response["statusCode"] == STATUS_OK
    assert json.loads(response["body"]) == processed_data

def test_processed_missing_hub():
    event = {
        "rawPath": PATH,
        "pathParameters": { },
        "queryStringParameters": { "date": DATE_1 }
    }

    response = lambda_handler(event, None)
    assert response["statusCode"] == STATUS_BAD_REQUEST
    assert json.loads(response["body"]) == {"error": "Missing hub_id"}

def test_processed_invalid_hub(setup_s3):
    event = {
        "rawPath": PATH,
        "pathParameters": { "hub_id": HUB_INVALID },
        "queryStringParameters": { "date": DATE_1 }
    }

    response = lambda_handler(event, None)
    assert response["statusCode"] == STATUS_BAD_REQUEST
    assert json.loads(response["body"]) == {"error": "Invalid hub_id"}

def test_processed_missing_date():
    event = {
        "rawPath": PATH,
        "pathParameters": { "hub_id": HUB_ID_1 },
    }

    response = lambda_handler(event, None)
    assert response["statusCode"] == STATUS_BAD_REQUEST
    assert json.loads(response["body"]) == {"error": "Missing date"}

def test_processed_invalid_date():
    event = {
        "rawPath": PATH,
        "pathParameters": { "hub_id": HUB_ID_1 },
        "queryStringParameters": { "date": DATE_INVALID }
    }

    response = lambda_handler(event, None)
    assert response["statusCode"] == STATUS_BAD_REQUEST
    assert json.loads(response["body"]) == {"error": "Invalid date format. Use DD-MM-YYYY"}

def test_processed_object_not_found(setup_s3):
    event = {
        "rawPath": PATH,
        "pathParameters": { "hub_id": HUB_ID_1 },
        "queryStringParameters": { "date": DATE_1 }
    }

    response = lambda_handler(event, None)
    assert response["statusCode"] == STATUS_NOT_FOUND
    assert json.loads(response["body"]) == {"error": "Data for hub_id and date not found"}