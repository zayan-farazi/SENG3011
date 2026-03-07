import json
from lambdas.retrieval import handler
from constants import *

PATH = "/ese/v1/retrieve/raw/weather"

def test_raw_valid(setup_s3):
    s3 = setup_s3
    with open(RAW_WEATHER_DATA_FILE, "r") as f:
        pirate_raw = json.load(f)

    s3.put_object(
        Bucket=TEST_BUCKET_NAME,
        Key=f"raw/weather/{HUB_ID_1}/{DATE_1}.json",
        Body=json.dumps(pirate_raw)
    )

    event = {
        "rawPath": PATH,
        "pathParameters": { "hub_id": HUB_ID_1 },
        "queryStringParameters": { "date": DATE_1 }
    }

    response = handler(event, None)
    assert response["statusCode"] == STATUS_OK
    assert json.loads(response["body"]) == pirate_raw

def test_raw_missing_hub():
    event = {
        "rawPath": PATH,
        "queryStringParameters": { "date": DATE_1 }
    }

    response = handler(event, None)
    assert response["statusCode"] == STATUS_BAD_REQUEST
    assert json.loads(response["body"]) == {"error": "Missing hub_id"}

def test_raw_invalid_hub():
    event = {
        "rawPath": PATH,
        "pathParameters": { "hub_id": HUB_INVALID },
        "queryStringParameters": { "date": DATE_1 }
    }

    response = handler(event, None)
    assert response["statusCode"] == STATUS_BAD_REQUEST
    assert json.loads(response["body"]) == {"error": "Invalid hub_id"}

def test_raw_missing_date():
    event = {
        "rawPath": PATH,
        "pathParameters": { "hub_id": HUB_ID_1 },
    }

    response = handler(event, None)
    assert response["statusCode"] == STATUS_BAD_REQUEST
    assert json.loads(response["body"]) == {"error": "Missing date"}

def test_raw_invalid_date():
    event = {
        "rawPath": PATH,
        "pathParameters": { "hub_id": HUB_ID_1 },
        "queryStringParameters": { "date": DATE_INVALID }
    }

    response = handler(event, None)
    assert response["statusCode"] == STATUS_BAD_REQUEST
    assert json.loads(response["body"]) == {"error": "Invalid date format. Use DD-MM-YYYY"}

def test_raw_object_not_found():
    event = {
        "rawPath": PATH,
        "pathParameters": { "hub_id": HUB_ID_1 },
        "queryStringParameters": { "date": DATE_1 }
    }

    response = handler(event, None)
    assert response["statusCode"] == STATUS_NOT_FOUND
    assert json.loads(response["body"]) == {"error": "Data for hub_id and date not found"}