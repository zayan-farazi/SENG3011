import json
import boto3
from decimal import Decimal
from lambdas.retrieval.handler import lambda_handler
from tests.test_constants import TEST_BUCKET_NAME, HUB_ID_1, HUB_INVALID, DATE_1, DATE_INVALID, RAW_WEATHER_DATA_FILE
from constants import STATUS_OK, STATUS_BAD_REQUEST, STATUS_NOT_FOUND, RETRIEVE_RAW_WEATHER_PATH
import constants

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
        "rawPath": RETRIEVE_RAW_WEATHER_PATH,
        "pathParameters": { "hub_id": HUB_ID_1 },
        "queryStringParameters": { "date": DATE_1 }
    }

    response = lambda_handler(event, None)
    assert response["statusCode"] == STATUS_OK
    assert json.loads(response["body"]) == pirate_raw

def test_raw_missing_hub():
    event = {
        "rawPath": RETRIEVE_RAW_WEATHER_PATH,
        "pathParameters": { },
        "queryStringParameters": { "date": DATE_1 }
    }

    response = lambda_handler(event, None)
    assert response["statusCode"] == STATUS_BAD_REQUEST
    assert json.loads(response["body"]) == {"error": "Missing hub_id"}

def test_raw_invalid_hub(setup_s3):
    event = {
        "rawPath": RETRIEVE_RAW_WEATHER_PATH,
        "pathParameters": { "hub_id": HUB_INVALID },
        "queryStringParameters": { "date": DATE_1 }
    }

    response = lambda_handler(event, None)
    assert response["statusCode"] == STATUS_BAD_REQUEST
    assert json.loads(response["body"]) == {"error": "Invalid hub_id"}

def test_raw_missing_date():
    event = {
        "rawPath": RETRIEVE_RAW_WEATHER_PATH,
        "pathParameters": { "hub_id": HUB_ID_1 },
    }

    response = lambda_handler(event, None)
    assert response["statusCode"] == STATUS_BAD_REQUEST
    assert json.loads(response["body"]) == {"error": "Missing date"}

def test_raw_invalid_date():
    event = {
        "rawPath": RETRIEVE_RAW_WEATHER_PATH,
        "pathParameters": { "hub_id": HUB_ID_1 },
        "queryStringParameters": { "date": DATE_INVALID }
    }

    response = lambda_handler(event, None)
    assert response["statusCode"] == STATUS_BAD_REQUEST
    assert json.loads(response["body"]) == {"error": "Invalid date format. Use DD-MM-YYYY"}

def test_raw_object_not_found(setup_s3):
    event = {
        "rawPath": RETRIEVE_RAW_WEATHER_PATH,
        "pathParameters": { "hub_id": HUB_ID_1 },
        "queryStringParameters": { "date": DATE_1 }
    }

    response = lambda_handler(event, None)
    assert json.loads(response["body"]) == {"error": "Data for hub_id and date not found"}
    assert response["statusCode"] == STATUS_NOT_FOUND

def test_raw_valid_dynamic_hub(setup_s3_dynamodb):
    table = boto3.resource("dynamodb", region_name=constants.DEFAULT_REGION).Table("locations")
    table.put_item(
        Item={
            "hub_id": "LOC_TEST01",
            "lat_lon": "12.345:67.890",
            "name": "Dynamic Port",
            "lat": Decimal("12.345"),
            "lon": Decimal("67.890"),
            "type": "dynamic",
            "created_at": "2026-04-15T00:00:00Z",
        }
    )

    with open(RAW_WEATHER_DATA_FILE, "r") as f:
        pirate_raw = json.load(f)

    setup_s3_dynamodb.put_object(
        Bucket=TEST_BUCKET_NAME,
        Key=f"raw/weather/LOC_TEST01/{DATE_1}.json",
        Body=json.dumps(pirate_raw)
    )

    event = {
        "rawPath": RETRIEVE_RAW_WEATHER_PATH,
        "pathParameters": {"hub_id": "LOC_TEST01"},
        "queryStringParameters": {"date": DATE_1}
    }

    response = lambda_handler(event, None)
    assert response["statusCode"] == STATUS_OK
    assert json.loads(response["body"]) == pirate_raw

def test_raw_missing_data_bucket_env(monkeypatch):
    monkeypatch.delenv("DATA_BUCKET", raising=False)
    event = {
        "rawPath": RETRIEVE_RAW_WEATHER_PATH,
        "pathParameters": { "hub_id": HUB_ID_1 },
        "queryStringParameters": { "date": DATE_1 }
    }

    response = lambda_handler(event, None)
    assert response["statusCode"] == 500
    assert json.loads(response["body"]) == {"error": "Missing DATA_BUCKET configuration"}
