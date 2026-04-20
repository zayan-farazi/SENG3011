import json
import boto3
from boto3.dynamodb.conditions import Key
import constants
from constants import STATUS_OK, STATUS_BAD_REQUEST, STATUS_UNAUTHORIZED
from tests.test_constants import HUB_NAME, LAT, LON
from lambdas.location.handler import lambda_handler


AUTH_CONTEXT = {
    "authorizer": {
        "jwt": {
            "claims": {
                "sub": "user-123",
                "email": "user@example.com",
                "email_verified": "true",
            }
        }
    }
}

def test_location_create_success(setup_dynamodb):
    event = {
        "httpMethod": "POST",
        "body": json.dumps({"lat": LAT, "lon": LON, "name": HUB_NAME}),
        "requestContext": AUTH_CONTEXT,
    }

    response = lambda_handler(event, None)
    assert response["statusCode"] == STATUS_OK

    body = json.loads(response["body"])
    assert "hub_id" in body
    assert body["hub_id"].startswith("LOC_")

    table = boto3.resource("dynamodb", region_name=constants.DEFAULT_REGION).Table("locations")
    item = table.get_item(Key={"hub_id": body["hub_id"]}).get("Item")
    assert item is not None
    assert item["name"] == HUB_NAME
    assert float(item["lat"]) == LAT
    assert float(item["lon"]) == LON

def test_existing_lat_lon_returns_same_hub(setup_dynamodb):
    first_event = {
        "httpMethod": "POST",
        "body": json.dumps({"lat": LAT, "lon": LON, "name": HUB_NAME}),
        "requestContext": AUTH_CONTEXT,
    }
    first_response = lambda_handler(first_event, None)
    assert first_response["statusCode"] == STATUS_OK
    first_body = json.loads(first_response["body"])

    second_event = {
        "httpMethod": "POST",
        "body": json.dumps({"lat": LAT, "lon": LON, "name": "Port 2"}),
        "requestContext": AUTH_CONTEXT,
    }
    second_response = lambda_handler(second_event, None)
    assert second_response["statusCode"] == STATUS_OK
    second_body = json.loads(second_response["body"])
    assert second_body["hub_id"] == first_body["hub_id"]

    table = boto3.resource("dynamodb", region_name=constants.DEFAULT_REGION).Table("locations")
    query_result = table.query(
        IndexName="lat-lon-index",
        KeyConditionExpression=Key("lat_lon").eq(f"{LAT}:{LON}"),
    )
    assert len(query_result["Items"]) == 1
    assert query_result["Items"][0]["hub_id"] == first_body["hub_id"]
    assert query_result["Items"][0]["name"] == HUB_NAME


def test_missing_name(setup_dynamodb):
    event = {
        "httpMethod": "POST",
        "body": json.dumps({"lat": LAT, "lon": LON}),
        "requestContext": AUTH_CONTEXT,
    }

    response = lambda_handler(event, None)
    assert response["statusCode"] == STATUS_BAD_REQUEST
    assert json.loads(response["body"]) == {"error": "Missing required field name"}

def test_missing_body(setup_dynamodb):
    event = {"httpMethod": "POST", "requestContext": AUTH_CONTEXT}

    response = lambda_handler(event, None)
    assert response["statusCode"] == STATUS_BAD_REQUEST
    assert json.loads(response["body"]) == {"error": "Request body is required"}

def test_invalid_name(setup_dynamodb):
    event = {
        "httpMethod": "POST",
        "body": json.dumps({"lat": LAT, "lon": LON, "name": "Bad!Name"}),
        "requestContext": AUTH_CONTEXT,
    }

    response = lambda_handler(event, None)
    assert response["statusCode"] == STATUS_BAD_REQUEST
    assert json.loads(response["body"])["error"] == (
        "Name can contain only letters, numbers, apostrophe, comma, dash, and spaces."
    )

def test_invalid_lat(setup_dynamodb):
    event = {
        "httpMethod": "POST",
        "body": json.dumps({"lat": 100, "lon": LON, "name": HUB_NAME}),
        "requestContext": AUTH_CONTEXT,
    }

    response = lambda_handler(event, None)
    assert response["statusCode"] == STATUS_BAD_REQUEST
    assert json.loads(response["body"])["error"] == "Latitude must be between -90 and 90."


def test_location_create_requires_auth(setup_dynamodb):
    event = {
        "httpMethod": "POST",
        "body": json.dumps({"lat": LAT, "lon": LON, "name": HUB_NAME}),
    }

    response = lambda_handler(event, None)

    assert response["statusCode"] == STATUS_UNAUTHORIZED
    assert json.loads(response["body"]) == {"error": "Unauthorized"}
