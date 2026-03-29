import json
import boto3
from boto3.dynamodb.conditions import Key
from constants import STATUS_OK, STATUS_BAD_REQUEST
from lambdas.location.handler import lambda_handler

def test_location_create_success(setup_dynamodb):
    event = {
        "httpMethod": "POST",
        "body": json.dumps({"lat": 1.234, "lon": 5.678, "name": "Port 1"}),
    }

    response = lambda_handler(event, None)
    assert response["statusCode"] == STATUS_OK

    body = json.loads(response["body"])
    assert "hub_id" in body
    assert body["hub_id"].startswith("LOC_")

    table = boto3.resource("dynamodb", region_name="us-east-1").Table("locations")
    item = table.get_item(Key={"hub_id": body["hub_id"]}).get("Item")
    assert item is not None
    assert item["name"] == "Port 1"
    assert float(item["lat"]) == 1.234
    assert float(item["lon"]) == 5.678

def test_existing_lat_lon_returns_same_hub(setup_dynamodb):
    first_event = {
        "httpMethod": "POST",
        "body": json.dumps({"lat": 1.234, "lon": 5.678, "name": "Port 1"}),
    }

    first_response = lambda_handler(first_event, None)
    assert first_response["statusCode"] == STATUS_OK
    first_body = json.loads(first_response["body"])

    second_event = {
        "httpMethod": "POST",
        "body": json.dumps({"lat": 1.234, "lon": 5.678, "name": "Port 2"}),
    }

    second_response = lambda_handler(second_event, None)
    assert second_response["statusCode"] == STATUS_OK
    second_body = json.loads(second_response["body"])
    assert second_body["hub_id"] == first_body["hub_id"]

    table = boto3.resource("dynamodb", region_name="us-east-1").Table("locations")
    query_result = table.query(
        IndexName="lat-lon-index",
        KeyConditionExpression=Key("lat_lon").eq("1.234:5.678"),
    )
    assert len(query_result["Items"]) == 1
    assert query_result["Items"][0]["hub_id"] == first_body["hub_id"]
    assert query_result["Items"][0]["name"] == "Port 1"


def test_missing_name(setup_dynamodb):
    event = {
        "httpMethod": "POST",
        "body": json.dumps({"lat": 1.234, "lon": 5.678}),
    }

    response = lambda_handler(event, None)
    assert response["statusCode"] == STATUS_BAD_REQUEST
    assert json.loads(response["body"]) == {"error": "Missing required field name"}

def test_missing_body(setup_dynamodb):
    event = {"httpMethod": "POST"}

    response = lambda_handler(event, None)
    assert response["statusCode"] == STATUS_BAD_REQUEST
    assert json.loads(response["body"]) == {"error": "Request body is required"}

def test_invalid_name(setup_dynamodb):
    event = {
        "httpMethod": "POST",
        "body": json.dumps({"lat": 1.234, "lon": 5.678, "name": "Bad!Name"}),
    }

    response = lambda_handler(event, None)
    assert response["statusCode"] == STATUS_BAD_REQUEST
    assert json.loads(response["body"])["error"] == (
        "Name can contain only letters, numbers, apostrophe, comma, dash, and spaces."
    )
