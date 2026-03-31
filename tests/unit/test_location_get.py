import json
import boto3
from decimal import Decimal
from constants import STATUS_OK, STATUS_BAD_REQUEST, STATUS_NOT_FOUND
from lambdas.location.handler import lambda_handler

def test_location_get_success(setup_dynamodb):
    table = boto3.resource("dynamodb", region_name="us-east-1").Table("locations")
    table.put_item(
        Item={
            "hub_id": "LOC_123",
            "lat_lon": "1.234:5.678",
            "name": "Port 1",
            "lat": Decimal("1.234"),
            "lon": Decimal("5.678"),
            "type": "dynamic",
            "created_at": "2026-03-29T00:00:00",
        }
    )

    event = {
        "httpMethod": "GET",
        "pathParameters": {"hub_id": "LOC_123"},
    }

    response = lambda_handler(event, None)
    assert response["statusCode"] == STATUS_OK
    assert json.loads(response["body"]) == {
        "hub_id": "LOC_123",
        "lat_lon": "1.234:5.678",
        "name": "Port 1",
        "lat": 1.234,
        "lon": 5.678,
        "type": "dynamic",
        "created_at": "2026-03-29T00:00:00",
    }


def test_missing_hub_id(setup_dynamodb):
    event = {
        "httpMethod": "GET",
        "pathParameters": {},
    }

    response = lambda_handler(event, None)
    assert response["statusCode"] == STATUS_BAD_REQUEST
    assert json.loads(response["body"]) == {"error": "Missing hub_id"}


def test_invalid_hub_id(setup_dynamodb):
    event = {
        "httpMethod": "GET",
        "pathParameters": {"hub_id": "LOC_UNKNOWN"},
    }

    response = lambda_handler(event, None)
    assert response["statusCode"] == STATUS_NOT_FOUND
    assert json.loads(response["body"]) == {"error": "Invalid hub_id"}
