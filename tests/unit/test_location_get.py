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


def test_location_list_all_hubs(setup_dynamodb):
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
    table.put_item(
        Item={
            "hub_id": "H001",
            "lat_lon": "2.345:6.789",
            "name": "Port 2",
            "lat": Decimal("2.345"),
            "lon": Decimal("6.789"),
            "type": "scheduled",
            "created_at": "2026-03-29T00:00:00",
        }
    )

    event = {
        "httpMethod": "GET",
        "rawPath": "/ese/v1/location/list",
        "pathParameters": {},
    }

    response = lambda_handler(event, None)
    assert response["statusCode"] == STATUS_OK
    assert json.loads(response["body"]) == {
        "hubs": [
            {"hub_id": "H001", "name": "Port 2", "lat": 2.345, "lon": 6.789},
            {"hub_id": "LOC_123", "name": "Port 1", "lat": 1.234, "lon": 5.678},
        ]
    }


def test_location_list_no_hubs(setup_dynamodb):
    boto3.resource("dynamodb", region_name="us-east-1").Table("locations")

    event = {
        "httpMethod": "GET",
        "rawPath": "/ese/v1/location/list",
        "pathParameters": {},
    }

    response = lambda_handler(event, None)
    assert response["statusCode"] == STATUS_OK
    assert json.loads(response["body"]) == { "hubs": [] }


def test_location_list_filtered_by_type(setup_dynamodb):
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
    table.put_item(
        Item={
            "hub_id": "H001",
            "lat_lon": "2.345:6.789",
            "name": "Port 2",
            "lat": Decimal("2.345"),
            "lon": Decimal("6.789"),
            "type": "scheduled",
            "created_at": "2026-03-29T00:00:00",
        }
    )

    event = {
        "httpMethod": "GET",
        "rawPath": "/ese/v1/location/list",
        "pathParameters": {},
        "queryStringParameters": {"type": "dynamic"},
    }

    response = lambda_handler(event, None)
    assert response["statusCode"] == STATUS_OK
    assert json.loads(response["body"]) == {
        "hubs": [
            {"hub_id": "LOC_123", "name": "Port 1", "lat": 1.234, "lon": 5.678},
        ]
    }


def test_location_list_invalid_type(setup_dynamodb):
    event = {
        "httpMethod": "GET",
        "rawPath": "/ese/v1/location/list",
        "pathParameters": {},
        "queryStringParameters": {"type": "invalid"},
    }

    response = lambda_handler(event, None)
    assert response["statusCode"] == STATUS_BAD_REQUEST
    assert json.loads(response["body"]) == {
        "error": "Query parameter 'type' must be one of: dynamic or scheduled"
    }
