import json
import boto3
from decimal import Decimal
import constants
from constants import STATUS_OK, STATUS_BAD_REQUEST, STATUS_NOT_FOUND
from tests.test_constants import HUB_NAME, LAT, LON
from lambdas.location.handler import lambda_handler

def test_location_get_success(setup_dynamodb):
    table = boto3.resource("dynamodb", region_name=constants.DEFAULT_REGION).Table("locations")
    table.put_item(
        Item={
            "hub_id": "LOC_123",
            "lat_lon": f"{LAT}:{LON}",
            "name": HUB_NAME,
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
        "lat_lon": f"{LAT}:{LON}",
        "name": HUB_NAME,
        "lat": LAT,
        "lon": LON,
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


def test_invalid_hub_id(setup_s3_dynamodb):
    event = {
        "httpMethod": "GET",
        "pathParameters": {"hub_id": "LOC_UNKNOWN"},
    }

    response = lambda_handler(event, None)
    assert response["statusCode"] == STATUS_NOT_FOUND
    assert json.loads(response["body"]) == {"error": "Invalid hub_id"}


def test_location_list_all_hubs(setup_s3_dynamodb):
    table = boto3.resource("dynamodb", region_name=constants.DEFAULT_REGION).Table("locations")
    table.put_item(
        Item={
            "hub_id": "LOC_123",
            "lat_lon": f"{LAT}:{LON}",
            "name": HUB_NAME,
            "lat": Decimal("1.234"),
            "lon": Decimal("5.678"),
            "type": "dynamic",
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
    hubs = json.loads(response["body"])["hubs"]
    assert any(hub["hub_id"] == "H001" for hub in hubs)
    assert any(hub["hub_id"] == "LOC_123" for hub in hubs)


def test_location_list_no_hubs(setup_s3_dynamodb):
    boto3.resource("dynamodb", region_name=constants.DEFAULT_REGION).Table("locations")

    event = {
        "httpMethod": "GET",
        "rawPath": "/ese/v1/location/list",
        "pathParameters": {},
    }

    response = lambda_handler(event, None)
    assert response["statusCode"] == STATUS_OK
    assert any(hub["hub_id"] == "H001" for hub in json.loads(response["body"])["hubs"])


def test_location_list_filtered_by_type(setup_s3_dynamodb):
    table = boto3.resource("dynamodb", region_name=constants.DEFAULT_REGION).Table("locations")
    table.put_item(
        Item={
            "hub_id": "LOC_123",
            "lat_lon": f"{LAT}:{LON}",
            "name": HUB_NAME,
            "lat": Decimal("1.234"),
            "lon": Decimal("5.678"),
            "type": "dynamic",
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
            {"hub_id": "LOC_123", "name": HUB_NAME, "lat": LAT, "lon": LON},
        ]
    }


def test_location_get_monitored_hub_from_catalog(setup_s3_dynamodb):
    event = {
        "httpMethod": "GET",
        "pathParameters": {"hub_id": "H001"},
    }

    response = lambda_handler(event, None)

    assert response["statusCode"] == STATUS_OK
    assert json.loads(response["body"]) == {
        "hub_id": "H001",
        "name": "Port of Singapore",
        "lat": 1.264,
        "lon": 103.82,
        "type": "monitored",
    }


def test_location_list_monitored_from_catalog(setup_s3_dynamodb):
    event = {
        "httpMethod": "GET",
        "rawPath": "/ese/v1/location/list",
        "pathParameters": {},
        "queryStringParameters": {"type": "monitored"},
    }

    response = lambda_handler(event, None)

    assert response["statusCode"] == STATUS_OK
    hubs = json.loads(response["body"])["hubs"]
    assert any(hub["hub_id"] == "H001" for hub in hubs)
    assert all(not hub["hub_id"].startswith("LOC_") for hub in hubs)


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
        "error": "Query parameter 'type' must be one of: dynamic or monitored"
    }
