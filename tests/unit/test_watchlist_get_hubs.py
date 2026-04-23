import json
from unittest.mock import patch, Mock
from lambdas.watchlist.handler import lambda_handler
from constants import STATUS_OK, STATUS_BAD_REQUEST


@patch("lambdas.watchlist.handler.boto3.resource")
def test_get_hubs_success(mock_boto, setup_dynamodb):
    mock_table = Mock()

    mock_table.query.return_value = {
        "Items": [
            {"hub_id": "H001"},
            {"hub_id": "H002"}
        ]
    }

    mock_boto.return_value.Table.return_value = mock_table

    event = {
        "httpMethod": "GET",
        "routeKey": "GET /ese/v1/watchlist/{email}",
        "pathParameters": {
            "email": "test@example.com"
        }
    }

    response = lambda_handler(event, None)

    assert response["statusCode"] == STATUS_OK

    body = json.loads(response["body"])

    assert set(body["hubs"]) == {"H001", "H002"}


def test_get_hubs_missing_email(setup_dynamodb):
    event = {
        "httpMethod": "GET",
        "routeKey": "GET /ese/v1/watchlist/{email}",
        "pathParameters": {}
    }

    response = lambda_handler(event, None)

    assert response["statusCode"] == STATUS_BAD_REQUEST

    body = json.loads(response["body"])["message"]
    assert body["error"] == "Missing email"


def test_get_hubs_invalid_email(setup_dynamodb):
    event = {
        "httpMethod": "GET",
        "routeKey": "GET /ese/v1/watchlist/{email}",
        "pathParameters": {
            "email": "not-an-email"
        }
    }

    response = lambda_handler(event, None)

    assert response["statusCode"] == STATUS_BAD_REQUEST

    body = json.loads(response["body"])["message"]
    assert body["error"] == "Invalid email"