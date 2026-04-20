import json
from unittest.mock import patch, Mock
from lambdas.watchlist.handler import lambda_handler
from constants import STATUS_OK, STATUS_BAD_REQUEST


@patch("lambdas.watchlist.handler.boto3.resource")
def test_get_messages_success(mock_boto, setup_dynamodb):

    mock_table = Mock()
    mock_boto.return_value.Table.return_value = mock_table

    mock_table.query.return_value = {
        "Items": [
            {
                "email": "test@example.com",
                "timestamp": "2026-04-20T10:00:00Z",
                "message": "Hub H001 critical"
            },
            {
                "email": "test@example.com",
                "timestamp": "2026-04-20T09:00:00Z",
                "message": "Hub H002 warning"
            }
        ]
    }

    event = {
        "httpMethod": "GET",
        "routeKey": "GET /ese/v1/watchlist/messages/{email}",
        "pathParameters": {
            "email": "test@example.com"
        }
    }

    response = lambda_handler(event, None)

    assert response["statusCode"] == STATUS_OK

    body = json.loads(response["body"])

    assert "messages" in body
    assert len(body["messages"]) == 2

    assert body["messages"][0]["message"] == "Hub H001 critical"
    assert body["messages"][1]["message"] == "Hub H002 warning"


@patch("lambdas.watchlist.handler.boto3.resource")
def test_get_messages_empty(mock_boto, setup_dynamodb):

    mock_table = Mock()
    mock_boto.return_value.Table.return_value = mock_table

    mock_table.query.return_value = {
        "Items": []
    }

    event = {
        "httpMethod": "GET",
        "routeKey": "GET /ese/v1/watchlist/messages/{email}",
        "pathParameters": {
            "email": "test@example.com"
        }
    }

    response = lambda_handler(event, None)

    assert response["statusCode"] == STATUS_OK

    body = json.loads(response["body"])
    assert body["messages"] == []


def test_get_messages_missing_email(setup_dynamodb):

    event = {
        "httpMethod": "GET",
        "routeKey": "GET /ese/v1/watchlist/messages/{email}",
        "pathParameters": {}
    }

    response = lambda_handler(event, None)

    assert response["statusCode"] == STATUS_BAD_REQUEST

    body = json.loads(response["body"])["message"]
    assert body["error"] == "Missing email"


def test_get_messages_invalid_email(setup_dynamodb):

    event = {
        "httpMethod": "GET",
        "routeKey": "GET /ese/v1/watchlist/messages/{email}",
        "pathParameters": {
            "email": "bad-email"
        }
    }

    response = lambda_handler(event, None)

    assert response["statusCode"] == STATUS_BAD_REQUEST

    body = json.loads(response["body"])["message"]
    assert body["error"] == "Invalid email"