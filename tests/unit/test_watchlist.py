import json
import boto3  # type: ignore
import constants
from lambdas.watchlist.handler import lambda_handler
from constants import STATUS_OK, STATUS_BAD_REQUEST, STATUS_NOT_FOUND
from unittest.mock import patch, Mock


TABLE_NAME = "watchlist"

@patch("lambdas.watchlist.handler.requests.get")
def test_post_add_email_success(mock_get, setup_dynamodb):
    mock_get.return_value = Mock(status_code=STATUS_OK)
    table = boto3.resource("dynamodb", region_name=constants.DEFAULT_REGION).Table(TABLE_NAME)

    event = {
        "httpMethod": "POST",
        "pathParameters": {
            "hub_id": "H001",
            "email": "test@example.com"
        }
    }

    response = lambda_handler(event, None)

    assert response["statusCode"] == STATUS_OK
    assert "added" in json.loads(response["body"])["message"]

    result = table.get_item(Key={
        "hub_id": "H001",
        "email": "test@example.com"
    })
    assert "Item" in result

@patch("lambdas.watchlist.handler.requests.get")
def test_delete_email_success(mock_get, setup_dynamodb):
    mock_get.return_value = Mock(status_code=STATUS_OK)
    table = boto3.resource("dynamodb", region_name=constants.DEFAULT_REGION).Table(TABLE_NAME)

    table.put_item(Item={
        "hub_id": "H001",
        "email": "test@example.com"
    })

    event = {
        "httpMethod": "DELETE",
        "pathParameters": {
            "hub_id": "H001",
            "email": "test@example.com"
        }
    }

    response = lambda_handler(event, None)

    assert response["statusCode"] == STATUS_OK
    assert "removed" in json.loads(response["body"])["message"]
    deleted = table.get_item(Key={"hub_id": "H001", "email": "test@example.com"})
    assert "Item" not in deleted


def test_missing_params(setup_dynamodb):
    event = {
        "httpMethod": "POST",
        "pathParameters": {}
    }

    response = lambda_handler(event, None)

    assert response["statusCode"] == STATUS_BAD_REQUEST
    assert json.loads(response["body"])["message"]["error"] == "Missing hub_id or email"


@patch("lambdas.watchlist.handler.requests.get")
def test_invalid_email(mock_get, setup_dynamodb):
    mock_get.return_value = Mock(status_code=STATUS_OK)
    event = {
        "httpMethod": "POST",
        "pathParameters": {
            "hub_id": "H001",
            "email": "not-an-email"
        }
    }

    response = lambda_handler(event, None)

    assert response["statusCode"] == STATUS_BAD_REQUEST
    assert json.loads(response["body"])["message"]["error"] == "Invalid email"


@patch("lambdas.watchlist.handler.requests.get")
def test_invalid_hub_id(mock_get, setup_dynamodb):
    mock_get.return_value = Mock(status_code=STATUS_NOT_FOUND)
    event = {
        "httpMethod": "POST",
        "pathParameters": {
            "hub_id": "H999",
            "email": "test@example.com"
        }
    }

    response = lambda_handler(event, None)

    assert response["statusCode"] == STATUS_BAD_REQUEST
    assert json.loads(response["body"])["message"]["error"] == "Invalid hub_id"

@patch("lambdas.watchlist.handler.requests.get")
def test_invalid_method(mock_get, setup_dynamodb):
    mock_get.return_value = Mock(status_code=STATUS_OK)
    event = {
        "httpMethod": "GET",
        "pathParameters": {
            "hub_id": "H001",
            "email": "test@example.com"
        }
    }

    response = lambda_handler(event, None)

    assert response["statusCode"] == STATUS_BAD_REQUEST
