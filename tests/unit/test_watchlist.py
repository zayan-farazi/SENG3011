import json
import boto3
from lambdas.watchlist.handler import lambda_handler
from constants import STATUS_OK, STATUS_BAD_REQUEST
from unittest.mock import patch, Mock


TABLE_NAME = "watchlist"

@patch("lambdas.watchlist.handler.requests.get")
def test_post_add_email_success(mock_get, setup_dynamodb):
    mock_get.return_value = Mock(status_code=200)
    table = boto3.resource("dynamodb", region_name="us-east-1").Table(TABLE_NAME)

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
    mock_get.return_value = Mock(status_code=200)
    table = boto3.resource("dynamodb", region_name="us-east-1").Table(TABLE_NAME)

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


def test_missing_params(setup_dynamodb):
    event = {
        "httpMethod": "POST",
        "pathParameters": {}
    }

    response = lambda_handler(event, None)

    assert response["statusCode"] == STATUS_BAD_REQUEST

@patch("lambdas.watchlist.handler.requests.get")
def test_invalid_method(mock_get, setup_dynamodb):
    mock_get.return_value = Mock(status_code=200)
    event = {
        "httpMethod": "GET",
        "pathParameters": {
            "hub_id": "H001",
            "email": "test@example.com"
        }
    }

    response = lambda_handler(event, None)

    assert response["statusCode"] == STATUS_BAD_REQUEST