import json
from unittest.mock import Mock, patch

import boto3
import constants
from constants import (
    STATUS_BAD_REQUEST,
    STATUS_FORBIDDEN,
    STATUS_OK,
    STATUS_UNAUTHORIZED,
)
from lambdas.watchlist.handler import lambda_handler


TABLE_NAME = "watchlist"


def auth_event(method, path_parameters=None, claims=None):
    return {
        "httpMethod": method,
        "pathParameters": path_parameters or {},
        "requestContext": {
            "authorizer": {
                "jwt": {
                    "claims": claims
                    or {
                        "sub": "user-123",
                        "email": "test@example.com",
                        "email_verified": "true",
                    }
                }
            }
        },
    }


@patch("lambdas.watchlist.handler.requests.get")
def test_post_add_watch_success(mock_get, setup_dynamodb):
    mock_get.return_value = Mock(status_code=STATUS_OK)
    table = boto3.resource("dynamodb", region_name=constants.DEFAULT_REGION).Table(TABLE_NAME)

    response = lambda_handler(auth_event("POST", {"hub_id": "H001"}), None)

    assert response["statusCode"] == STATUS_OK
    assert "added" in json.loads(response["body"])["message"]

    result = table.get_item(Key={"user_id": "user-123", "hub_id": "H001"})
    assert result["Item"]["notification_email"] == "test@example.com"


@patch("lambdas.watchlist.handler.requests.get")
def test_delete_watch_success(mock_get, setup_dynamodb):
    mock_get.return_value = Mock(status_code=STATUS_OK)
    table = boto3.resource("dynamodb", region_name=constants.DEFAULT_REGION).Table(TABLE_NAME)
    table.put_item(
        Item={
            "user_id": "user-123",
            "hub_id": "H001",
            "notification_email": "test@example.com",
            "created_at": "2026-04-20T00:00:00+00:00",
        }
    )

    response = lambda_handler(auth_event("DELETE", {"hub_id": "H001"}), None)

    assert response["statusCode"] == STATUS_OK
    assert "removed" in json.loads(response["body"])["message"]
    deleted = table.get_item(Key={"user_id": "user-123", "hub_id": "H001"})
    assert "Item" not in deleted


def test_list_watchlist_returns_only_current_user(setup_dynamodb):
    table = boto3.resource("dynamodb", region_name=constants.DEFAULT_REGION).Table(TABLE_NAME)
    table.put_item(
        Item={
            "user_id": "user-123",
            "hub_id": "H001",
            "notification_email": "test@example.com",
            "created_at": "2026-04-20T00:00:00+00:00",
        }
    )
    table.put_item(
        Item={
            "user_id": "user-999",
            "hub_id": "H002",
            "notification_email": "other@example.com",
            "created_at": "2026-04-20T00:00:00+00:00",
        }
    )

    response = lambda_handler(auth_event("GET"), None)

    assert response["statusCode"] == STATUS_OK
    assert json.loads(response["body"]) == {
        "hubs": [
            {
                "hub_id": "H001",
                "notification_email": "test@example.com",
                "created_at": "2026-04-20T00:00:00+00:00",
            }
        ]
    }


def test_watchlist_requires_auth(setup_dynamodb):
    response = lambda_handler({"httpMethod": "GET", "pathParameters": {}}, None)

    assert response["statusCode"] == STATUS_UNAUTHORIZED
    assert json.loads(response["body"]) == {"error": "Unauthorized"}


def test_watchlist_write_requires_verified_email(setup_dynamodb):
    response = lambda_handler(
        auth_event(
            "POST",
            {"hub_id": "H001"},
            {
                "sub": "user-123",
                "email": "test@example.com",
                "email_verified": "false",
            },
        ),
        None,
    )

    assert response["statusCode"] == STATUS_FORBIDDEN
    assert json.loads(response["body"]) == {"error": "Verified email required"}


@patch("lambdas.watchlist.handler.requests.get")
def test_invalid_hub_id(mock_get, setup_dynamodb):
    mock_get.return_value = Mock(status_code=constants.STATUS_NOT_FOUND)

    response = lambda_handler(auth_event("POST", {"hub_id": "H999"}), None)

    assert response["statusCode"] == STATUS_BAD_REQUEST
    assert json.loads(response["body"]) == {"error": "Invalid hub_id"}


def test_missing_hub_id_for_write(setup_dynamodb):
    response = lambda_handler(auth_event("POST"), None)

    assert response["statusCode"] == STATUS_BAD_REQUEST
    assert json.loads(response["body"]) == {"error": "Missing hub_id"}
