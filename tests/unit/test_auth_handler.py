import json
from unittest.mock import MagicMock, patch

from constants import STATUS_BAD_REQUEST, STATUS_FORBIDDEN, STATUS_OK
from lambdas.auth.handler import lambda_handler


def auth_event(method, raw_path="/ese/v1/auth/profile", body=None):
    event = {
        "httpMethod": method,
        "rawPath": raw_path,
        "routeKey": f"{method} {raw_path}",
        "requestContext": {
            "authorizer": {
                "jwt": {
                    "claims": {
                        "sub": "user-123",
                        "email": "user@example.com",
                        "email_verified": "true",
                        "cognito:username": "cognito-user-123",
                    }
                }
            }
        },
    }
    if body is not None:
        event["body"] = json.dumps(body)
    return event


@patch("lambdas.auth.handler._cognito_client")
@patch.dict(
    "os.environ",
    {"COGNITO_USER_POOL_ID": "pool-id", "COGNITO_USER_POOL_CLIENT_ID": "client-id"},
    clear=False,
)
def test_get_profile(mock_client_factory):
    mock_client = MagicMock()
    mock_client.admin_get_user.return_value = {
        "UserAttributes": [
            {"Name": "email", "Value": "user@example.com"},
            {"Name": "email_verified", "Value": "true"},
            {"Name": "preferred_username", "Value": "zayan"},
            {"Name": "custom:company_name", "Value": "OpenAI Logistics"},
        ]
    }
    mock_client_factory.return_value = mock_client

    response = lambda_handler(auth_event("GET"), None)

    assert response["statusCode"] == STATUS_OK
    assert json.loads(response["body"]) == {
        "username": "zayan",
        "email": "user@example.com",
        "email_verified": True,
        "company_name": "OpenAI Logistics",
    }


@patch("lambdas.auth.handler._cognito_client")
@patch.dict(
    "os.environ",
    {"COGNITO_USER_POOL_ID": "pool-id", "COGNITO_USER_POOL_CLIENT_ID": "client-id"},
    clear=False,
)
def test_update_profile(mock_client_factory):
    mock_client = MagicMock()
    mock_client_factory.return_value = mock_client

    response = lambda_handler(
        auth_event(
            "PUT",
            body={
                "username": "new-display-name",
                "email": "new@example.com",
                "company_name": "OpenAI Logistics",
            },
        ),
        None,
    )

    assert response["statusCode"] == STATUS_OK
    mock_client.admin_update_user_attributes.assert_called_once()


@patch("lambdas.auth.handler._cognito_client")
@patch.dict(
    "os.environ",
    {"COGNITO_USER_POOL_ID": "pool-id", "COGNITO_USER_POOL_CLIENT_ID": "client-id"},
    clear=False,
)
def test_update_profile_requires_supported_fields(mock_client_factory):
    mock_client_factory.return_value = MagicMock()

    response = lambda_handler(auth_event("PUT", body={"ignored": "value"}), None)

    assert response["statusCode"] == STATUS_BAD_REQUEST
    assert json.loads(response["body"]) == {"error": "No supported profile fields provided"}


@patch("lambdas.auth.handler._cognito_client")
@patch.dict(
    "os.environ",
    {"COGNITO_USER_POOL_ID": "pool-id", "COGNITO_USER_POOL_CLIENT_ID": "client-id"},
    clear=False,
)
def test_update_password(mock_client_factory):
    mock_client = MagicMock()
    mock_client_factory.return_value = mock_client

    response = lambda_handler(
        auth_event(
            "PUT",
            raw_path="/ese/v1/auth/password",
            body={
                "current_password": "OldPassword123!",
                "new_password": "NewPassword123!",
            },
        ),
        None,
    )

    assert response["statusCode"] == STATUS_OK
    mock_client.admin_initiate_auth.assert_called_once()
    mock_client.admin_set_user_password.assert_called_once()


@patch("lambdas.auth.handler._cognito_client")
@patch.dict(
    "os.environ",
    {"COGNITO_USER_POOL_ID": "pool-id", "COGNITO_USER_POOL_CLIENT_ID": "client-id"},
    clear=False,
)
def test_update_password_rejects_wrong_current_password(mock_client_factory):
    class NotAuthorizedException(Exception):
        pass

    mock_client = MagicMock()
    mock_client.exceptions = MagicMock(NotAuthorizedException=NotAuthorizedException)
    mock_client.admin_initiate_auth.side_effect = NotAuthorizedException()
    mock_client_factory.return_value = mock_client

    response = lambda_handler(
        auth_event(
            "PUT",
            raw_path="/ese/v1/auth/password",
            body={
                "current_password": "wrong",
                "new_password": "NewPassword123!",
            },
        ),
        None,
    )

    assert response["statusCode"] == STATUS_FORBIDDEN
    assert json.loads(response["body"]) == {"error": "Current password is incorrect"}
