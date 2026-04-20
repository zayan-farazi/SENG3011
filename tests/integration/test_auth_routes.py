import json
from unittest.mock import Mock, patch

import constants
from lambdas.location.handler import lambda_handler as location_handler
from lambdas.watchlist.handler import lambda_handler as watchlist_handler


def auth_event(method, path_parameters=None, claims=None, body=None):
    event = {
        "httpMethod": method,
        "pathParameters": path_parameters or {},
        "requestContext": {
            "authorizer": {
                "jwt": {
                    "claims": claims
                    or {
                        "sub": "user-123",
                        "email": "user@example.com",
                        "email_verified": "true",
                    }
                }
            }
        },
    }
    if body is not None:
        event["body"] = json.dumps(body)
    return event


@patch("lambdas.watchlist.handler.requests.get")
def test_authenticated_user_can_add_list_and_remove_watch(mock_get, setup_s3):
    mock_get.return_value = Mock(status_code=constants.STATUS_OK)

    add_response = watchlist_handler(auth_event("POST", {"hub_id": "H001"}), None)
    assert add_response["statusCode"] == constants.STATUS_OK

    list_response = watchlist_handler(auth_event("GET"), None)
    assert list_response["statusCode"] == constants.STATUS_OK
    assert json.loads(list_response["body"])["hubs"][0]["hub_id"] == "H001"

    delete_response = watchlist_handler(auth_event("DELETE", {"hub_id": "H001"}), None)
    assert delete_response["statusCode"] == constants.STATUS_OK


@patch("lambdas.watchlist.handler.requests.get")
def test_one_user_cannot_read_another_users_watchlist(mock_get, setup_s3):
    mock_get.return_value = Mock(status_code=constants.STATUS_OK)
    watchlist_handler(auth_event("POST", {"hub_id": "H001"}), None)

    other_user_response = watchlist_handler(
        auth_event(
            "GET",
            claims={
                "sub": "user-999",
                "email": "other@example.com",
                "email_verified": "true",
            },
        ),
        None,
    )

    assert other_user_response["statusCode"] == constants.STATUS_OK
    assert json.loads(other_user_response["body"]) == {"hubs": []}


def test_unauthenticated_calls_receive_401_on_protected_routes(setup_s3):
    create_response = location_handler(
        {
            "httpMethod": "POST",
            "body": json.dumps({"lat": 1.234, "lon": 5.678, "name": "Secure Port"}),
        },
        None,
    )
    watchlist_response = watchlist_handler({"httpMethod": "GET", "pathParameters": {}}, None)

    assert create_response["statusCode"] == constants.STATUS_UNAUTHORIZED
    assert watchlist_response["statusCode"] == constants.STATUS_UNAUTHORIZED


def test_public_location_reads_still_work_without_auth(setup_s3):
    response = location_handler(
        {
            "httpMethod": "GET",
            "pathParameters": {"hub_id": "H001"},
        },
        None,
    )

    assert response["statusCode"] == constants.STATUS_OK
    assert json.loads(response["body"])["hub_id"] == "H001"
