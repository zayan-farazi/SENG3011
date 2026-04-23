import json
import os

import boto3
import constants
import logging
from auth_context import AuthError, auth_error_response, require_authenticated_user

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def response(status, body):
    return {
        "statusCode": status,
        "body": json.dumps(body),
    }


def get_http_method(event):
    request_context = event.get("requestContext") or {}
    http_context = request_context.get("http") or {}
    return http_context.get("method") or event.get("httpMethod")


def _cognito_client():
    region = os.environ.get("AWS_REGION", constants.DEFAULT_REGION)
    return boto3.client("cognito-idp", region_name=region)


def _get_user_profile(client, user_pool_id, username):
    result = client.admin_get_user(UserPoolId=user_pool_id, Username=username)
    attrs = {item["Name"]: item["Value"] for item in result.get("UserAttributes", [])}
    return {
        "username": attrs.get("preferred_username") or username,
        "email": attrs.get("email"),
        "email_verified": attrs.get("email_verified") == "true",
        "company_name": attrs.get("custom:company_name", ""),
    }


def _update_profile(client, user_pool_id, username, body):
    attrs = []
    if "username" in body:
        attrs.append({"Name": "preferred_username", "Value": body["username"]})
    if "email" in body:
        attrs.append({"Name": "email", "Value": body["email"]})
        attrs.append({"Name": "email_verified", "Value": "false"})
    if "company_name" in body:
        attrs.append({"Name": "custom:company_name", "Value": body["company_name"]})

    if not attrs:
        return response(constants.STATUS_BAD_REQUEST, {"error": "No supported profile fields provided"})

    client.admin_update_user_attributes(
        UserPoolId=user_pool_id,
        Username=username,
        UserAttributes=attrs,
    )
    return response(constants.STATUS_OK, {"message": "Profile updated"})


def _change_password(client, user_pool_id, client_id, username, body):
    current_password = body.get("current_password")
    new_password = body.get("new_password")
    if not current_password or not new_password:
        return response(
            constants.STATUS_BAD_REQUEST,
            {"error": "current_password and new_password are required"},
        )

    client.admin_initiate_auth(
        UserPoolId=user_pool_id,
        ClientId=client_id,
        AuthFlow="ADMIN_USER_PASSWORD_AUTH",
        AuthParameters={
            "USERNAME": username,
            "PASSWORD": current_password,
        },
    )
    client.admin_set_user_password(
        UserPoolId=user_pool_id,
        Username=username,
        Password=new_password,
        Permanent=True,
    )
    return response(constants.STATUS_OK, {"message": "Password updated"})


def lambda_handler(event, context):
    try:
        user = require_authenticated_user(event)
    except AuthError as error:
        logger.error("auth route auth failure: %s", error.message)
        return auth_error_response(error)

    method = get_http_method(event)
    body = json.loads(event.get("body") or "{}")
    client = _cognito_client()
    user_pool_id = os.environ["COGNITO_USER_POOL_ID"]
    client_id = os.environ["COGNITO_USER_POOL_CLIENT_ID"]
    username = user["username"]
    if not username:
        return response(constants.STATUS_FORBIDDEN, {"error": "Missing Cognito username claim"})

    try:
        if method == "GET":
            return response(constants.STATUS_OK, _get_user_profile(client, user_pool_id, username))
        if method == "PUT":
            route_key = event.get("routeKey", "")
            raw_path = event.get("rawPath", "")
            if route_key == "PUT /ese/v1/auth/password" or raw_path.endswith("/ese/v1/auth/password"):
                return _change_password(client, user_pool_id, client_id, username, body)
            return _update_profile(client, user_pool_id, username, body)
    except client.exceptions.NotAuthorizedException:
        return response(constants.STATUS_FORBIDDEN, {"error": "Current password is incorrect"})
    except client.exceptions.InvalidPasswordException as exc:
        return response(constants.STATUS_BAD_REQUEST, {"error": str(exc)})
    except Exception as exc:
        logger.exception("auth handler failure")
        return response(constants.STATUS_INTERNAL_SERVER_ERROR, {"error": str(exc)})

    return response(constants.STATUS_BAD_REQUEST, {"error": "Method not allowed"})
