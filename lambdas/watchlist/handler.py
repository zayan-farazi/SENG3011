import json
import os
from datetime import datetime, timezone

import boto3
import constants
import logging
import requests
from auth_context import AuthError, auth_error_response, require_authenticated_user
from boto3.dynamodb.conditions import Key

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def add_watch(user_id, email, hub_id, table):
    try:
        table.put_item(
            Item={
                "user_id": user_id,
                "hub_id": hub_id,
                "notification_email": email,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        logger.info("added hub %s to user %s watchlist", hub_id, user_id)
    except Exception as exc:
        logger.error("DynamoDB error: %s", exc)
        return response(constants.STATUS_INTERNAL_SERVER_ERROR, {"error": "Database error"})

    return response(constants.STATUS_OK, {"message": f"hub {hub_id} added to watchlist"})


def delete_watch(user_id, hub_id, table):
    try:
        table.delete_item(
            Key={
                "user_id": user_id,
                "hub_id": hub_id,
            }
        )
        logger.info("removed hub %s from user %s watchlist", hub_id, user_id)
    except Exception as exc:
        logger.error("DynamoDB error: %s", exc)
        return response(constants.STATUS_INTERNAL_SERVER_ERROR, {"error": "Database error"})

    return response(constants.STATUS_OK, {"message": f"hub {hub_id} removed from watchlist"})


def list_watches(user_id, table):
    try:
        result = table.query(KeyConditionExpression=Key("user_id").eq(user_id))
    except Exception as exc:
        logger.error("DynamoDB error: %s", exc)
        return response(constants.STATUS_INTERNAL_SERVER_ERROR, {"error": "Database error"})

    hubs = [
        {
            "hub_id": item["hub_id"],
            "notification_email": item["notification_email"],
            "created_at": item["created_at"],
        }
        for item in result.get("Items", [])
    ]
    hubs.sort(key=lambda item: item["hub_id"])
    return response(constants.STATUS_OK, {"hubs": hubs})


def list_notifications(user_id, table):
    try:
        result = table.query(
            KeyConditionExpression=Key("user_id").eq(user_id),
            ScanIndexForward=False,
        )
    except Exception as exc:
        logger.error("DynamoDB error: %s", exc)
        return response(constants.STATUS_INTERNAL_SERVER_ERROR, {"error": "Database error"})

    notifications = [
        {
            "sent_at": item["sent_at"],
            "hub_id": item["hub_id"],
            "notification_email": item["notification_email"],
            "subject": item["subject"],
            "message": item["message"],
        }
        for item in result.get("Items", [])
    ]
    return response(constants.STATUS_OK, {"notifications": notifications})


def valid_hub_id(base_url, hub_id):
    url = f"{base_url}/{constants.LOCATION_PATH}/{hub_id}"
    res = requests.get(url, timeout=10)
    return res.status_code == constants.STATUS_OK


def get_http_method(event):
    request_context = event.get("requestContext") or {}
    http_context = request_context.get("http") or {}
    return http_context.get("method") or event.get("httpMethod")


def lambda_handler(event, context):
    region = os.environ.get("AWS_REGION", constants.DEFAULT_REGION)
    dynamodb = boto3.resource("dynamodb", region_name=region)
    table = dynamodb.Table(os.environ.get("WATCHLIST_TABLE_NAME", "watchlist"))
    messages_table = dynamodb.Table(os.environ.get("MESSAGES_TABLE_NAME", "messages"))

    http_method = get_http_method(event)
    path_params = event.get("pathParameters") or {}
    hub_id = path_params.get("hub_id")
    route_key = event.get("routeKey", "")

    try:
        user = require_authenticated_user(event, require_verified_email=http_method in {"POST", "DELETE"})
    except AuthError as error:
        logger.error("watchlist auth failure: %s", error.message)
        return auth_error_response(error)

    if http_method == "GET":
        if route_key == "GET /ese/v1/watchlist/notifications":
            logger.info("listing notifications for user %s", user["user_id"])
            return list_notifications(user["user_id"], messages_table)
        logger.info("listing watchlist for user %s", user["user_id"])
        return list_watches(user["user_id"], table)

    if not hub_id:
        logger.error("missing hub_id")
        return response(constants.STATUS_BAD_REQUEST, {"error": "Missing hub_id"})

    base_url = os.environ.get("API_BASE_URL")
    if not base_url:
        logger.error("Missing API_BASE_URL configuration")
        return response(constants.STATUS_INTERNAL_SERVER_ERROR, {"error": "Missing API_BASE_URL configuration"})

    if not valid_hub_id(base_url, hub_id):
        logger.error("Invalid hub_id")
        return response(constants.STATUS_BAD_REQUEST, {"error": "Invalid hub_id"})

    if http_method == "POST":
        logger.info("POST method requested")
        return add_watch(user["user_id"], user["notification_email"], hub_id, table)

    if http_method == "DELETE":
        logger.info("DELETE method requested")
        return delete_watch(user["user_id"], hub_id, table)

    logger.info("Invalid request call")
    return response(constants.STATUS_BAD_REQUEST, {"error": "Method not allowed"})


def response(status, body):
    return {
        "statusCode": status,
        "body": json.dumps(body),
    }
