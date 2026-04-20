import json
import re
import os
import boto3
import constants
import logging
from urllib.parse import unquote
import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def add_email(hub_id, email, table):
    try:
        table.put_item(
            Item={
                "email": email,
                "hub_id": hub_id,
            }
        )
        logger.info(f"adding hub {hub_id} to {email} watchlist")
    except Exception as e:
        logger.error(f"DynamoDB error: {str(e)}")
        return response(constants.STATUS_INTERNAL_SERVER_ERROR, {"error": "Database error"})

    return response(constants.STATUS_OK, f"hub {hub_id} added to {email} watchlist")


def delete_email(hub_id, email, table):
    try:
        table.delete_item(
            Key={
                "email": email,
                "hub_id": hub_id,
            }
        )
        logger.info(f"deleting hub {hub_id} from {email} watchlist")
    except Exception as e:
        logger.error(f"DynamoDB error: {str(e)}")
        return response(constants.STATUS_INTERNAL_SERVER_ERROR, {"error": "Database error"})

    return response(constants.STATUS_OK, f"hub {hub_id} removed from {email} watchlist")


def valid_email(email):
    pattern = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
    return re.match(pattern, email) is not None


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

    http_method = get_http_method(event)
    path_params = event.get("pathParameters") or {}

    hub_id = path_params.get("hub_id")
    email = path_params.get("email")

    if not email or not hub_id:
        logger.error("missing hub_id or email")
        return response(constants.STATUS_BAD_REQUEST, {"error": "Missing hub_id or email"})

    email = unquote(email)

    if not valid_email(email):
        logger.error("Invalid email")
        return response(constants.STATUS_BAD_REQUEST, {"error": "Invalid email"})

    base_url = os.environ.get("API_BASE_URL")
    if not base_url:
        logger.error("Missing API_BASE_URL configuration")
        return response(constants.STATUS_INTERNAL_SERVER_ERROR, {"error": "Missing API_BASE_URL configuration"})

    if not valid_hub_id(base_url, hub_id):
        logger.error("Invalid hub_id")
        return response(constants.STATUS_BAD_REQUEST, {"error": "Invalid hub_id"})

    if http_method == "POST":
        logger.info("POST method requested")
        return add_email(hub_id, email, table)

    elif http_method == "DELETE":
        logger.info("DELETE method requested")
        return delete_email(hub_id, email, table)

    else:
        logger.info("Invalid request call")
        return response(constants.STATUS_BAD_REQUEST, "Method not allowed")


def response(status, message):
    return {
        "statusCode": status,
        "body": json.dumps({"message": message}),
    }