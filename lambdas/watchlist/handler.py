import json
import os
import boto3
import constants
import logging
from urllib.parse import unquote

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def add_email(path_params, table):
    hub_id = path_params.get("hub_id")
    email = path_params.get("email")

    if not hub_id or not email:
        logger.error("Missing hub_id or email")
        return response(constants.STATUS_BAD_REQUEST, "missing hub_id or email")

    email = unquote(email)

    
    try: 
        table.put_item(
            Item={
                "hub_id": hub_id,
                "email": email
            }
        )
        logger.info(f"adding {email} to hub {hub_id} watchlist")
    except Exception as e: 
        logger.error(f"DynamoDB error: {str(e)}")
        return response(constants.STATUS_INTERNAL_SERVER_ERROR, "Database error")

    return response(constants.STATUS_OK, f"{email} added to hub {hub_id}")


def delete_email(path_params, table):
    hub_id = path_params.get("hub_id")
    email = path_params.get("email")

    if not hub_id or not email:
        logger.error("Missing hub_id or email")
        return response(constants.STATUS_BAD_REQUEST, "missing hub_id or email")

    email = unquote(email)

    
    try:
        table.delete_item(
            Key={
                "hub_id": hub_id,
                "email": email
            }   
        )
        logger.info(f"deleting {email} from hub {hub_id} watchlist")
    except Exception as e:
        logger.error(f"DynamoDB error: {str(e)}")
        return response(constants.STATUS_INTERNAL_SERVER_ERROR, "Database error")


    return response(constants.STATUS_OK, f"{email} removed from hub {hub_id}")


def lambda_handler(event, context):
    region = os.environ.get("AWS_REGION", "us-east-1")
    dynamodb = boto3.resource("dynamodb", region_name=region)
    table = dynamodb.Table("watchlist")


    http_method = event.get("httpMethod")
    path_params = event.get("pathParameters") or {}

    if http_method == "POST":
        logger.info("POST method requested")
        return add_email(path_params, table)

    elif http_method == "DELETE":
        logger.info("DELETE method requested")
        return delete_email(path_params, table)

    else:
        logger.info("Invalid request call")
        return response(constants.STATUS_BAD_REQUEST, "Method not allowed")



def response(status, message):
    return {
        "statusCode": status,
        "body": json.dumps({"message": message})
    }