import json
import re
import os
import boto3
import constants
import logging
from urllib.parse import unquote

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def add_email(hub_id, email, table):
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


def delete_email(hub_id, email, table):
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

def valid_email(email):
    pattern = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
    return re.match(pattern, email) is not None

def valid_hub(hub_id):
    s3 = boto3.client("s3")
    bucket_name = os.environ.get("DATA_BUCKET")
    s3_response = s3.get_object(Bucket=bucket_name, Key=constants.HUBS_FILE_KEY)
    hubs = json.loads(s3_response["Body"].read().decode("utf-8"))
    return hub_id in hubs

def lambda_handler(event, context):
    region = os.environ.get("AWS_REGION", "us-east-1")
    dynamodb = boto3.resource("dynamodb", region_name=region)
    table = dynamodb.Table("watchlist")

    http_method = event.get("httpMethod")
    path_params = event.get("pathParameters") or {}

    hub_id = path_params.get("hub_id")
    email = path_params.get("email")
    

    if not email or not hub_id:
        logger.error("missing hub_id or email")
        return response(constants.STATUS_BAD_REQUEST, "Missing hub_id or email")
    
    email = unquote(email)

    if not valid_email(email):
        logger.error("Invalid email")
        return response(constants.STATUS_BAD_REQUEST, "Invalid email")
    
    if not valid_hub(hub_id):
        logger.error("Invalid hub_id")
        return response(constants.STATUS_BAD_REQUEST, "Invalid hub")


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
        "body": json.dumps({"message": message})
    }