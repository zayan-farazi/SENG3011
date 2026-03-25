import json
import boto3
import os
import logging
from datetime import datetime
import constants

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    logger.info(f"Incoming retrieval request: event={event}")
    s3 = boto3.client("s3")
    bucket_name = os.environ.get("DATA_BUCKET")

    path = event.get("rawPath", "")
    path_params = event.get("pathParameters") or {}
    query_params = event.get("queryStringParameters") or {}
    hub_id = path_params.get("hub_id")
    date = query_params.get("date")

    if not bucket_name:
        logger.error("Missing DATA_BUCKET configuration")
        return response(constants.STATUS_INTERNAL_SERVER_ERROR, {"error": "Missing DATA_BUCKET configuration"})

    if not hub_id:
        logger.error("Missing hub_id in request")
        return response(constants.STATUS_BAD_REQUEST, {"error": "Missing hub_id"})

    if not date:
        logger.error("Missing date in request")
        return response(constants.STATUS_BAD_REQUEST, {"error": "Missing date"})

    try:
        datetime.strptime(date, constants.DATE_FORMAT)

        logger.info(f"Looking up {constants.HUBS_FILE_KEY} in bucket={bucket_name}")
        obj = s3.get_object(Bucket=bucket_name, Key=constants.HUBS_FILE_KEY)
        hubs = json.loads(obj["Body"].read())
        if hub_id not in hubs:
            logger.error(f"Invalid hub_id requested: {hub_id}")
            return response(constants.STATUS_BAD_REQUEST, {"error": "Invalid hub_id"})
    
        if "raw" in path:
            key = f"raw/weather/{hub_id}/{date}.json"
        elif "processed" in path:
            key = f"processed/weather/{hub_id}/{date}.json"
        
        obj = s3.get_object(Bucket=bucket_name, Key=key)
        data = json.loads(obj["Body"].read())
        logger.info(f"Successfully retrieved object s3://{bucket_name}/{key}")
        return response(constants.STATUS_OK, data)

    except ValueError:
        logger.exception(f"Invalid date format: {date}")
        return response(constants.STATUS_BAD_REQUEST, {"error": "Invalid date format. Use DD-MM-YYYY"})
    
    except s3.exceptions.NoSuchKey:
        logger.exception(f"No data found for key s3://{bucket_name}/{key}")
        return response(constants.STATUS_NOT_FOUND, {"error": "Data for hub_id and date not found"})

    except Exception as e:
        logger.exception(f"Unhandled error: {str(e)}")
        return response(constants.STATUS_INTERNAL_SERVER_ERROR, {"error": str(e)})

def response(status, body):
    return {
        "statusCode": status,
        "body": json.dumps(body)
    }
