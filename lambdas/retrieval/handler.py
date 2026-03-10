import json
import boto3
import os
from datetime import datetime
import constants

def lambda_handler(event, context):
    s3 = boto3.client("s3")
    bucket_name = os.environ.get("DATA_BUCKET", "seng-3011-bkt-zayan-dev")

    path = event.get("rawPath", "")
    path_params = event.get("pathParameters") or {}
    query_params = event.get("queryStringParameters") or {}
    hub_id = path_params.get("hub_id")
    date = query_params.get("date")

    if not hub_id:
        return response(constants.STATUS_BAD_REQUEST, {"error": "Missing hub_id"})

    if not date:
        return response(constants.STATUS_BAD_REQUEST, {"error": "Missing date"})

    try:
        datetime.strptime(date, constants.DATE_FORMAT)

        obj = s3.get_object(Bucket=bucket_name, Key=constants.HUBS_FILE_KEY)
        hubs = json.loads(obj["Body"].read())
        if hub_id not in hubs:
            return response(constants.STATUS_BAD_REQUEST, {"error": "Invalid hub_id"})
    
        if "raw" in path:
            key = f"raw/weather/{hub_id}/{date}.json"
        elif "processed" in path:
            key = f"processed/weather/{hub_id}/{date}.json"
        
        obj = s3.get_object(Bucket=bucket_name, Key=key)
        data = json.loads(obj["Body"].read())

        return response(constants.STATUS_OK, data)

    except ValueError:
        return response(constants.STATUS_BAD_REQUEST, {"error": "Invalid date format. Use DD-MM-YYYY"})
    
    except s3.exceptions.NoSuchKey:
        return response(constants.STATUS_NOT_FOUND, {"error": "Data for hub_id and date not found"})

    except Exception as e:
        return response(constants.STATUS_INTERNAL_SERVER_ERROR, {"error": str(e)})

def response(status, body):
    return {
        "statusCode": status,
        "body": json.dumps(body)
    }