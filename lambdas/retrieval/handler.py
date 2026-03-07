import json
import boto3
import os
from datetime import datetime
from constants import *

def lambda_handler(event, context):
    s3 = boto3.client("s3")
    bucket_name = os.environ.get("DATA_BUCKET", "seng-3011-bkt-zayan-dev")

    path = event.get("rawPath", "")
    hub_id = event["pathParameters"].get("hub_id")
    date = event.get("queryStringParameters", {}).get("date")

    if not hub_id:
        return response(STATUS_BAD_REQUEST, {"error": "Missing hub_id"})

    if not date:
        return response(STATUS_BAD_REQUEST, {"error": "Missing date"})

    try:
        datetime.strptime(date, DATE_FORMAT)

        obj = s3.get_object(Bucket=bucket_name, Key=HUBS_FILE_KEY)
        hubs = json.loads(obj["Body"].read())
        if hub_id not in hubs:
            return response(STATUS_BAD_REQUEST, {"error": "Invalid hub_id"})
    
        if "raw" in path:
            key = f"raw/weather/{hub_id}/{date}.json"
        elif "processed" in path:
            key = f"processed/weather/{hub_id}/{date}.json"
        
        obj = s3.get_object(Bucket=bucket_name, Key=key)
        data = json.loads(obj["Body"].read())

        return response(STATUS_OK, data)

    except ValueError:
        return response(STATUS_BAD_REQUEST, {"error": "Invalid date format. Use DD-MM-YYYY"})
    
    except s3.exceptions.NoSuchKey:
        return response(STATUS_NOT_FOUND, {"error": "Data for hub_id and date not found"})

    except Exception as e:
        return response(STATUS_INTERNAL_SERVER_ERROR, {"error": str(e)})

def response(status, body):
    return {
        "statusCode": status,
        "body": json.dumps(body)
    }