import json
import boto3
from datetime import datetime
from constants import *

s3 = boto3.client("s3")

def lambda_handler(event, context):

    path = event.get("rawPath", "")
    hub_id = event["pathParameters"].get("hub_id")
    query = event.get("rawQueryString", {})

    if not hub_id:
        return response(STATUS_BAD_REQUEST, {"error": "Missing hub_id"})
    
    obj = s3.get_object(Bucket=BUCKET_NAME, Key="hubs.json")
    hubs = json.loads(obj["Body"].read())
    if hub_id not in hubs:
        return response(STATUS_BAD_REQUEST, {"error": "Invalid hub_id"})

    date = query.get("date")
    if not date:
        return response(STATUS_BAD_REQUEST, {"error": "Missing date"})

    try:
        # Validate date format
        datetime.strptime(date, DATE_FORMAT)
        if "raw" in path:
            key = f"raw/weather/{hub_id}/{date}.json"
        elif "processed" in path:
            key = f"processed/weather/{hub_id}/{date}.json"
        
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
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