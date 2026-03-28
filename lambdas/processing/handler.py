import json
from datetime import datetime, timezone
import boto3
import logging
import botocore
import requests
import os
import constants
from metrics import log_metric

PROCESSED_KEY = "processed/weather"
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def response(status, body):
    return {"statusCode": status, "body": json.dumps(body)}

def get_hub_info_from_pos(lat, lon):
    logger.info(f"Lookup hub by coordinates lat={lat}, lon={lon}")
    s3_client = boto3.client("s3")
    bucket_name = os.environ.get("DATA_BUCKET")
    try:
        response_obj = s3_client.get_object(Bucket=bucket_name, Key=constants.HUBS_FILE_KEY)
        content = json.loads(response_obj["Body"].read().decode("utf-8"))
        for hub, hub_info in content.items():
            if (str(hub_info["lon"])) == str(lon) and str((hub_info["lat"])) == str(lat):
                logger.info(f"Found hub_id={hub} for lat={lat}, lon={lon}")
                return {"hub_id": hub, "hub_name": hub_info["name"]}
        raise ValueError(f"No hub found for lat={lat}, lon={lon}")
    except ValueError:
        raise
    except Exception as e:
        logger.exception(f"Error reading {constants.HUBS_FILE_KEY} from {bucket_name}")
        raise RuntimeError(f"Error reading {constants.HUBS_FILE_KEY} from {bucket_name}")

def convert_unix_to_utc(timestamp):
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def unix_to_date(timestamp):
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m-%d")

def check_six_hour_point(timestamp):
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).hour % 6 == 0

def process_data(body):
    logger.info(f"Processing raw weather data for lat={body.get('latitude')}, lon={body.get('longitude')}")
    s3_client = boto3.client("s3")
    bucket_name = os.environ.get("DATA_BUCKET")
    curr_unix_time = body["currently"]["time"]
    lat, lon = body["latitude"], body["longitude"]
    hourly_data = body["hourly"]["data"]
    hub_info = get_hub_info_from_pos(lat, lon)
    hub_id, hub_name = hub_info["hub_id"], hub_info["hub_name"]
    schema_version = "1.0"
    forecast_origin = convert_unix_to_utc(curr_unix_time)

    days = []
    date = None
    day_counter = 0
    
    for obj in hourly_data:
        if not check_six_hour_point(obj["time"]):
            continue
        
        curr_date = unix_to_date(obj["time"])
        if date != curr_date:
            date = curr_date
            day_counter += 1
            days.append({"date": date, "day": day_counter, "snapshots": []})

        snapshot = {
            "forecast_timestamp": convert_unix_to_utc(obj["time"]),
            "forecast_lead_hours": int((obj["time"] - curr_unix_time) / 3600),
            "features": {
                "temperature": obj["temperature"],
                "wind_speed": obj["windSpeed"],
                "wind_gust": obj["windGust"],
                "precip_intensity": obj["precipIntensity"],
                "pressure": obj["pressure"],
                "humidity": obj["humidity"]
            }
        }
        days[-1]["snapshots"].append(snapshot)
    processed_data = {
        "schema_version": schema_version,
        "hub_id": hub_id,
        "hub_name": hub_name,
        "lat": lat,
        "lon": lon,
        "forecast_origin": forecast_origin,
        "days": days
    }
    date = datetime.fromtimestamp(curr_unix_time, tz=timezone.utc).strftime(constants.DATE_FORMAT)

    obj_key = f"{PROCESSED_KEY}/{hub_id}/{date}.json"
    s3_client.put_object(Bucket=bucket_name, Key=obj_key, Body=json.dumps(processed_data),
                         ContentType="application/json")
    logger.info(f"Raw weather data for hub_id={hub_id}, date={date} processed successfully and stored")
    log_metric(constants.WEATHER_RECORDS_PROCESSED, 1, constants.WEATHER_SERVICE)
    return processed_data

def handle_s3_event(event):
    base_url = os.environ["API_BASE_URL"]
    url = f"{base_url}/{constants.RETRIEVE_RAW_WEATHER_PATH}"
    res = []
    
    for record in event["Records"]:
        try:
            path = record["s3"]["object"]["key"]
            logger.info(f"Processing S3 record for key={path}")
            _, _, hub_id, date = path.split("/")
            date = date.removesuffix(".json")
            query_params_input = {"date": date}
            logger.info(f"Calling retrieval API for raw weather data for hub_id={hub_id}, date={date}")
            resp = requests.get(f"{url}/{hub_id}", params=query_params_input, timeout=10)

            if resp.status_code == constants.STATUS_NOT_FOUND:
                raise LookupError(f"Raw weather data not found for hub {hub_id} on {date}")
            if resp.status_code != constants.STATUS_OK:
                raise RuntimeError(f"Retrieval service returned {resp.status_code}: {resp.text}")
            
            data = resp.json()
            processed_data = process_data(data)
            res.append({"status": "processed", "processed_data": (processed_data)})
        except Exception as e:
            logger.exception(f"Error processing record for {record.get('s3', {}).get('object', {}).get('key', 'unknown')}: {e}")
            res.append({"status": "error", "error": str(e), "key": record.get("s3", {}).get("object", {}).get("key", "unknown")})
            
    logger.info(f"handle_s3_event completed with {len(res)} records processed")
    return res

def lambda_handler(event, context):
    try:
        # Check for bucket name existence
        bucket_name = os.environ.get("DATA_BUCKET")
        if not bucket_name:
            logger.error("Missing DATA_BUCKET configuration")
            return response(
                constants.STATUS_INTERNAL_SERVER_ERROR,
                {"error": "Missing DATA_BUCKET configuration"}
            )
        
        if "Records" in event and event["Records"][0].get("eventSource") == "aws:s3":
            logger.info(f"Processing triggered by S3 event: {event}")
            return handle_s3_event(event)
        elif "body" in event:
            logger.info(f"Processing triggered by API request: {event}")
            res = process_data(json.loads(event["body"]))
            return response(constants.STATUS_OK, {"message": f"Data processed successfully for {res['hub_id']}", "processed_data": json.dumps(res)})
        else:
            logger.error("Request missing raw data payload")
            return response(constants.STATUS_BAD_REQUEST, {"error": "Raw data not provided"}) 
    except ValueError as e:
        logger.exception(str(e))
        return response(constants.STATUS_BAD_REQUEST, {"error": str(e)})
    except RuntimeError as e:
        logger.exception(str(e))
        return response(constants.STATUS_INTERNAL_SERVER_ERROR, {"error": str(e)})
    except Exception as e:
        logger.exception(f"Unhandled error: {str(e)}")
        return response(constants.STATUS_INTERNAL_SERVER_ERROR, {"error": str(e)})
