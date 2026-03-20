import json
from datetime import datetime, timezone
import boto3
import logging
import botocore
import requests
import os
import constants


hub_key = constants.HUBS_FILE_KEY
processed_key = "processed/weather"
log = logging.getLogger()
log.setLevel(logging.INFO)

def response(status, body):
    return {"statusCode": status, "body": json.dumps(body)}

def check_raw_format(body): 
    required_top_keys = ["currently", "latitude", "longitude", "hourly"]
    
    for key in required_top_keys:
        if key not in body:
            raise ValueError(f"Missing key: {key}")

    if "time" not in body["currently"]:
        raise ValueError("Missing key: currently.time")
    
    if not isinstance(body["currently"]["time"], (int, float)):
        raise TypeError("currently.time must be a number")

    if not isinstance(body["latitude"], (int, float)):
        raise TypeError("latitude must be a number")

    if not isinstance(body["longitude"], (int, float)):
        raise TypeError("longitude must be a number")

    if "data" not in body["hourly"]:
        raise ValueError("Missing key: hourly.data")

    if not isinstance(body["hourly"]["data"], list):
        raise TypeError("hourly.data must be a list")

    required_hourly_keys = [
        "time", "temperature", "windSpeed",
        "windGust", "precipIntensity",
        "pressure", "humidity"
    ]
    for i, entry in enumerate(body["hourly"]["data"]):
        if not isinstance(entry, dict):
            raise TypeError(f"hourly.data[{i}] must be an object")

        for key in required_hourly_keys:
            if key not in entry:
                raise ValueError(f"Missing key in hourly.data[{i}]: {key}")
        if not isinstance(entry["time"], (int, float)):
            raise TypeError(f"hourly.data[{i}].time must be a number")


def get_hub_info_from_pos(lat, lon):
    s3_client = boto3.client("s3")
    bucket_name = os.environ.get("DATA_BUCKET")
    try:
        response_obj = s3_client.get_object(Bucket=bucket_name, Key=hub_key)
        content = json.loads(response_obj["Body"].read().decode("utf-8"))
        for hub, hub_info in content.items():
            if (str(hub_info["lon"])) == str(lon) and str((hub_info["lat"])) == str(lat):
                return {"hub_id": hub, "hub_name": hub_info["name"]}
        raise ValueError(f"No hub found for lat={lat}, lon={lon}")
    except botocore.exceptions.ClientError:
        raise RuntimeError(f"Hubs file not found in bucket {bucket_name}")
    except Exception as e:
        raise RuntimeError(f"Error reading hubs file from {bucket_name}: {e}")


def convert_unix_to_utc(timestamp):
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def unix_to_date(timestamp):
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m-%d")


def check_six_hour_point(timestamp):
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).hour % 6 == 0


def handle_s3_event(event):
    s3_client = boto3.client("s3")
    bucket_name = os.environ.get("DATA_BUCKET")
    base_url = os.environ["API_BASE_URL"]
    url = f"{base_url}/{constants.RETRIEVE_RAW_WEATHER_PATH}"
    res = []
    
    for record in event["Records"]:
        try:
            path = record["s3"]["object"]["key"]
            _, _, hub_id, date = path.split("/")
            date = date.removesuffix(".json")
            query_params_input = {"date": date}
            resp = requests.get(f"{url}/{hub_id}", params=query_params_input)

            if resp.status_code == constants.STATUS_NOT_FOUND:
                raise LookupError(f"Processed data not found for hub {hub_id} on {date}")
            if resp.status_code != constants.STATUS_OK:
                raise RuntimeError(f"Retrieval service returned {resp.status_code}: {resp.text}")
            
            data = resp.json() 
            res_data = processing_data(data)
            obj_key = f"{processed_key}/{hub_id}/{date}.json"
            s3_client.put_object(Bucket=bucket_name, Key=obj_key, Body=json.dumps(res_data),
                                ContentType="application/json")
            res.append({"status": "processed", "processed_data": (res_data)})

        except Exception as e:
            log.exception(f"Error processing record for {record.get('s3', {}).get('object', {}).get('key', 'unknown')}: {e}")
            res.append({"status": "error", "error": str(e), "key": record.get("s3", {}).get("object", {}).get("key", "unknown")})
            
    return res


def processing_data(body):
    check_raw_format(body)
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
    res_data = {
        "schema_version": schema_version,
        "hub_id": hub_id,
        "hub_name": hub_name,
        "lat": lat,
        "lon": lon,
        "forecast_origin": forecast_origin,
        "days": days
    }
    date = datetime.fromtimestamp(curr_unix_time, tz=timezone.utc).strftime(constants.DATE_FORMAT)

    obj_key = f"{processed_key}/{hub_id}/{date}.json"
    s3_client.put_object(Bucket=bucket_name, Key=obj_key, Body=json.dumps(res_data),
                         ContentType="application/json")
    return res_data


def lambda_handler(event, context):
    try:
        # Check for bucket name existence
        bucket_name = os.environ.get("DATA_BUCKET")
        if not bucket_name:
            return response(
                constants.STATUS_INTERNAL_SERVER_ERROR,
                {"error": "Missing DATA_BUCKET configuration"}
            )
        
        if "Records" in event and event["Records"][0].get("eventSource") == "aws:s3":
            return handle_s3_event(event)
        elif "body" in event:
            res = processing_data(json.loads(event["body"]))
            return response(constants.STATUS_OK, {"message": f"Data processed successfully for {res['hub_id']}", "processed_data": json.dumps(res)})
        else:
            return response(constants.STATUS_BAD_REQUEST,
                            {"error": "Raw data not provided"}) 
    except Exception as e:
        msg = str(e)
        if "No hub found" in msg:
            status = constants.STATUS_BAD_REQUEST
        elif "Hubs file not found" in msg:
            status = constants.STATUS_NOT_FOUND
        elif isinstance(e, ValueError) or isinstance(e, TypeError):
            status = constants.STATUS_BAD_REQUEST
        else:
            status = constants.STATUS_INTERNAL_SERVER_ERROR

        return response(status, {"error": msg})
