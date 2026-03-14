import json
from datetime import datetime, timezone
import urllib.request
import urllib.parse
import boto3
import botocore
import os
import constants


hub_key = constants.HUBS_FILE_KEY
processed_key = "processed/weather"
get_raw_url = "https://ese/v1/retrieve/raw/weather"


def response(status, body):
    return {"statusCode": status, "body": json.dumps(body)}


def get_hub_info_from_pos(lat, lon):
    bucket_name = os.environ.get("DATA_BUCKET", "seng-3011-bkt-zayan-dev")
    s3_client = boto3.client("s3")
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
    for record in event["Records"]:
        path = record["s3"]["object"]["key"]
        _, _, _, hub_id, date = path.split("/")
        query_params = urllib.parse.urlencode({"date": date})
        try:
            with urllib.request.urlopen(f"{get_raw_url}/{hub_id}?{query_params}") as resp:
                data = json.loads(resp.read().decode("utf-8"))
                processing_data(data)
        except Exception as e:
            return response(constants.STATUS_INTERNAL_SERVER_ERROR,
                            {"error": f"Failed processing S3 object {path}: {e}"})
    return response(constants.STATUS_OK, {"message": "S3 event processed successfully"})


def processing_data(body):
    bucket_name = os.environ.get("DATA_BUCKET", "seng-3011-bkt-zayan-dev")
    s3_client = boto3.client("s3")
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
        if date is not curr_date:
            date = curr_date
            day_counter += 1
            days.append({"date": date, "day": day_counter, "snapshots": []})

        snapshot = {
            "forecast_timestamp": convert_unix_to_utc(obj["time"]),
            "forecast_lead_hours": (obj["time"] - curr_unix_time) / 3600,
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


def lambda_handler(event, context):
    try:
        if "Records" in event and event["Records"][0].get("eventSource") == "aws:s3":
            return handle_s3_event(event)
        elif "body" in event:
            processing_data(json.loads(event["body"]))
            return response(constants.STATUS_OK, {"message": "Data processed successfully"})
        else:
            return response(constants.STATUS_BAD_REQUEST,
                            {"error": "Missing 'body' or 'Records' in event"})
    except Exception as e:
        msg = str(e)
        if "No hub found" in msg:
            status = constants.STATUS_BAD_REQUEST
        elif "Hubs file not found" in msg:
            status = constants.STATUS_NOT_FOUND
        else:
            status = constants.STATUS_INTERNAL_SERVER_ERROR

        return response(status, {"error": msg})