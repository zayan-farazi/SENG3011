import json
import boto3
import requests
import os
from datetime import datetime, timezone
import constants
import logging
from hub_catalog import load_hubs, load_seed_hubs
from lambdas.metrics import log_metric

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def fetch_weather(lat, lon, api_key):
    url = f"https://api.pirateweather.net/forecast/{api_key}/{lat},{lon}"
    querystring = {"exclude":"","extend":"hourly","lang":"","units":"","version":"","tmextra":"","icon":""}

    logger.info(f"PirateWeather API call: fetching weather data for lat={lat}, lon={lon}")
    try:
        weather_data = requests.get(url, params=querystring, timeout=10)
        weather_data.raise_for_status()
        logger.info(f"PirateWeather API success: weather data fetched for lat={lat}, lon={lon}")
        return weather_data.text
    except Exception as e:
        logger.exception(f"PirateWeather API error fetching weather for lat={lat}, lon={lon}")
        log_metric(constants.WEATHER_API_ERRORS, 1, constants.WEATHER_SERVICE)
        raise RuntimeError(f"Failed to fetch weather data from PirateWeather for hub at lat={lat}, lon={lon}") from e


def store_weather(s3, bucket_name, hub_id, date, weather_data):
    s3.put_object(
        Bucket=bucket_name,
        Key=f"raw/weather/{hub_id}/{date}.json",
        Body=weather_data,
        ContentType="application/json"
    )
    logger.info(f"Stored weather data for hub={hub_id}, date={date}")
    log_metric(constants.WEATHER_RECORDS_INGESTED, 1, constants.WEATHER_SERVICE)

def lambda_handler(event, context):
    log_metric(constants.INGESTION_REQUESTS, 1, constants.WEATHER_SERVICE)
    s3 = boto3.client("s3")
    bucket_name = os.environ.get("DATA_BUCKET")
    api_key = os.environ.get("API_KEY")

    if not bucket_name:
        logger.error("Missing DATA_BUCKET configuration")
        return response(constants.STATUS_INTERNAL_SERVER_ERROR, {"error": "Missing DATA_BUCKET configuration"})

    if not api_key:
        logger.error("Missing PirateWeather API key")
        return response(constants.STATUS_INTERNAL_SERVER_ERROR, {"error": "Missing PirateWeather API key"})

    hub_id = event.get("pathParameters") or {}
    hub_id = hub_id.get("hub_id")

    hubs = load_hubs(s3, bucket_name)

    if hub_id:
        logger.info(f"Ingestion for hub {hub_id} requested")
        hub_info = hubs.get(hub_id)
        if not hub_info:
            logger.error(f"Invalid hub_id requested: {hub_id}")
            return response(constants.STATUS_BAD_REQUEST, {"error": "Invalid hub_id"})
        hubs = {hub_id: hub_info}
    else:
        hubs = load_seed_hubs(s3, bucket_name)
        logger.info(f"Automated ingestion triggered, loaded {len(hubs)} seed hubs")

    try:
        date = datetime.now(timezone.utc).strftime(constants.DATE_FORMAT)
        for hub_id, hub_info in hubs.items():
            weather_data = fetch_weather(hub_info["lat"], hub_info["lon"], api_key)
            store_weather(s3, bucket_name, hub_id, date, weather_data)

        return response(constants.STATUS_OK, {"message": "Success"})
    except RuntimeError as e:
        return response(constants.STATUS_BAD_GATEWAY, {"error": str(e)})
    except Exception as e:
        logger.exception(f"Unhandled error: {str(e)}")
        return response(constants.STATUS_INTERNAL_SERVER_ERROR, {"error": str(e)})

def response(status, body):
    return {
        "statusCode": status,
        "body": json.dumps(body)
    }
            
    
