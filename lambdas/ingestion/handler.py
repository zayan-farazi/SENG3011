import json
import boto3
import requests
import os
from datetime import datetime
import constants
from hub_catalog import load_hubs


def fetch_weather(lat, lon, api_key):
    url = f"https://api.pirateweather.net/forecast/{api_key}/{lat},{lon}"
    querystring = {"exclude":"","extend":"hourly","lang":"","units":"","version":"","tmextra":"","icon":""}

    weather_data = requests.get(url, params=querystring)
    weather_data.raise_for_status()
    return weather_data.text

def store_weather(s3, bucket_name, hub_id, date, weather_data):
    s3.put_object(
        Bucket=bucket_name,
        Key=f"raw/weather/{hub_id}/{date}.json",
        Body=weather_data,
        ContentType="application/json"
    )

def lambda_handler(event, context):
    s3 = boto3.client("s3")
    bucket_name = os.environ.get("DATA_BUCKET")
    api_key = os.environ.get("API_KEY")

    if not bucket_name:
        return response(constants.STATUS_INTERNAL_SERVER_ERROR, {"error": "Missing DATA_BUCKET configuration"})
    
    if not api_key:
        return response(constants.STATUS_INTERNAL_SERVER_ERROR, {"error": "Missing API key"})

    hub_id = event.get("pathParameters") or {}
    hub_id = hub_id.get("hub_id")

    hubs = load_hubs(s3, bucket_name)
 
    
    if hub_id:
        if hub_id not in hubs:
            return response(constants.STATUS_BAD_REQUEST, {"error": "Invalid hub_id"})
        hubs = {hub_id: hubs[hub_id]}

    date = datetime.now().strftime(constants.DATE_FORMAT)

    for hub_id, hub_data in hubs.items():
        
        weather_data = fetch_weather(hub_data["lat"], hub_data["lon"], api_key)
        store_weather(s3, bucket_name, hub_id, date, weather_data)

    return response(constants.STATUS_OK, {"message": "Success"})

def response(status, body):
    return {
        "statusCode": status,
        "body": json.dumps(body)
    }
            
    
