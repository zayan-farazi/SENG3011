import os
import json
import boto3
import requests
from jsonschema import validate
from .schemas.raw_weather_schema import RAW_WEATHER_SCHEMA
import constants

API_KEY = os.environ.get("API_KEY")
BASE_URL = "https://api.pirateweather.net/forecast"
BUCKET_NAME = os.environ.get("DATA_BUCKET")

def load_hubs_from_s3():
    s3 = boto3.client("s3")
    s3_response = s3.get_object(Bucket=BUCKET_NAME, Key=constants.HUBS_FILE_KEY)
    return json.loads(s3_response["Body"].read().decode("utf-8"))

def fetch_weather(lat, lon):
    url = f"{BASE_URL}/{API_KEY}/{lat},{lon}"
    querystring = {"exclude":"","extend":"hourly","lang":"","units":"","version":"","tmextra":"","icon":""}
    response = requests.get(url, params=querystring)
    response.raise_for_status()
    return response.json()

def test_pirateweather_contract():
    hubs = load_hubs_from_s3()
    for hub_id, hub_data in hubs.items():
        data = fetch_weather(hub_data["lat"], hub_data["lon"])
        validate(instance=data, schema=RAW_WEATHER_SCHEMA)