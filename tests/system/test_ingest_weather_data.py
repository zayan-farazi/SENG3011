import requests
import os
import boto3
import json
from jsonschema import validate, ValidationError             
from datetime import datetime
from test_constants import HUB_ID_1, HUB_INVALID
from constants import STATUS_OK, STATUS_BAD_REQUEST, INGEST_WEATHER_PATH, DATE_FORMAT

BASE_URL = os.environ["DEV_BASE_URL"]


def valid_ingestion():
    s3 = boto3.client("s3")
    url = f"{BASE_URL}/{INGEST_WEATHER_PATH}/{HUB_ID_1}"
    response = requests.get(url)
    assert response.status_code == STATUS_OK
    
    date = datetime.now().strftime(DATE_FORMAT)
    expected_key = f"raw/weather/{HUB_ID_1}/{date}.json"

    result = s3.get_object(
        Bucket=os.environ["DATA_BUCKET"],
        Key=expected_key
    )

    body = result["Body"].read().decode()
    data = json.loads(body)

    assert result is not None

    with open("schema.json") as file:
        schema = json.load(file)  
    
    try:
        validate(instance=data, schema=schema)
    except ValidationError as e:
        print(e.message)
        assert False


def invalid_hub():
    url = f"{BASE_URL}/{INGEST_WEATHER_PATH}/{HUB_ID_1}"
    response = requests.get(url)

    assert response.status_code == STATUS_BAD_REQUEST
    assert response.json == {"error": "Invalid hub_id"}






