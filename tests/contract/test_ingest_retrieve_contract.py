import requests
import os
from jsonschema import validate
from datetime import datetime, timezone
from test_constants import HUB_ID_1
from constants import STATUS_OK, INGEST_WEATHER_PATH, DATE_FORMAT, RETRIEVE_RAW_WEATHER_PATH
from .schemas.raw_weather_schema import RAW_WEATHER_SCHEMA, INGESTION_API_SCHEMA

BASE_URL = os.environ["DEV_BASE_URL"]


def test_valid_ingestion_retrieval_contract():
    url = f"{BASE_URL}/{INGEST_WEATHER_PATH}/{HUB_ID_1}"
    response = requests.post(url)
    assert response.status_code == STATUS_OK
    validate(instance=response.json(), schema=INGESTION_API_SCHEMA)
    
    date = datetime.now(timezone.utc).strftime(DATE_FORMAT)
    req_url = f"{BASE_URL}/{RETRIEVE_RAW_WEATHER_PATH}/{HUB_ID_1}"
    result = requests.get(url=req_url, params={"date": date})    
    assert result.status_code == STATUS_OK
    data = result.json() 
    validate(instance=data, schema=RAW_WEATHER_SCHEMA)



