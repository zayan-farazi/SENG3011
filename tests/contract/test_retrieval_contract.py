import requests
import os
from jsonschema import validate
from datetime import datetime, timezone
from test_constants import HUB_ID_1, DATE_3
from constants import STATUS_OK, RETRIEVE_RAW_WEATHER_PATH, RETRIEVE_PROCESSED_WEATHER_PATH
from .schemas.raw_weather_schema import RAW_WEATHER_SCHEMA
from .schemas.processed_data_schema import PROCESSED_DATA_SCHEMA

BASE_URL = os.environ["DEV_BASE_URL"]

def test_retrieve_raw_contract():
    """Test raw data retrieval endpoint contract"""
    date = datetime.now(timezone.utc).strftime("%d-%m-%Y") 
    url = f"{BASE_URL}/{RETRIEVE_RAW_WEATHER_PATH}/{HUB_ID_1}"
    
    response = requests.get(url, params={"date": date})
    assert response.status_code == STATUS_OK
    data = response.json()
    validate(instance=data, schema=RAW_WEATHER_SCHEMA)


def test_retrieve_processed_contract():
    """Test processed data retrieval endpoint contract"""
    url = f"{BASE_URL}/{RETRIEVE_PROCESSED_WEATHER_PATH}/{HUB_ID_1}"
    
    response = requests.get(url, params={"date": DATE_3})
    assert response.status_code == STATUS_OK
    data = response.json()
    validate(instance=data, schema=PROCESSED_DATA_SCHEMA)