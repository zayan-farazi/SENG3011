from jsonschema import validate
import json
import os
import requests
from tests.test_constants import HUB_ID_1, DATE_3, RAW_WEATHER_DATA_H1
from datetime import datetime, timezone
from constants import STATUS_OK, RETRIEVE_PROCESSED_WEATHER_PATH, PROCESS_WEATHER_PATH, INGEST_WEATHER_PATH, RETRIEVE_RAW_WEATHER_PATH
from .schemas.processed_data_schema import PROCESSED_DATA_SCHEMA, PROCESSING_API_SCHEMA
from .schemas.raw_weather_schema import RAW_WEATHER_SCHEMA

BASE_URL = os.environ["STAGING_BASE_URL"]

def test_valid_process_retrieval_contract():
    url_process = f"{BASE_URL}/{PROCESS_WEATHER_PATH}"

    with open(RAW_WEATHER_DATA_H1, "r") as f:
        data = json.load(f)
    response_process = requests.post(url_process, json=data)
    assert response_process.status_code == STATUS_OK

    body = response_process.json()
    processed_data = body["processed_data"]
    if isinstance(processed_data, str):
        processed_data = json.loads(processed_data)
    validate(instance={"message": body["message"], "processed_data": processed_data}, schema=PROCESSING_API_SCHEMA)
    validate(instance=processed_data, schema=PROCESSED_DATA_SCHEMA)

    url_retrieval = f"{BASE_URL}/{RETRIEVE_PROCESSED_WEATHER_PATH}/{HUB_ID_1}"
    response_retrieval = requests.get(
        url_retrieval,
        params={"date": DATE_3}
    )

    assert response_retrieval.status_code == STATUS_OK
    stored = response_retrieval.json()
    validate(instance=stored, schema=PROCESSED_DATA_SCHEMA)



def test_ingest_retrieve_process_contract():
    """Full flow: ingest -> retrieve raw -> process -> retrieve processed -> validate schemas"""
    
    # Ingest raw data
    ingest_url = f"{BASE_URL}/{INGEST_WEATHER_PATH}/{HUB_ID_1}"
    response_ingest = requests.post(ingest_url)
    assert response_ingest.status_code == STATUS_OK

    # Retrieve raw data and validate raw schema
    date_str = datetime.now(timezone.utc).strftime("%d-%m-%Y")
    retrieve_raw_url = f"{BASE_URL}/{RETRIEVE_RAW_WEATHER_PATH}/{HUB_ID_1}"
    response_raw = requests.get(retrieve_raw_url, params={"date": date_str})
    assert response_raw.status_code == STATUS_OK
    raw_data = response_raw.json()
    validate(instance=raw_data, schema=RAW_WEATHER_SCHEMA)

    # Process the raw data
    process_url = f"{BASE_URL}/{PROCESS_WEATHER_PATH}"
    response_process = requests.post(process_url, json=raw_data)
    assert response_process.status_code == STATUS_OK
    body = response_process.json()
    
    processed_data = body["processed_data"]
    if isinstance(processed_data, str):
        processed_data = json.loads(processed_data)

    # Validate processing API contract
    validate(instance={"message": body["message"], "processed_data": processed_data}, schema=PROCESSING_API_SCHEMA)
    validate(instance=processed_data, schema=PROCESSED_DATA_SCHEMA)

    # Retrieve processed data and validate schema
    retrieve_processed_url = f"{BASE_URL}/{RETRIEVE_PROCESSED_WEATHER_PATH}/{HUB_ID_1}"
    response_processed = requests.get(retrieve_processed_url, params={"date": date_str})
    assert response_processed.status_code == STATUS_OK
    stored_processed = response_processed.json()
    validate(instance=stored_processed, schema=PROCESSED_DATA_SCHEMA)
