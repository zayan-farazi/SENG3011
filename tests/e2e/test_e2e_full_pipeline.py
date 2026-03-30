import requests
import os
import json
from tests.test_constants import HUB_ID_1
from datetime import datetime, timezone, timedelta
from constants import STATUS_OK, DATE_FORMAT, RISK_LOCATION_PATH, INGEST_WEATHER_PATH, RETRIEVE_PROCESSED_WEATHER_PATH, RETRIEVE_RAW_WEATHER_PATH, PROCESS_WEATHER_PATH
BASE_URL = os.environ["DEV_BASE_URL"]

def test_e2e_full_pipeline():
    #  Ingest data (calls PirateWeather + stores raw in S3)
    url_ingest = f"{BASE_URL}/{INGEST_WEATHER_PATH}/{HUB_ID_1}"
    resp_ingest = requests.post(url_ingest)
    assert resp_ingest.status_code == STATUS_OK

    # Retrieve raw data
    date = datetime.now(timezone.utc).strftime(DATE_FORMAT)
    url_raw = f"{BASE_URL}/{RETRIEVE_RAW_WEATHER_PATH}/{HUB_ID_1}"
    resp_raw = requests.get(url_raw, params={"date": date})
    assert resp_raw.status_code == STATUS_OK
    raw_data = resp_raw.json()
    assert "hourly" in raw_data

    # Process raw data
    url_process = f"{BASE_URL}/{PROCESS_WEATHER_PATH}"
    resp_process = requests.post(url_process, json=raw_data)
    assert resp_process.status_code == STATUS_OK

    processed = resp_process.json()["processed_data"]
    if isinstance(processed, str):
        processed = json.loads(processed)
    assert "schema_version" in processed

    # Retrieve processed data (persisted)
    url_processed = f"{BASE_URL}/{RETRIEVE_PROCESSED_WEATHER_PATH}/{HUB_ID_1}"
    resp_processed = requests.get(url_processed, params={"date": date})
    assert resp_processed.status_code == STATUS_OK

    stored_processed = resp_processed.json()
    assert stored_processed == processed

    # Run analytics
    url_analytics = f"{BASE_URL}/{RISK_LOCATION_PATH}/{HUB_ID_1}"
    resp_analytics = requests.get(url_analytics, params={"date": date})
    assert resp_analytics.status_code == STATUS_OK

    analytics = resp_analytics.json()
    assert "events" in analytics
    assert len(analytics["events"]) > 0
    assert analytics["events"][0]["attribute"]["hub_id"] == HUB_ID_1


def test_e2e_wrong_date():
    '''' this test ensures ingestion does not store unexpected data that affect all other services'''
    tomorrow = (datetime.now(timezone.utc) + timedelta(days=1)).strftime(DATE_FORMAT)

    # Ingest data
    resp_ingest = requests.post(f"{BASE_URL}/{INGEST_WEATHER_PATH}/{HUB_ID_1}")
    assert resp_ingest.status_code == STATUS_OK

    # Retrieve raw data
    resp_raw = requests.get(
        f"{BASE_URL}/{RETRIEVE_RAW_WEATHER_PATH}/{HUB_ID_1}",
        params={"date": tomorrow}
    )
    assert resp_raw.status_code != STATUS_OK

    # Retrieve processed → should FAIL (not processed yet)
    resp_processed = requests.get(
        f"{BASE_URL}/{RETRIEVE_PROCESSED_WEATHER_PATH}/{HUB_ID_1}",
        params={"date": tomorrow}
    )
    assert resp_processed.status_code != STATUS_OK

    # Call analytics → should use cached risk score
    resp_analytics_fail = requests.get(
        f"{BASE_URL}/{RISK_LOCATION_PATH}/{HUB_ID_1}",
        params={"date": tomorrow}
    )
    
    assert resp_analytics_fail.status_code == STATUS_OK 
    analytics = resp_analytics_fail.json()
    for event in analytics.get("events", []):
        if "date" in event["attribute"]:
            # No tommorrow date is recorded
            assert event["attribute"]["date"] != tomorrow