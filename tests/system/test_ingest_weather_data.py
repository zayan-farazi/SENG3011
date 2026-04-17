import requests
import os
from datetime import datetime, timezone
from tests.test_constants import HUB_ID_1, HUB_INVALID
from constants import STATUS_OK, STATUS_BAD_REQUEST, INGEST_WEATHER_PATH, DATE_FORMAT, RETRIEVE_RAW_WEATHER_PATH

BASE_URL = os.environ["STAGING_BASE_URL"]


def test_valid_ingestion():
    url = f"{BASE_URL}/{INGEST_WEATHER_PATH}/{HUB_ID_1}"
    response = requests.post(url)
    print(response.json())
    assert response.status_code == STATUS_OK

    date = datetime.now(timezone.utc).strftime(DATE_FORMAT)
    req_url = f"{BASE_URL}/{RETRIEVE_RAW_WEATHER_PATH}/{HUB_ID_1}"
    result = requests.get(url=req_url, params={"date": date})
    assert result.status_code == STATUS_OK

def test_invalid_hub():
    url = f"{BASE_URL}/{INGEST_WEATHER_PATH}/{HUB_INVALID}"
    response = requests.post(url)

    assert response.status_code == STATUS_BAD_REQUEST
    assert response.json() == {"error": "Invalid hub_id"}






