import requests
import os
from tests.test_constants import HUB_ID_1, HUB_INVALID, DATE_2, FUTURE_DATE
from constants import STATUS_OK, STATUS_BAD_REQUEST, STATUS_NOT_FOUND, RETRIEVE_RAW_WEATHER_PATH

BASE_URL = os.environ["DEV_BASE_URL"]

def test_raw_valid():
    url = f"{BASE_URL}/{RETRIEVE_RAW_WEATHER_PATH}/{HUB_ID_1}"
    response = requests.get(
        url,
        params={"date": DATE_2}
    )

    assert response.status_code == STATUS_OK
    data = response.json()
    assert "latitude" in data
    assert "longitude" in data
    assert "currently" in data
    assert "hourly" in data
    hourly = data["hourly"]
    assert isinstance(hourly, dict)
    assert "data" in hourly
    assert isinstance(hourly["data"], list)
    assert "precipIntensity" in hourly["data"][0]
    assert "windSpeed" in hourly["data"][0]
    assert "daily" in data

def test_raw_invalid_hub():
    url = f"{BASE_URL}/{RETRIEVE_RAW_WEATHER_PATH}/{HUB_INVALID}"
    response = requests.get(
        url,
        params={"date": DATE_2}
    )

    assert response.status_code == STATUS_BAD_REQUEST
    assert response.json() == {"error": "Invalid hub_id"}

def test_raw_object_not_found():
    url = f"{BASE_URL}/{RETRIEVE_RAW_WEATHER_PATH}/{HUB_ID_1}"
    response = requests.get(
        url,
        params={"date": FUTURE_DATE}
    )

    assert response.status_code == STATUS_NOT_FOUND
    assert response.json() == {"error": "Data for hub_id and date not found"}
