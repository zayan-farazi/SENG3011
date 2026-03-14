import requests
import os
import json
from test_constants import HUB_ID_1, HUB_INVALID, DATE_1
from constants import STATUS_OK, STATUS_BAD_REQUEST, STATUS_NOT_FOUND, RETRIEVE_RAW_WEATHER_PATH

BASE_URL = os.environ["BASE_URL"]

def test_raw_valid():
    url = f"{BASE_URL}/{RETRIEVE_RAW_WEATHER_PATH}/{HUB_ID_1}"
    response = requests.get(
        url,
        params={"date": DATE_1} ## change
    )

    response_data = response.json()
    assert response_data["statusCode"] == STATUS_OK
    body = json.loads(response_data["body"])
    assert "latitude" in body
    assert "longitude" in body
    assert "currently" in body
    assert "minutely" in body
    assert "hourly" in body
    assert isinstance(body["currently"], dict)
    assert "data" in body["currently"]
    assert "daily" in body

def test_raw_invalid_hub():
    url = f"{BASE_URL}/{RETRIEVE_RAW_WEATHER_PATH}/{HUB_INVALID}"
    response = requests.get(
        url,
        params={"date": DATE_1} ## change
    )

    response_data = response.json()
    assert response_data["statusCode"] == STATUS_BAD_REQUEST
    assert json.loads(response_data["body"]) == {"error": "Invalid hub_id"}

def test_raw_missing_date():
    url = f"{BASE_URL}/{RETRIEVE_RAW_WEATHER_PATH}/{HUB_ID_1}"
    response = requests.get(url)

    response_data = response.json()
    assert response_data["statusCode"] == STATUS_BAD_REQUEST
    assert json.loads(response_data["body"]) == {"error": "Missing date"}

def test_raw_object_not_found():
    url = f"{BASE_URL}/{RETRIEVE_RAW_WEATHER_PATH}/{HUB_ID_1}"
    response = requests.get(
        url,
        params={"date": DATE_1} ## change
    )

    response_data = response.json()
    assert response_data["statusCode"] == STATUS_NOT_FOUND
    assert json.loads(response_data["body"]) == {"error": "Data for hub_id and date not found"}
