import requests
import os
import json
from test_constants import HUB_ID_1, DATE_1, DATE_INVALID
from constants import STATUS_OK, STATUS_BAD_REQUEST, STATUS_NOT_FOUND, RETRIEVE_PROCESSED_WEATHER_PATH

BASE_URL = os.environ["BASE_URL"]

def test_processed_valid():
    url = f"{BASE_URL}/{RETRIEVE_PROCESSED_WEATHER_PATH}/{HUB_ID_1}"
    response = requests.get(
        url,
        params={"date": DATE_1} ## change
    )

    response_data = response.json()
    assert response_data["statusCode"] == STATUS_OK
    body = json.loads(response_data["body"])
    assert body["hub_id"] == HUB_ID_1
    assert body["hub_name"] == "Port of Singapore"
    assert "lat" in body
    assert "lon" in body
    assert "days" in body
    assert isinstance(body["days"], list)
    assert len(body["days"]) == 7
    for day in body["days"]:
        assert "date" in day
        assert "day" in day
        assert "snapshots" in day

def test_processed_invalid_date():
    url = f"{BASE_URL}/{RETRIEVE_PROCESSED_WEATHER_PATH}/{HUB_ID_1}"
    response = requests.get(
        url,
        params={"date": DATE_INVALID} ## change
    )

    response_data = response.json()
    assert response_data["statusCode"] == STATUS_BAD_REQUEST
    assert json.loads(response_data["body"]) == {"error": "Invalid date format. Use DD-MM-YYYY"}

def test_processed_missing_hub():
    url = f"{BASE_URL}/{RETRIEVE_PROCESSED_WEATHER_PATH}"
    response = requests.get(
        url,
        params={"date": DATE_1} ## change
    )

    response_data = response.json()
    assert response_data["statusCode"] == STATUS_BAD_REQUEST
    assert json.loads(response_data["body"]) == {"error": "Missing hub_id"}

def test_processed_object_not_found():
    url = f"{BASE_URL}/{RETRIEVE_PROCESSED_WEATHER_PATH}/{HUB_ID_1}"
    response = requests.get(
        url,
        params={"date": DATE_1} ## change
    )

    response_data = response.json()
    assert response_data["statusCode"] == STATUS_NOT_FOUND
    assert json.loads(response_data["body"]) == {"error": "Data for hub_id and date not found"}
