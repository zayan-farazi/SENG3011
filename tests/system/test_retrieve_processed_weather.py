import requests
import os
from test_constants import HUB_ID_1, DATE_2, DATE_INVALID
from constants import STATUS_OK, STATUS_BAD_REQUEST, RETRIEVE_PROCESSED_WEATHER_PATH

BASE_URL = os.environ["DEV_BASE_URL"]

def test_processed_valid():
    url = f"{BASE_URL}/{RETRIEVE_PROCESSED_WEATHER_PATH}/{HUB_ID_1}"
    response = requests.get(
        url,
        params={"date": DATE_2}
    )

    assert response.status_code == STATUS_OK
    data = response.json()
    assert data["hub_id"] == HUB_ID_1
    assert data["hub_name"] == "Port of Singapore"
    assert "lat" in data
    assert "lon" in data
    assert "days" in data
    assert isinstance(data["days"], list)
    for day in data["days"]:
        assert "date" in day
        assert "day" in day
        assert "snapshots" in day
        assert isinstance(day["snapshots"], list)
        for snapshot in day["snapshots"]:
            assert "forecast_timestamp" in snapshot
            assert "forecast_lead_hours" in snapshot
            assert "features" in snapshot

def test_processed_invalid_date():
    url = f"{BASE_URL}/{RETRIEVE_PROCESSED_WEATHER_PATH}/{HUB_ID_1}"
    response = requests.get(
        url,
        params={"date": DATE_INVALID}
    )

    assert response.status_code == STATUS_BAD_REQUEST
    assert response.json() == {"error": "Invalid date format. Use DD-MM-YYYY"}

def test_processed_missing_date():
    url = f"{BASE_URL}/{RETRIEVE_PROCESSED_WEATHER_PATH}/{HUB_ID_1}"
    response = requests.get(url)

    assert response.status_code ==STATUS_BAD_REQUEST
    assert response.json() == {"error": "Missing date"}
