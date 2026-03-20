import requests
import os
from test_constants import HUB_ID_1, HUB_INVALID, DATE_2, DATE_INVALID
from constants import STATUS_OK, STATUS_BAD_REQUEST, RISK_LOCATION_PATH

BASE_URL = os.environ["DEV_BASE_URL"]

def test_risk_valid():
    url = f"{BASE_URL}/{RISK_LOCATION_PATH}/{HUB_ID_1}"
    response = requests.get(
        url,
        params={"date": DATE_2}
    )

    assert response.status_code == STATUS_OK
    data = response.json()
    assert data["dataset_type"] == "Supply Chain Disruption Risk Assessment"
    assert "events" in data
    assert "data_source" in data
    assert "time_object" in data
    assert isinstance(data["events"], list)

    daily_events = [e for e in data["events"] if e["event_type"] == "daily_risk_assessment"]
    outlook_events = [e for e in data["events"] if e["event_type"] == "seven_day_outlook"]
    assert len(daily_events) >= 1
    assert len(outlook_events) == 1

    day = daily_events[0]["attribute"]
    assert "peak_risk_score" in day
    assert "risk_level" in day
    assert "snapshots" in day
    assert day["risk_level"] in ("Low", "Elevated", "High", "Critical")

    for s in day["snapshots"]:
        assert "risk_score" in s
        assert "risk_level" in s
        assert "primary_driver" in s
        assert 0.0 <= s["risk_score"] <= 1.0

    outlook = outlook_events[0]["attribute"]
    assert "outlook_risk_score" in outlook
    assert "outlook_risk_level" in outlook
    assert "peak_day" in outlook
    assert "days_assessed" in outlook

def test_risk_invalid_hub():
    url = f"{BASE_URL}/{RISK_LOCATION_PATH}/{HUB_INVALID}"
    response = requests.get(
        url,
        params={"date": DATE_2}
    )

    assert response.status_code == STATUS_BAD_REQUEST
    assert response.json() == {"error": "Invalid hub_id"}

def test_risk_invalid_date():
    url = f"{BASE_URL}/{RISK_LOCATION_PATH}/{HUB_ID_1}"
    response = requests.get(
        url,
        params={"date": DATE_INVALID}
    )

    assert response.status_code == STATUS_BAD_REQUEST
    assert "date format" in response.json()["error"].lower()

def test_risk_missing_date():
    url = f"{BASE_URL}/{RISK_LOCATION_PATH}/{HUB_ID_1}"
    response = requests.get(url)

    assert response.status_code == STATUS_OK
    data = response.json()
    assert "events" in data
    assert data["dataset_type"] == "Supply Chain Disruption Risk Assessment"
