import requests
import json
import os
from tests.test_constants import HUB_ID_1, DATE_3, RAW_WEATHER_DATA_H1
from datetime import datetime
from constants import STATUS_OK, STATUS_BAD_REQUEST, RETRIEVE_PROCESSED_WEATHER_PATH, PROCESS_WEATHER_PATH

BASE_URL = os.environ["DEV_BASE_URL"]


def is_iso_datetime(value):
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        return True
    except Exception:
        return False

def is_iso_date(value):
    try:
        datetime.strptime(value, "%Y-%m-%d")
        return True
    except Exception:
        return False

def validate_processed_format(data):
    try:
        required_top_keys = [
            "schema_version", "hub_id", "hub_name",
            "lat", "lon", "forecast_origin", "days"
        ]
        if not all(k in data for k in required_top_keys):
            return False

        if not isinstance(data["schema_version"], str):
            return False
        if not isinstance(data["hub_id"], str):
            return False
        if not isinstance(data["hub_name"], str):
            return False
        if not isinstance(data["lat"], (int, float)):
            return False
        if not isinstance(data["lon"], (int, float)):
            return False
        if not is_iso_datetime(data["forecast_origin"]):
            return False
        if not isinstance(data["days"], list):
            return False

        for day in data["days"]:
            if not isinstance(day, dict):
                return False
            if not all(k in day for k in ["date", "day", "snapshots"]):
                return False
            if not is_iso_date(day["date"]):
                return False
            if not isinstance(day["day"], int):
                return False
            if not isinstance(day["snapshots"], list):
                return False
            for snap in day["snapshots"]:
                if not isinstance(snap, dict):
                    return False

                required_snap_keys = [
                    "forecast_timestamp",
                    "forecast_lead_hours",
                    "features"
                ]
                if not all(k in snap for k in required_snap_keys):
                    return False

                if not is_iso_datetime(snap["forecast_timestamp"]):
                    return False
                if not isinstance(snap["forecast_lead_hours"], int):
                    return False
                if not isinstance(snap["features"], dict):
                    return False
                features = snap["features"]
                required_features = [
                    "temperature", "wind_speed", "wind_gust",
                    "precip_intensity", "pressure", "humidity"
                ]

                if not all(k in features for k in required_features):
                    return False
                for key in required_features:
                    if not isinstance(features[key], (int, float)):
                        return False

        return True

    except Exception:
        return False


def test_process_raw_valid():
    url_process = f"{BASE_URL}/{PROCESS_WEATHER_PATH}"

    with open(RAW_WEATHER_DATA_H1, "r") as f:
        data = json.load(f)

    # Process the raw data obtained
    response_process = requests.post(url_process, json=data) 
    assert response_process.status_code == STATUS_OK
    processed = response_process.json()["processed_data"]
    if isinstance(processed, str):
        processed = json.loads(processed)
    assert validate_processed_format(processed) is True

    # Use process retrieval to get processed data from database and check if it is there and in right format
    url_retrieaval = f"{BASE_URL}/{RETRIEVE_PROCESSED_WEATHER_PATH}/{HUB_ID_1}"
    response_retrieval = requests.get(
        url_retrieaval,
        params={"date": DATE_3}
    )
    stored = response_retrieval.json()
    assert validate_processed_format(stored) is True
    assert stored == processed

def test_process_raw_invalid_hub():
    url_process = f"{BASE_URL}/{PROCESS_WEATHER_PATH}"

    # Invalid latitude and longitude
    invalid_payload = {
        "currently": {"time": 123456},
        "latitude": 999, 
        "longitude": 999,  
        "hourly": {"data": []}
    }

    response = requests.post(url_process, json=invalid_payload)

    assert "No hub found" in response.json()["error"]
    assert response.status_code == STATUS_BAD_REQUEST