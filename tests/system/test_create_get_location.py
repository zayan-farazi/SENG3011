import os
import uuid
import requests
from constants import STATUS_OK, STATUS_BAD_REQUEST, STATUS_NOT_FOUND, LOCATION_PATH
from tests.test_constants import HUB_INVALID, HUB_ID_1

BASE_URL = os.environ["STAGING_BASE_URL"]

def _unique_location_payload():
    unique_suffix = uuid.uuid4().hex[:8]
    lat = 10.0 + (int(unique_suffix, 16) % 500) / 1000.0
    lon = 50.0 + (int(unique_suffix, 16) % 500) / 1000.0
    return {
        "lat": lat,
        "lon": lon,
        "name": f"Test Location {unique_suffix}"
    }

def test_location_create_valid():
    url = f"{BASE_URL}/{LOCATION_PATH}"
    payload = _unique_location_payload()

    response = requests.post(url, json=payload)

    assert response.status_code == STATUS_OK
    data = response.json()
    assert isinstance(data, dict)
    assert "hub_id" in data
    assert isinstance(data["hub_id"], str)
    assert data["hub_id"].startswith("LOC_")

def test_location_get_valid_dynamic():
    create_url = f"{BASE_URL}/{LOCATION_PATH}"
    payload = _unique_location_payload()
    create_response = requests.post(create_url, json=payload)
    assert create_response.status_code == STATUS_OK
    hub_id = create_response.json()["hub_id"]

    get_url = f"{BASE_URL}/{LOCATION_PATH}/{hub_id}"
    get_response = requests.get(get_url)

    assert get_response.status_code == STATUS_OK
    hub = get_response.json()
    assert hub["hub_id"] == hub_id
    assert hub["name"] == payload["name"]
    assert float(hub["lat"]) == round(payload["lat"], 3)
    assert float(hub["lon"]) == round(payload["lon"], 3)
    assert hub["type"] == "dynamic"

def test_location_get_valid_scheduled():
    url = f"{BASE_URL}/{LOCATION_PATH}/{HUB_ID_1}"
    response = requests.get(url)

    assert response.status_code == STATUS_OK
    hub = response.json()
    assert hub["hub_id"] == HUB_ID_1
    assert hub["name"] == "Port of Singapore"
    assert float(hub["lat"]) == 1.264
    assert float(hub["lon"]) == 103.820
    assert hub["type"] == "scheduled"

def test_location_create_invalid_name():
    url = f"{BASE_URL}/{LOCATION_PATH}"
    payload = {
        "lat": 12.345,
        "lon": 67.890,
        "name": "Invalid@Name!"
    }

    response = requests.post(url, json=payload)

    assert response.status_code == STATUS_BAD_REQUEST
    assert "Name can contain only letters" in response.json()["error"]

def test_location_get_invalid_hub():
    url = f"{BASE_URL}/{LOCATION_PATH}/{HUB_INVALID}"
    response = requests.get(url)

    assert response.status_code == STATUS_NOT_FOUND
    assert response.json() == {"error": "Invalid hub_id"}
