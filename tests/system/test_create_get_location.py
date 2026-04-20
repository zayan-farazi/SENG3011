import os
import uuid
import requests
import pytest
from constants import STATUS_OK, STATUS_BAD_REQUEST, STATUS_NOT_FOUND, LOCATION_PATH
from tests.test_constants import HUB_INVALID, HUB_ID_1

BASE_URL = os.environ["STAGING_BASE_URL"]
AUTH_ID_TOKEN = os.environ.get("AUTH_ID_TOKEN")


def auth_headers():
    if not AUTH_ID_TOKEN:
        pytest.skip("AUTH_ID_TOKEN is required for auth-protected location creation tests")
    return {"Authorization": f"Bearer {AUTH_ID_TOKEN}"}

def _unique_location_payload():
    unique_suffix = uuid.uuid4().hex
    lat_millis = 10000 + (int(unique_suffix[:8], 16) % 70000)
    lon_millis = 20000 + (int(unique_suffix[8:16], 16) % 140000)
    lat = lat_millis / 1000.0
    lon = lon_millis / 1000.0
    return {
        "lat": lat,
        "lon": lon,
        "name": f"Test Location {unique_suffix[:12]}"
    }

def test_location_create_valid():
    url = f"{BASE_URL}/{LOCATION_PATH}"
    payload = _unique_location_payload()

    response = requests.post(url, json=payload, headers=auth_headers())

    assert response.status_code == STATUS_OK
    data = response.json()
    assert isinstance(data, dict)
    assert "hub_id" in data
    assert isinstance(data["hub_id"], str)
    assert data["hub_id"].startswith("LOC_")

def test_location_get_valid_dynamic():
    create_url = f"{BASE_URL}/{LOCATION_PATH}"
    payload = _unique_location_payload()
    create_response = requests.post(create_url, json=payload, headers=auth_headers())
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

def test_location_get_valid_monitored():
    url = f"{BASE_URL}/{LOCATION_PATH}/{HUB_ID_1}"
    response = requests.get(url)

    assert response.status_code == STATUS_OK
    hub = response.json()
    assert hub["hub_id"] == HUB_ID_1
    assert hub["name"] == "Port of Singapore"
    assert float(hub["lat"]) == 1.264
    assert float(hub["lon"]) == 103.820
    assert hub["type"] == "monitored"

def test_location_create_invalid_name():
    url = f"{BASE_URL}/{LOCATION_PATH}"
    payload = {
        "lat": 12.345,
        "lon": 67.890,
        "name": "Invalid@Name!"
    }

    response = requests.post(url, json=payload, headers=auth_headers())

    assert response.status_code == STATUS_BAD_REQUEST
    assert "Name can contain only letters" in response.json()["error"]

def test_location_create_invalid_lon():
    url = f"{BASE_URL}/{LOCATION_PATH}"
    payload = {
        "lat": 12.345,
        "lon": 200,
        "name": "Port 1"
    }

    response = requests.post(url, json=payload, headers=auth_headers())

    assert response.status_code == STATUS_BAD_REQUEST
    assert response.json()["error"] == "Longitude must be between -180 and 180."

def test_location_get_invalid_hub():
    url = f"{BASE_URL}/{LOCATION_PATH}/{HUB_INVALID}"
    response = requests.get(url)

    assert response.status_code == STATUS_NOT_FOUND
    assert response.json() == {"error": "Invalid hub_id"}


def test_location_list_all():
    create_url = f"{BASE_URL}/{LOCATION_PATH}"
    payload = _unique_location_payload()
    create_response = requests.post(create_url, json=payload, headers=auth_headers())
    assert create_response.status_code == STATUS_OK
    created_hub_id = create_response.json()["hub_id"]

    list_url = f"{BASE_URL}/{LOCATION_PATH}/list"
    response = requests.get(list_url)

    assert response.status_code == STATUS_OK
    body = response.json()
    assert "hubs" in body
    assert isinstance(body["hubs"], list)
    assert any(hub["hub_id"] == HUB_ID_1 for hub in body["hubs"])
    assert any(hub["hub_id"] == created_hub_id for hub in body["hubs"])
    for hub in body["hubs"]:
        assert set(hub.keys()) == {"hub_id", "name", "lat", "lon"}


def test_location_list_dynamic():
    create_url = f"{BASE_URL}/{LOCATION_PATH}"
    payload = _unique_location_payload()
    create_response = requests.post(create_url, json=payload, headers=auth_headers())
    assert create_response.status_code == STATUS_OK
    created_hub_id = create_response.json()["hub_id"]

    list_url = f"{BASE_URL}/{LOCATION_PATH}/list"
    response = requests.get(list_url, params={"type": "dynamic"})

    assert response.status_code == STATUS_OK
    hubs = response.json()["hubs"]
    assert any(hub["hub_id"] == created_hub_id for hub in hubs)
    for hub in hubs:
        assert set(hub.keys()) == {"hub_id", "name", "lat", "lon"}
        assert hub["hub_id"].startswith("LOC_")


def test_location_list_monitored():
    list_url = f"{BASE_URL}/{LOCATION_PATH}/list"
    response = requests.get(list_url, params={"type": "monitored"})

    assert response.status_code == STATUS_OK
    hubs = response.json()["hubs"]
    assert any(hub["hub_id"] == HUB_ID_1 for hub in hubs)
    for hub in hubs:
        assert set(hub.keys()) == {"hub_id", "name", "lat", "lon"}
        assert hub["hub_id"].startswith("H")


def test_location_list_limit():
    list_url = f"{BASE_URL}/{LOCATION_PATH}/list"
    response = requests.get(list_url, params={"limit": 5})

    assert response.status_code == STATUS_OK
    hubs = response.json()["hubs"]
    assert len(hubs) == 5


def test_location_list_invalid_limit():
    list_url = f"{BASE_URL}/{LOCATION_PATH}/list"
    response = requests.get(list_url, params={"limit": 0})

    assert response.status_code == STATUS_BAD_REQUEST
    assert response.json() == {
        "error": "Query parameter 'limit' must be a positive integer"
    }


def test_location_list_invalid_type():
    list_url = f"{BASE_URL}/{LOCATION_PATH}/list"
    response = requests.get(list_url, params={"type": "invalid"})

    assert response.status_code == STATUS_BAD_REQUEST
    assert response.json() == {
        "error": "Query parameter 'type' must be one of: dynamic or monitored"
    }
