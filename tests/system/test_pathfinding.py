import requests
import os
from constants import STATUS_BAD_REQUEST, STATUS_OK
from tests.test_constants import HUB_ID_1

BASE_URL = os.environ["STAGING_BASE_URL"]

def test_pathfinding():
    hub_id_1 = HUB_ID_1
    hub_id_2 = "H003"
    
    res = requests.get(f"{BASE_URL}/ese/v1/pathfinding/{hub_id_1}/{hub_id_2}")
    
    print(f"Status: {res.status_code}")
    print(f"Body: {res.json()}")
    
    assert res.status_code == STATUS_OK
    body = res.json()
    assert "route" in body
    assert "total_distance_km" in body
    assert "average_risk_score" in body
    assert isinstance(body["route"], list)
    assert len(body["route"]) >= 2
    assert body["route"][0]["hub_id"] == hub_id_1
    assert body["route"][-1]["hub_id"] == hub_id_2
    assert body["total_distance_km"] > 0

    for stop in body["route"]:
        assert set(stop.keys()) == {
            "hub_id",
            "name",
            "latitude",
            "longitude",
            "risk_score",
        }
        assert isinstance(stop["hub_id"], str)
        assert isinstance(stop["name"], str)
        assert isinstance(stop["latitude"], (int, float))
        assert isinstance(stop["longitude"], (int, float))

def test_pathfinding_invalid():
    hub_id_1 = "1"
    hub_id_2 = "-1"

    res = requests.get(f"{BASE_URL}/ese/v1/pathfinding/{hub_id_1}/{hub_id_2}")

    assert res.status_code == STATUS_BAD_REQUEST
    assert "error" in res.json()


