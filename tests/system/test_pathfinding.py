import requests
import os
from constants import STATUS_BAD_REQUEST, STATUS_OK

BASE_URL = os.environ["STAGING_BASE_URL"]

def test_pathfinding():
    hub_id_1 = "H001"
    hub_id_2 = "H003"
    
    res = requests.get(f"{BASE_URL}/ese/v1/pathfinding/{hub_id_1}/{hub_id_2}")
    
    print(f"Status: {res.status_code}")
    print(f"Body: {res.json()}")
    
    assert res.status_code == STATUS_OK
    body = res.json()
    assert "route" in body
    assert "total_distance_km" in body

def test_pathfinding_invalid():
    hub_id_1 = "1"
    hub_id_2 = "-1"

    res = requests.get(f"{BASE_URL}/ese/v1/pathfinding/{hub_id_1}/{hub_id_2}")

    assert res.status_code == STATUS_BAD_REQUEST



