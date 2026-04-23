import os
import json

import boto3
import requests

import constants
from constants import STATUS_BAD_REQUEST, STATUS_OK

BASE_URL = os.environ["STAGING_BASE_URL"]
AWS_REGION = os.environ.get("AWS_REGION", constants.DEFAULT_REGION)
DATA_BUCKET = os.environ["DATA_BUCKET_NAME"]


def _load_graph_artifact():
    s3 = boto3.client("s3", region_name=AWS_REGION)
    graph_obj = s3.get_object(Bucket=DATA_BUCKET, Key=constants.HUB_GRAPH_RUNTIME_KEY)
    return json.loads(graph_obj["Body"].read().decode("utf-8"))


def _pick_reachable_path_pair():
    graph_artifact = _load_graph_artifact()
    for hub_id, edges in graph_artifact["edges"].items():
        if edges:
            return hub_id, edges[0]["to"]
    raise AssertionError("Precomputed hub graph does not contain any traversable edges")

def test_pathfinding():
    hub_id_1, hub_id_2 = _pick_reachable_path_pair()
    
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

