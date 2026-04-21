import json
import os
import pytest
import boto3
import networkx as nx
from decimal import Decimal

import constants
from lambdas.pathfinding.handler import (
    _GRAPH_CACHE,
    _SCORES_CACHE,
    haversine_km,
    load_all_risk_scores,
    risk_scalar,
    load_precomputed_graph,
    path_details_json,
    lambda_handler,
)


# ---------------------------------------------------------------------------
# haversine_km
# ---------------------------------------------------------------------------

def test_haversine_same_point_is_zero():
    assert haversine_km(51.5, -0.1, 51.5, -0.1) == pytest.approx(0.0)

def test_haversine_london_to_paris():
    dist = haversine_km(51.5074, -0.1278, 48.8566, 2.3522)
    assert dist == pytest.approx(341.0, rel=0.02)

def test_haversine_is_symmetric():
    assert haversine_km(10, 20, 30, 40) == pytest.approx(haversine_km(30, 40, 10, 20))


# ---------------------------------------------------------------------------
# risk_scalar
# ---------------------------------------------------------------------------

def test_risk_scalar_returns_one_plus_score():
    assert risk_scalar("H1", {"H1": 0.5}) == pytest.approx(1.5)



# ---------------------------------------------------------------------------
# load_all_risk_scores
# ---------------------------------------------------------------------------

def test_load_all_risk_scores_returns_scores(setup_dynamodb):
    _SCORES_CACHE["loaded_at"] = 0.0
    _SCORES_CACHE["scores_by_hub"] = None
    dynamodb = boto3.resource("dynamodb", region_name=constants.DEFAULT_REGION)
    dynamodb.Table("scores").put_item(Item={"hub_id": "H1", "risk_score": Decimal("0.42")})
    scores = load_all_risk_scores(dynamodb)
    assert scores["H1"] == pytest.approx(0.42)

def test_load_all_risk_scores_skips_items_missing_score(setup_dynamodb):
    _SCORES_CACHE["loaded_at"] = 0.0
    _SCORES_CACHE["scores_by_hub"] = None
    boto3.client("dynamodb", region_name=constants.DEFAULT_REGION).put_item(
        TableName="scores",
        Item={"hub_id": {"S": "NO_SCORE"}},
    )
    dynamodb = boto3.resource("dynamodb", region_name=constants.DEFAULT_REGION)
    scores = load_all_risk_scores(dynamodb)
    assert "NO_SCORE" not in scores


# ---------------------------------------------------------------------------
# load_precomputed_graph
# ---------------------------------------------------------------------------

MOCK_HUBS = {
    "A": {"name": "Alpha",   "lat": "0.0", "lon": "0.0"},
    "B": {"name": "Beta",    "lat": "1.0", "lon": "0.0"},
    "C": {"name": "Gamma",   "lat": "2.0", "lon": "0.0"},
    "D": {"name": "Delta",   "lat": "3.0", "lon": "0.0"},
    "E": {"name": "Epsilon", "lat": "4.0", "lon": "0.0"},
}
MOCK_SCORES = {hub_id: 0.0 for hub_id in MOCK_HUBS}

@pytest.fixture
def mock_graph_artifact_s3(setup_s3):
    setup_s3.put_object(
        Bucket=os.environ["DATA_BUCKET"],
        Key=constants.HUB_GRAPH_RUNTIME_KEY,
        Body=json.dumps(
            {
                "nodes": {
                    hub_id: {"name": hub["name"], "lat": float(hub["lat"]), "lon": float(hub["lon"])}
                    for hub_id, hub in MOCK_HUBS.items()
                },
                "edges": {
                    "A": [{"to": "B", "distance_km": 111.0}, {"to": "C", "distance_km": 222.0}],
                    "B": [{"to": "A", "distance_km": 111.0}, {"to": "C", "distance_km": 111.0}],
                    "C": [{"to": "B", "distance_km": 111.0}, {"to": "D", "distance_km": 111.0}],
                    "D": [{"to": "C", "distance_km": 111.0}, {"to": "E", "distance_km": 111.0}],
                    "E": [{"to": "D", "distance_km": 111.0}],
                },
                "k": 2,
                "generated_at": "2026-04-21T10:00:00Z",
            }
        ),
        ContentType="application/json",
    )
    return setup_s3

def test_graph_has_all_hubs_as_nodes(mock_graph_artifact_s3):
    _GRAPH_CACHE["loaded_at"] = 0.0
    _GRAPH_CACHE["graph_key"] = None
    _GRAPH_CACHE["token"] = None
    _GRAPH_CACHE["graph"] = None
    mock_graph = load_precomputed_graph(mock_graph_artifact_s3, os.environ["DATA_BUCKET"])
    assert set(mock_graph.nodes) == set(MOCK_HUBS.keys())

def test_graph_edges_have_distance(mock_graph_artifact_s3):
    _GRAPH_CACHE["loaded_at"] = 0.0
    _GRAPH_CACHE["graph_key"] = None
    _GRAPH_CACHE["token"] = None
    _GRAPH_CACHE["graph"] = None
    mock_graph = load_precomputed_graph(mock_graph_artifact_s3, os.environ["DATA_BUCKET"])
    assert isinstance(mock_graph, nx.DiGraph)
    for _, _, data in mock_graph.edges(data=True):
        assert "distance_km" in data

def test_load_precomputed_graph_reuses_cache_for_same_artifact(mock_graph_artifact_s3):
    _GRAPH_CACHE["loaded_at"] = 0.0
    _GRAPH_CACHE["graph_key"] = None
    _GRAPH_CACHE["token"] = None
    _GRAPH_CACHE["graph"] = None

    graph_1 = load_precomputed_graph(mock_graph_artifact_s3, os.environ["DATA_BUCKET"])
    graph_2 = load_precomputed_graph(mock_graph_artifact_s3, os.environ["DATA_BUCKET"])

    assert graph_1 is graph_2


# ---------------------------------------------------------------------------
# path_details_json
# ---------------------------------------------------------------------------

@pytest.fixture
def simple_graph():
    G = nx.Graph()
    G.add_node("A", name="Alpha", lat=0.0, lon=0.0)
    G.add_node("B", name="Beta",  lat=1.0, lon=0.0)
    G.add_node("C", name="Gamma", lat=2.0, lon=0.0)
    G.add_edge("A", "B", weight=100.0, distance_km=111.0)
    G.add_edge("B", "C", weight=100.0, distance_km=111.0)
    return G

def test_path_details_total_distance(simple_graph):
    result = path_details_json(["A", "B", "C"], simple_graph, {"A": 0.0, "B": 0.0, "C": 0.0})
    assert result["total_distance_km"] == pytest.approx(222.0)

def test_path_details_average_risk_score(simple_graph):
    result = path_details_json(["A", "B", "C"], simple_graph, {"A": 0.0, "B": 0.5, "C": 1.0})
    assert result["average_risk_score"] == pytest.approx(0.5)

def test_path_details_no_scores_gives_none_average(simple_graph):
    result = path_details_json(["A", "B", "C"], simple_graph, {})
    assert result["average_risk_score"] is None

def test_path_details_route_order_is_preserved(simple_graph):
    result = path_details_json(["A", "B", "C"], simple_graph, {})
    assert [s["hub_id"] for s in result["route"]] == ["A", "B", "C"]


# ---------------------------------------------------------------------------
# lambda_handler (end-to-end with mocked AWS)
# ---------------------------------------------------------------------------

def test_handler_missing_hub_ids_returns_400(setup_s3_dynamodb):
    result = lambda_handler({"pathParameters": {}}, {})
    assert result["statusCode"] == constants.STATUS_BAD_REQUEST

def test_handler_unknown_hub_ids_returns_400(setup_s3_dynamodb):
    dynamodb = boto3.resource("dynamodb", region_name=constants.DEFAULT_REGION)
    s3 = boto3.client("s3", region_name=constants.DEFAULT_REGION)
    with open(constants.HUBS_FILE_KEY) as f:
        hubs = json.load(f)
    s3.put_object(
        Bucket=os.environ["DATA_BUCKET"],
        Key=constants.HUB_GRAPH_RUNTIME_KEY,
        Body=json.dumps(
            {
                "nodes": {
                    hub_id: {"name": hub["name"], "lat": float(hub["lat"]), "lon": float(hub["lon"])}
                    for hub_id, hub in hubs.items()
                },
                "edges": {hub_id: [] for hub_id in hubs},
                "k": 0,
                "generated_at": "2026-04-21T10:00:00Z",
            }
        ),
        ContentType="application/json",
    )
    for hub_id in hubs:
        dynamodb.Table("scores").put_item(Item={"hub_id": hub_id, "risk_score": Decimal("0.1")})
    result = lambda_handler({"pathParameters": {"hub_id_1": "FAKE", "hub_id_2": "ALSO_FAKE"}}, {})
    assert result["statusCode"] == constants.STATUS_BAD_REQUEST

def test_handler_valid_route_returns_200_with_expected_keys(setup_s3_dynamodb):
    dynamodb = boto3.resource("dynamodb", region_name=constants.DEFAULT_REGION)
    with open(constants.HUBS_FILE_KEY) as f:
        hubs = json.load(f)
    s3 = boto3.client("s3", region_name=constants.DEFAULT_REGION)
    hub_ids = list(hubs.keys())
    s3.put_object(
        Bucket=os.environ["DATA_BUCKET"],
        Key=constants.HUB_GRAPH_RUNTIME_KEY,
        Body=json.dumps(
            {
                "nodes": {
                    hub_id: {"name": hub["name"], "lat": float(hub["lat"]), "lon": float(hub["lon"])}
                    for hub_id, hub in hubs.items()
                },
                "edges": {
                    hub_ids[i]: [{"to": hub_ids[i + 1], "distance_km": 100.0}]
                    for i in range(len(hub_ids) - 1)
                }
                | {hub_ids[-1]: []},
                "k": 1,
                "generated_at": "2026-04-21T10:00:00Z",
            }
        ),
        ContentType="application/json",
    )
    for hub_id in hubs:
        dynamodb.Table("scores").put_item(Item={"hub_id": hub_id, "risk_score": Decimal("0.1")})

    result = lambda_handler({"pathParameters": {"hub_id_1": hub_ids[0], "hub_id_2": hub_ids[-1]}}, {})

    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    assert "route" in body
    assert "total_distance_km" in body
    assert body["route"][0]["hub_id"] == hub_ids[0]
    assert body["route"][-1]["hub_id"] == hub_ids[-1]


def test_handler_no_path_returns_404(setup_s3_dynamodb):
    dynamodb = boto3.resource("dynamodb", region_name=constants.DEFAULT_REGION)
    with open(constants.HUBS_FILE_KEY) as f:
        hubs = json.load(f)
    s3 = boto3.client("s3", region_name=constants.DEFAULT_REGION)
    hub_ids = list(hubs.keys())
    s3.put_object(
        Bucket=os.environ["DATA_BUCKET"],
        Key=constants.HUB_GRAPH_RUNTIME_KEY,
        Body=json.dumps(
            {
                "nodes": {
                    hub_id: {"name": hub["name"], "lat": float(hub["lat"]), "lon": float(hub["lon"])}
                    for hub_id, hub in hubs.items()
                },
                "edges": {hub_id: [] for hub_id in hubs},
                "k": 0,
                "generated_at": "2026-04-21T10:00:00Z",
            }
        ),
        ContentType="application/json",
    )
    for hub_id in hubs:
        dynamodb.Table("scores").put_item(Item={"hub_id": hub_id, "risk_score": Decimal("0.1")})

    result = lambda_handler({"pathParameters": {"hub_id_1": hub_ids[0], "hub_id_2": hub_ids[1]}}, {})

    assert result["statusCode"] == constants.STATUS_NOT_FOUND
    assert "No path found between hubs" in json.loads(result["body"])["error"]
