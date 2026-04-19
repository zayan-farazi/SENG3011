import json
import os
import pytest
import boto3
import networkx as nx
from decimal import Decimal
from unittest.mock import patch
from moto import mock_aws

import constants
from tests.test_constants import TEST_BUCKET_NAME
from lambdas.pathfinding.handler import (
    haversine_km,
    load_all_risk_scores,
    risk_scalar,
    build_hub_graph,
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

def test_risk_scalar_missing_hub_returns_error_response():
    result = risk_scalar("MISSING", {})
    assert result["statusCode"] == constants.STATUS_INTERNAL_SERVER_ERROR
    assert "error" in json.loads(result["body"])


# ---------------------------------------------------------------------------
# load_all_risk_scores
# ---------------------------------------------------------------------------

def test_load_all_risk_scores_returns_scores(setup_dynamodb):
    dynamodb = boto3.resource("dynamodb", region_name=constants.DEFAULT_REGION)
    dynamodb.Table("scores").put_item(Item={"hub_id": "H1", "risk_score": Decimal("0.42")})
    scores = load_all_risk_scores(dynamodb)
    assert scores["H1"] == pytest.approx(0.42)

def test_load_all_risk_scores_skips_items_missing_score(setup_dynamodb):
    boto3.client("dynamodb", region_name=constants.DEFAULT_REGION).put_item(
        TableName="scores",
        Item={"hub_id": {"S": "NO_SCORE"}},
    )
    dynamodb = boto3.resource("dynamodb", region_name=constants.DEFAULT_REGION)
    scores = load_all_risk_scores(dynamodb)
    assert "NO_SCORE" not in scores


# ---------------------------------------------------------------------------
# build_hub_graph
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
def mock_graph():
    with patch("lambdas.pathfinding.handler.load_hubs", return_value=MOCK_HUBS):
        from unittest.mock import MagicMock
        yield build_hub_graph(MagicMock(), "bucket", k=2, scores_by_hub=MOCK_SCORES)

def test_graph_has_all_hubs_as_nodes(mock_graph):
    assert set(mock_graph.nodes) == set(MOCK_HUBS.keys())

def test_graph_edges_have_weight_and_distance(mock_graph):
    for _, _, data in mock_graph.edges(data=True):
        assert "weight" in data
        assert "distance_km" in data

def test_graph_zero_risk_weight_equals_raw_distance(mock_graph):
    for _, _, data in mock_graph.edges(data=True):
        assert data["weight"] == pytest.approx(data["distance_km"])


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
    with open(constants.HUBS_FILE_KEY) as f:
        hubs = json.load(f)
    for hub_id in hubs:
        dynamodb.Table("scores").put_item(Item={"hub_id": hub_id, "risk_score": Decimal("0.1")})
    result = lambda_handler({"pathParameters": {"hub_id_1": "FAKE", "hub_id_2": "ALSO_FAKE"}}, {})
    assert result["statusCode"] == constants.STATUS_BAD_REQUEST

def test_handler_valid_route_returns_200_with_expected_keys(setup_s3_dynamodb):
    dynamodb = boto3.resource("dynamodb", region_name=constants.DEFAULT_REGION)
    with open(constants.HUBS_FILE_KEY) as f:
        hubs = json.load(f)
    for hub_id in hubs:
        dynamodb.Table("scores").put_item(Item={"hub_id": hub_id, "risk_score": Decimal("0.1")})

    hub_ids = list(hubs.keys())
    result = lambda_handler({"pathParameters": {"hub_id_1": hub_ids[0], "hub_id_2": hub_ids[-1]}}, {})

    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    assert "route" in body
    assert "total_distance_km" in body
    assert body["route"][0]["hub_id"] == hub_ids[0]
    assert body["route"][-1]["hub_id"] == hub_ids[-1]