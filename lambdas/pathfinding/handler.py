import os
import math
import json
import time
import boto3
import networkx as nx
import constants


_GRAPH_CACHE_TTL_SECONDS = 300
_SCORES_CACHE_TTL_SECONDS = 300
_SCORES_CACHE = {"loaded_at": 0.0, "scores_by_hub": None}
_GRAPH_CACHE = {
    "loaded_at": 0.0,
    "graph_key": None,
    "token": None,
    "graph": None,
}

def response(status, body):
    return {
        "statusCode": status,
        "body": json.dumps(body)
    }


def haversine_km(lat1, lon1, lat2, lon2):
    r = 6371.0

    lat1 = math.radians(float(lat1))
    lon1 = math.radians(float(lon1))
    lat2 = math.radians(float(lat2))
    lon2 = math.radians(float(lon2))

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c


def load_all_risk_scores(dynamodb):
    now = time.time()
    if (
        _SCORES_CACHE["scores_by_hub"] is not None
        and now - _SCORES_CACHE["loaded_at"] < _SCORES_CACHE_TTL_SECONDS
    ):
        return _SCORES_CACHE["scores_by_hub"]

    table = dynamodb.Table(os.environ.get("SCORES_TABLE_NAME", "scores"))

    items = []
    scan_response = table.scan()
    items.extend(scan_response.get("Items", []))

    while "LastEvaluatedKey" in scan_response:
        scan_response = table.scan(
            ExclusiveStartKey=scan_response["LastEvaluatedKey"]
        )
        items.extend(scan_response.get("Items", []))

    scores_by_hub = {}

    for item in items:
        hub_id = item.get("hub_id")
        risk_score = item.get("risk_score")

        if hub_id is None or risk_score is None:
            continue
        risk_score = float(risk_score)
        scores_by_hub[hub_id] = risk_score

    _SCORES_CACHE["loaded_at"] = now
    _SCORES_CACHE["scores_by_hub"] = scores_by_hub
    return scores_by_hub


def risk_scalar(hub_id, scores_by_hub):
    risk_score = scores_by_hub.get(hub_id)
    if risk_score is None:
        return 1
    return 1 + risk_score 


def load_precomputed_graph(s3, bucket_name):
    now = time.time()
    graph_key = os.environ.get("HUB_GRAPH_RUNTIME_KEY", constants.HUB_GRAPH_RUNTIME_KEY)
    metadata = s3.head_object(Bucket=bucket_name, Key=graph_key)
    cache_token = (
        metadata.get("ETag"),
        metadata.get("LastModified"),
        metadata.get("ContentLength"),
    )

    if (
        _GRAPH_CACHE["graph"] is not None
        and now - _GRAPH_CACHE["loaded_at"] < _GRAPH_CACHE_TTL_SECONDS
        and _GRAPH_CACHE["graph_key"] == graph_key
        and _GRAPH_CACHE["token"] == cache_token
    ):
        return _GRAPH_CACHE["graph"]

    response = s3.get_object(Bucket=bucket_name, Key=graph_key)
    artifact = json.loads(response["Body"].read().decode("utf-8"))

    G = nx.DiGraph()

    for hub_id, hub in artifact.get("nodes", {}).items():
        G.add_node(
            hub_id,
            name=hub["name"],
            lat=float(hub["lat"]),
            lon=float(hub["lon"]),
            type="monitored",
        )

    for hub_id, neighbours in artifact.get("edges", {}).items():
        for neighbour in neighbours:
            G.add_edge(
                hub_id,
                neighbour["to"],
                distance_km=float(neighbour["distance_km"]),
            )

    _GRAPH_CACHE["loaded_at"] = now
    _GRAPH_CACHE["graph_key"] = graph_key
    _GRAPH_CACHE["token"] = cache_token
    _GRAPH_CACHE["graph"] = G
    return G


def path_details_json(path, graph, scores_by_hub):
    route = []
    risk_scores = []

    for hub_id in path:
        risk_score = scores_by_hub.get(hub_id)

        if risk_score is not None:
            risk_score = float(risk_score)
            risk_scores.append(risk_score)

        route.append({
            "hub_id": hub_id,
            "name": graph.nodes[hub_id]["name"],
            "latitude": graph.nodes[hub_id]["lat"],
            "longitude": graph.nodes[hub_id]["lon"],
            "risk_score": risk_score
        })

    total_distance_km = 0.0
    for i in range(len(path) - 1):
        total_distance_km += graph[path[i]][path[i + 1]]["distance_km"]

    average_risk_score = None
    if risk_scores:
        average_risk_score = sum(risk_scores) / len(risk_scores)

    return {
        "route": route,
        "total_distance_km": total_distance_km,
        "average_risk_score": average_risk_score
    }


def lambda_handler(event, context):
    path_params = event.get("pathParameters") or {}
    hub_id_1 = path_params.get("hub_id_1")
    hub_id_2 = path_params.get("hub_id_2")

    if not hub_id_1 or not hub_id_2:
        return response(constants.STATUS_BAD_REQUEST, {"error": "Missing hub_id(s)"})
    

    s3 = boto3.client(
        "s3",
        region_name=os.environ.get("AWS_REGION", constants.DEFAULT_REGION)
    )
    dynamodb = boto3.resource(
        "dynamodb",
        region_name=os.environ.get("AWS_REGION", constants.DEFAULT_REGION)
    )

    bucket_name = os.environ.get("DATA_BUCKET")

    if not bucket_name:
        return response(500, {"error": "DATA_BUCKET is not configured"})

    scores_by_hub = load_all_risk_scores(dynamodb=dynamodb)

    try:
        G = load_precomputed_graph(s3=s3, bucket_name=bucket_name)
    except Exception:
        return response(constants.STATUS_INTERNAL_SERVER_ERROR, {"error": "Hub graph is unavailable"})

    if hub_id_1 not in G or hub_id_2 not in G:
        return response(
            constants.STATUS_BAD_REQUEST,
            {"error": f"One or both hub IDs not found: {hub_id_1}, {hub_id_2}"}
        )

    try:
        path = nx.dijkstra_path(
            G,
            source=hub_id_1,
            target=hub_id_2,
            weight=lambda _u, v, data: data["distance_km"] * risk_scalar(v, scores_by_hub),
        )
    except nx.NodeNotFound:
        return response(constants.STATUS_BAD_REQUEST, {"error": f"One or both hub IDs not found: {hub_id_1}, {hub_id_2}"})
    except nx.NetworkXNoPath:
        return response(constants.STATUS_INTERNAL_SERVER_ERROR, {"error": "graph construction error"})    

    result = path_details_json(path, G, scores_by_hub)

    return response(200, result)
