import os
import math
import json
import boto3
import networkx as nx
import constants
from hub_catalog import load_hubs

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

    return scores_by_hub


def risk_scalar(hub_id, scores_by_hub):
    risk_score = scores_by_hub.get(hub_id)
    if risk_score is None:
        return response(constants.STATUS_INTERNAL_SERVER_ERROR, {"error": "Cant find risk score"})
    return 1 + risk_score 


def build_hub_graph(s3, bucket_name, k=4, scores_by_hub=None):
    hubs = load_hubs(s3, bucket_name)
    scores_by_hub = scores_by_hub or {}

    G = nx.Graph()

    for hub_id, hub in hubs.items():
        G.add_node(
            hub_id,
            name=hub["name"],
            lat=float(hub["lat"]),
            lon=float(hub["lon"]),
            type="monitored",
            risk_score=scores_by_hub.get(hub_id),
        )

    hub_ids = list(hubs.keys())

    for hub_id in hub_ids:
        current_hub = hubs[hub_id]
        distances = []

        for other_hub_id in hub_ids:
            if other_hub_id == hub_id:
                continue

            other_hub = hubs[other_hub_id]

            raw_distance = haversine_km(
                current_hub["lat"],
                current_hub["lon"],
                other_hub["lat"],
                other_hub["lon"],
            )

            weighted_distance = raw_distance * risk_scalar(other_hub_id, scores_by_hub)

            distances.append((other_hub_id, raw_distance, weighted_distance))

        distances.sort(key=lambda x: x[2])

        for neighbour_hub_id, raw_distance, weighted_distance in distances[:k]:
            G.add_edge(
                hub_id,
                neighbour_hub_id,
                weight=weighted_distance,
                distance_km=raw_distance,
            )

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

    G = build_hub_graph(
        s3=s3,
        bucket_name=bucket_name,
        k=4,
        scores_by_hub=scores_by_hub,
    )

    try:
        path = nx.dijkstra_path(G, source=hub_id_1, target=hub_id_2, weight="weight")
    except nx.NodeNotFound:
        return response(constants.STATUS_BAD_REQUEST, {"error": f"One or both hub IDs not found: {hub_id_1}, {hub_id_2}"})
    except nx.NetworkXNoPath:
        return response(constants.STATUS_INTERNAL_SERVER_ERROR, {"error": "graph construction error"})    

    result = path_details_json(path, G, scores_by_hub)

    return response(200, result)