import json
import os

import boto3
from botocore.config import Config

import constants


AWS_REGION = os.environ.get("AWS_REGION", constants.DEFAULT_REGION)
DATA_BUCKET = os.environ["DATA_BUCKET_NAME"]
HUB_SYNC_LAMBDA_NAME = os.environ["HUB_SYNC_LAMBDA_NAME"]


def test_hub_sync_generates_runtime_catalog_and_graph():
    lambda_client = boto3.client(
        "lambda",
        region_name=AWS_REGION,
        config=Config(connect_timeout=30, read_timeout=240),
    )
    s3 = boto3.client("s3", region_name=AWS_REGION)

    response = lambda_client.invoke(
        FunctionName=HUB_SYNC_LAMBDA_NAME,
        InvocationType="RequestResponse",
        Payload=b"{}",
    )

    assert response["StatusCode"] == 200

    payload = json.loads(response["Payload"].read().decode("utf-8"))
    assert payload["statusCode"] == constants.STATUS_OK

    body = json.loads(payload["body"])
    assert body["message"] == "Hub catalog sync complete"
    assert body["hub_count"] > 0
    assert body["runtime_key"] == constants.HUBS_RUNTIME_KEY
    assert body["graph_runtime_key"] == constants.HUB_GRAPH_RUNTIME_KEY
    assert body["snapshot_key"].startswith(f"{constants.HUBS_HISTORY_PREFIX}/")

    runtime_obj = s3.get_object(Bucket=DATA_BUCKET, Key=constants.HUBS_RUNTIME_KEY)
    runtime_hubs = json.loads(runtime_obj["Body"].read().decode("utf-8"))
    assert isinstance(runtime_hubs, dict)
    assert len(runtime_hubs) == body["hub_count"]
    assert runtime_hubs

    graph_obj = s3.get_object(Bucket=DATA_BUCKET, Key=constants.HUB_GRAPH_RUNTIME_KEY)
    graph_artifact = json.loads(graph_obj["Body"].read().decode("utf-8"))
    assert graph_artifact["k"] == 6
    assert "generated_at" in graph_artifact
    assert set(graph_artifact["nodes"]) == set(runtime_hubs)
    assert set(graph_artifact["edges"]) == set(runtime_hubs)

    sample_hub_id = next(iter(runtime_hubs))
    sample_node = graph_artifact["nodes"][sample_hub_id]
    assert set(sample_node.keys()) == {"name", "lat", "lon"}

    if len(runtime_hubs) > 1:
        assert graph_artifact["edges"][sample_hub_id]
        sample_edge = graph_artifact["edges"][sample_hub_id][0]
        assert set(sample_edge.keys()) == {"to", "distance_km"}
        assert sample_edge["to"] in runtime_hubs
        assert sample_edge["distance_km"] > 0
