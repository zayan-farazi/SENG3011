import os

import boto3
import botocore

import constants
from hub_catalog import load_hubs


def _s3_client():
    return boto3.client("s3", region_name=os.environ.get("AWS_REGION", constants.DEFAULT_REGION))


def _dynamodb_resource():
    return boto3.resource("dynamodb", region_name=os.environ.get("AWS_REGION", constants.DEFAULT_REGION))


def get_dynamic_hub(hub_id, dynamodb=None):
    dynamodb = dynamodb or _dynamodb_resource()
    table = dynamodb.Table(os.environ.get("LOCATION_TABLE_NAME", "locations"))

    try:
        item = table.get_item(Key={"hub_id": hub_id}).get("Item")
    except botocore.exceptions.ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        if error_code in {"ResourceNotFoundException", "ResourceNotFound"}:
            return None
        raise

    if not item or item.get("type") != "dynamic":
        return None

    return {
        "hub_id": item["hub_id"],
        "name": item["name"],
        "lat": float(item["lat"]),
        "lon": float(item["lon"]),
        "type": "dynamic",
    }


def get_monitored_hub(hub_id, bucket_name, s3=None):
    if not bucket_name:
        return None

    s3 = s3 or _s3_client()
    hubs = load_hubs(s3, bucket_name)
    hub = hubs.get(hub_id)
    if not hub:
        return None

    return {
        "hub_id": hub_id,
        "name": hub["name"],
        "lat": float(hub["lat"]),
        "lon": float(hub["lon"]),
        "type": "monitored",
    }


def resolve_hub(hub_id, bucket_name, s3=None, dynamodb=None):
    dynamic_hub = get_dynamic_hub(hub_id, dynamodb=dynamodb)
    if dynamic_hub:
        return dynamic_hub

    return get_monitored_hub(hub_id, bucket_name, s3=s3)
