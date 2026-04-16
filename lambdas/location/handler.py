import re
import os
import boto3  # type: ignore
import json
from datetime import datetime, timezone
from boto3.dynamodb.conditions import Key, Attr  # type: ignore
from decimal import Decimal
import uuid
import constants
import logging
from hub_catalog import load_hubs
from hub_lookup import get_dynamic_hub
from lambdas.metrics import log_metric

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)

def response(status, body):
    return {"statusCode": status, "body": json.dumps(body, cls=DecimalEncoder)}

def create_dynamic_hub(table, lat, lon, name):
    # normalise lat/lon to always be 3dp (to help check uniqueness)
    lat = round(lat, 3)
    lon = round(lon, 3)
    lat_lon = f"{lat:.3f}:{lon:.3f}"

    # Check if hub exists
    logger.info(f"Checking for existing hub with lat_lon={lat_lon}")
    existing = table.query(
        IndexName="lat-lon-index",
        KeyConditionExpression=Key("lat_lon").eq(lat_lon)
    )
    if existing["Items"]:
        hub = existing["Items"][0]
        logger.info(f"Found existing hub {hub['hub_id']} for lat_lon={lat_lon}")
        return hub

    # Insert new dynamic hub
    hub_id = f"LOC_{uuid.uuid4().hex[:8]}"
    item = {
        "hub_id": hub_id,
        "lat_lon": lat_lon,
        "name": name,
        "lat": Decimal(str(lat)),
        "lon": Decimal(str(lon)),
        "type": "dynamic",
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    logger.info(f"Creating new hub: hub_id={hub_id} name={name} lat={lat} lon={lon}")
    log_metric(constants.DYNAMIC_HUBS_CREATED, 1, constants.LOCATION_SERVICE)
    table.put_item(Item=item)
    return item

def get_hub(table, hub_id):
    hub = get_dynamic_hub(hub_id)
    if not hub:
        return None

    response = table.get_item(Key={"hub_id": hub_id})
    return response.get("Item")

def get_monitored_hub(bucket_name, hub_id):
    if not bucket_name:
        return None

    s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION", constants.DEFAULT_REGION))
    hubs = load_hubs(s3, bucket_name)
    hub = hubs.get(hub_id)
    if not hub:
        return None

    return {
        "hub_id": hub_id,
        "name": hub["name"],
        "lat": hub["lat"],
        "lon": hub["lon"],
        "type": "monitored",
    }


def list_hubs(table, bucket_name, hub_type=None):
    scan_kwargs = {"FilterExpression": Attr("type").eq("dynamic")}

    items = []
    response = table.scan(**scan_kwargs)
    items.extend(response.get("Items", []))

    while "LastEvaluatedKey" in response:
        response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"], **scan_kwargs)
        items.extend(response.get("Items", []))

    dynamic_hubs = [
        {
            "hub_id": item["hub_id"],
            "name": item["name"],
            "lat": item["lat"],
            "lon": item["lon"],
        }
        for item in items
    ]

    if hub_type == "dynamic":
        return sorted(dynamic_hubs, key=lambda hub: hub["hub_id"])

    monitored_hubs = []
    if bucket_name:
        s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION", constants.DEFAULT_REGION))
        monitored_hubs = [
            {
                "hub_id": hub_id,
                "name": hub["name"],
                "lat": hub["lat"],
                "lon": hub["lon"],
            }
            for hub_id, hub in load_hubs(s3, bucket_name).items()
        ]

    if hub_type == "monitored":
        return sorted(monitored_hubs, key=lambda hub: hub["hub_id"])

    return sorted([*monitored_hubs, *dynamic_hubs], key=lambda hub: hub["hub_id"])


def parse_limit(raw_limit):
    if raw_limit is None:
        return None

    try:
        limit = int(raw_limit)
    except (TypeError, ValueError):
        return None

    if limit <= 0:
        return None

    return limit


def get_http_method(event):
    request_context = event.get("requestContext") or {}
    http_context = request_context.get("http") or {}
    return http_context.get("method") or event.get("httpMethod")

def get_request_path(event):
    raw_path = event.get("rawPath")
    if raw_path:
        return raw_path

    request_context = event.get("requestContext") or {}
    http_context = request_context.get("http") or {}
    if http_context.get("path"):
        return http_context["path"]

    return event.get("path", "")

def lambda_handler(event, context):
    region = os.environ.get("AWS_REGION", constants.DEFAULT_REGION)
    dynamodb = boto3.resource("dynamodb", region_name=region)
    table = dynamodb.Table(os.environ.get("LOCATION_TABLE_NAME", "locations"))

    http_method = get_http_method(event)
    path_params = event.get("pathParameters") or {}
    bucket_name = os.environ.get("DATA_BUCKET")
    
    # POST /ese/v1/location
    if http_method == "POST":
        logger.info("Incoming request to create new location")
        body = event.get("body")
        if not body:
            logger.error("POST /location failed: request body is missing")
            return response(constants.STATUS_BAD_REQUEST, {"error": "Request body is required"})

        body = json.loads(body)
        try:
            lat = body["lat"]
            lon = body["lon"]
            name = body["name"]
        except KeyError as e:
            logger.exception(f"POST /location failed: missing required field {e.args[0]}")
            return response(
                constants.STATUS_BAD_REQUEST,
                {"error": f"Missing required field {e.args[0]}"}
            )

        if not re.match(r"^[A-Za-z0-9',\- ]+$", name):
            logger.error(f"POST /location failed: invalid name {name}")
            return response(
                constants.STATUS_BAD_REQUEST,
                {
                    "error": "Name can contain only letters, numbers, apostrophe, comma, dash, and spaces."
                }
            )
        
        lat = float(lat)
        lon = float(lon)
        if lat < -90 or lat > 90:
            logger.error(f"POST /location failed: lat {lat} must be between -90 and 90")
            return response(
                constants.STATUS_BAD_REQUEST, { "error": "Latitude must be between -90 and 90." }
            )
        
        if lon < -180 or lon > 180:
            logger.error(f"POST /location failed: lon {lon} must be between -180 and 180")
            return response(
                constants.STATUS_BAD_REQUEST, { "error": "Longitude must be between -180 and 180." }
            )

        hub = create_dynamic_hub(table, lat, lon, name)
        return response(constants.STATUS_OK, { "hub_id": hub["hub_id"] })

    if http_method == "GET":
        request_path = get_request_path(event)

        # GET /ese/v1/location/{hub_id}
        hub_id = path_params.get("hub_id")
        if hub_id:
            logger.info(f"Incoming request to fetch hub details for hub_id={hub_id}")
            hub = get_hub(table, hub_id)
            if not hub:
                hub = get_monitored_hub(bucket_name, hub_id)
            if hub:
                logger.info(f"Hub details found for hub_id={hub_id}")
                return response(constants.STATUS_OK, hub)

            logger.error(f"No hub details found for hub_id={hub_id}")
            return response(constants.STATUS_NOT_FOUND, {"error": "Invalid hub_id"})

        if not request_path.endswith("/ese/v1/location/list"):
            logger.exception("GET /location failed: missing hub_id path parameter")
            return response(constants.STATUS_BAD_REQUEST, {"error": "Missing hub_id"})

        # GET /ese/v1/location/list
        query_params = event.get("queryStringParameters") or {}
        hub_type = query_params.get("type")
        raw_limit = query_params.get("limit")
        # No type: return all hubs
        # type=dynamic: return dynamic hubs
        # type=monitored: return monitored hubs
        if hub_type not in (None, "dynamic", "monitored"):
            logger.error(f"GET /location/list failed: invalid type filter {hub_type}")
            return response(
                constants.STATUS_BAD_REQUEST,
                {"error": "Query parameter 'type' must be one of: dynamic or monitored"}
            )

        limit = parse_limit(raw_limit)
        if raw_limit is not None and limit is None:
            logger.error(f"GET /location/list failed: invalid limit {raw_limit}")
            return response(
                constants.STATUS_BAD_REQUEST,
                {"error": "Query parameter 'limit' must be a positive integer"}
            )

        logger.info(
            "Listing hubs with type filter=%s limit=%s",
            hub_type or "all",
            limit if limit is not None else "all",
        )
        hubs = list_hubs(table, bucket_name, hub_type)
        if limit is not None:
            hubs = hubs[:limit]
        return response(constants.STATUS_OK, {"hubs": hubs})

    logger.error("Unhandled error")
    return response(constants.STATUS_INTERNAL_SERVER_ERROR, {"error": "Internal error completing request"})
