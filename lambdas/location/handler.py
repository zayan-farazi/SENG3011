import re
import os
import boto3
import json
from datetime import datetime, timezone
from boto3.dynamodb.conditions import Key
from decimal import Decimal
import uuid
import constants
import logging
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
    lat = round(float(lat), 3)
    lon = round(float(lon), 3)
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
    response = table.get_item(Key={"hub_id": hub_id})
    return response.get("Item")


def get_http_method(event):
    request_context = event.get("requestContext") or {}
    http_context = request_context.get("http") or {}
    return http_context.get("method") or event.get("httpMethod")

def lambda_handler(event, context):
    region = os.environ.get("AWS_REGION", "us-east-1")
    dynamodb = boto3.resource("dynamodb", region_name=region)
    table = dynamodb.Table("locations")

    http_method = get_http_method(event)
    path_params = event.get("pathParameters") or {}
    
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

        hub = create_dynamic_hub(table, lat, lon, name)
        return response(constants.STATUS_OK, { "hub_id": hub["hub_id"] })

    # GET /ese/v1/location/{hub_id}
    if http_method == "GET":
        try:
            hub_id = path_params["hub_id"]
        except KeyError:
            logger.exception("GET /location failed: missing hub_id path parameter")
            return response(constants.STATUS_BAD_REQUEST, {"error": "Missing hub_id"})
        
        logger.info(f"Incoming request to fetch hub details for hub_id={hub_id}")
        hub = get_hub(table, hub_id)
        if hub:
            logger.info(f"Hub details found for hub_id={hub_id}")
            return response(constants.STATUS_OK, hub)
        else:
            logger.error(f"No hub details found for hub_id={hub_id}")
            return response(constants.STATUS_NOT_FOUND, {"error": "Invalid hub_id"})

    logger.error("Unhandled error")
    return response(constants.STATUS_INTERNAL_SERVER_ERROR, {"error": "Internal error completing request"})
