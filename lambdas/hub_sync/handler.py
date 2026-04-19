import json
import logging
import os
import re
from datetime import datetime, timezone

import boto3
import requests

import constants

log = logging.getLogger()
log.setLevel(logging.INFO)

PORTWATCH_PAGE_SIZE = 1000
SKIP_NAME_TERMS = (
    "terminal",
    "anchorage",
    "berth",
    "jetty",
    "buoy",
    "offshore oil",
)


def _response(status, body):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }


def _sanitize_identifier(value):
    sanitized = re.sub(r"[^A-Za-z0-9]+", "_", str(value).strip()).strip("_")
    return sanitized.upper()


def _normalize_name(value):
    normalized = str(value or "").lower()
    normalized = normalized.replace("port of ", "")
    normalized = normalized.replace(" port", "")
    normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
    return " ".join(normalized.split())


def _clean_hub_name(value):
    name = str(value or "").strip()
    if not name:
        return name

    # remove anything in parentheses and final comma suffix
    # name = re.sub(r"\s*\([^)]*\)", "", name).strip()
    # name = re.sub(r",\s*[^,()]+$", "", name).strip()

    return re.sub(r"\s+", " ", name)


def _should_skip_feature(name):
    normalized_name = _normalize_name(name)
    # ignore facility-level records 
    return any(term in normalized_name for term in SKIP_NAME_TERMS)


def _legacy_hub_id(lat, lon, name, legacy_hubs):
    rounded_key = (round(float(lat), 3), round(float(lon), 3))
    for hub_id, hub_info in legacy_hubs.items():
        if rounded_key == (round(float(hub_info["lat"]), 3), round(float(hub_info["lon"]), 3)):
            return hub_id, hub_info

    normalized_name = _normalize_name(name)
    for hub_id, hub_info in legacy_hubs.items():
        legacy_name = _normalize_name(hub_info.get("name"))
        if legacy_name and (legacy_name == normalized_name or legacy_name in normalized_name or normalized_name in legacy_name):
            return hub_id, hub_info

    return None, None


def _fetch_portwatch_features(base_url, api_key):
    headers = {}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    features = []
    offset = 0

    while True:
        response = requests.get(
            base_url,
            params={
                "where": "1=1",
                "outFields": "*",
                "outSR": 4326,
                "f": "json",
                "resultOffset": offset,
                "resultRecordCount": PORTWATCH_PAGE_SIZE,
            },
            headers=headers,
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        page = payload.get("features", [])

        if not page:
            break

        features.extend(page)
        if len(page) < PORTWATCH_PAGE_SIZE:
            break
        offset += PORTWATCH_PAGE_SIZE

    if not features:
        raise ValueError("PortWatch returned an empty hub catalog")

    log.info("Fetched %s total PortWatch hub records", len(features))
    return features


def _normalize_feature(feature, legacy_hubs):
    attributes = feature.get("attributes", {})
    upstream_id = attributes.get("portid") or attributes.get("ObjectId")
    raw_name = attributes.get("portname") or attributes.get("fullname")
    lat = attributes.get("lat")
    lon = attributes.get("lon")

    if not upstream_id or not raw_name or lat is None or lon is None:
        raise ValueError("PortWatch feature is missing required hub fields")

    try:
        lat = float(lat)
        lon = float(lon)
    except (TypeError, ValueError) as exc:
        raise ValueError("PortWatch feature contains invalid coordinates") from exc

    if lat < -90 or lat > 90 or lon < -180 or lon > 180:
        raise ValueError("PortWatch feature contains out-of-range coordinates")

    if _should_skip_feature(raw_name):
        log.info("Skipping facility-level PortWatch record source_port_id=%s name=%s", upstream_id, raw_name)
        return None, None

    name = _clean_hub_name(raw_name)
    hub_id, hub_info = _legacy_hub_id(lat, lon, name, legacy_hubs)
    if not hub_id:
        hub_id = f"PW_{_sanitize_identifier(upstream_id)}"
    else:
        # retain legacy name
        name = hub_info.get("name")

    return hub_id, {
        "name": name,
        "lat": round(lat, 3),
        "lon": round(lon, 3),
        "country": attributes.get("country"),
        "locode": attributes.get("LOCODE"),
        "source": "PortWatch",
        "source_port_id": str(upstream_id),
    }


def _build_runtime_catalog(features, legacy_hubs):
    catalog = {}
    seen_source_ids = set()

    for feature in features:
        attributes = feature.get("attributes", {})
        source_id = str(attributes.get("portid") or attributes.get("ObjectId"))
        if source_id in seen_source_ids:
            continue
        seen_source_ids.add(source_id)

        hub_id, hub_info = _normalize_feature(feature, legacy_hubs)
        if hub_id is None:
            continue
        if hub_id in catalog:
            log.warning(
                "Skipping duplicate final hub_id during sync hub_id=%s source_port_id=%s name=%s",
                hub_id,
                hub_info.get("source_port_id"),
                hub_info.get("name"),
            )
            continue
        catalog[hub_id] = hub_info

    if not catalog:
        raise ValueError("Normalized PortWatch catalog is empty")

    log.info(
        "Built runtime hub catalog catalog_size=%s", len(catalog),
    )
    return catalog


def _write_catalog(s3, bucket, runtime_key, history_prefix, catalog):
    body = json.dumps(catalog, sort_keys=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    snapshot_key = f"{history_prefix}/{timestamp}.json"

    log.info(
        "Writing hub catalog to S3 bucket=%s runtime_key=%s snapshot_key=%s",
        bucket,
        runtime_key,
        snapshot_key,
    )
    s3.put_object(Bucket=bucket, Key=runtime_key, Body=body, ContentType="application/json")
    s3.put_object(Bucket=bucket, Key=snapshot_key, Body=body, ContentType="application/json")

    return snapshot_key


def lambda_handler(event, context):
    bucket = os.environ.get("DATA_BUCKET")
    portwatch_hubs_url = os.environ.get("PORTWATCH_HUBS_URL")
    portwatch_api_key = os.environ.get("PORTWATCH_API_KEY", "")
    hubs_runtime_key = os.environ.get("HUBS_RUNTIME_KEY", constants.HUBS_RUNTIME_KEY)
    hubs_seed_key = os.environ.get("HUBS_SEED_KEY", constants.HUBS_SEED_KEY)
    hubs_history_prefix = os.environ.get("HUBS_HISTORY_PREFIX", constants.HUBS_HISTORY_PREFIX)

    if not bucket:
        return _response(constants.STATUS_INTERNAL_SERVER_ERROR, {"error": "Missing DATA_BUCKET configuration"})
    if not portwatch_hubs_url:
        return _response(constants.STATUS_INTERNAL_SERVER_ERROR, {"error": "Missing PORTWATCH_HUBS_URL configuration"})

    s3 = boto3.client("s3")

    try:
        legacy_hubs_obj = s3.get_object(Bucket=bucket, Key=hubs_seed_key)
        legacy_hubs = json.loads(legacy_hubs_obj["Body"].read().decode("utf-8"))

        features = _fetch_portwatch_features(portwatch_hubs_url, portwatch_api_key)
        catalog = _build_runtime_catalog(features, legacy_hubs)
        snapshot_key = _write_catalog(s3, bucket, hubs_runtime_key, hubs_history_prefix, catalog)

        return _response(
            constants.STATUS_OK,
            {
                "message": "Hub catalog sync complete",
                "hub_count": len(catalog),
                "runtime_key": hubs_runtime_key,
                "snapshot_key": snapshot_key,
            },
        )
    except ValueError as exc:
        log.exception(f"PortWatch hub sync rejected: {exc}")
        return _response(constants.STATUS_BAD_REQUEST, {"error": str(exc)})
    except Exception as exc:
        log.exception(f"PortWatch hub sync failed: {exc}")
        return _response(constants.STATUS_INTERNAL_SERVER_ERROR, {"error": "Hub sync failed"})
