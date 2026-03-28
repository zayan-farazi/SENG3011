import os
import json
import logging
import numpy as np
import boto3
import joblib  # type: ignore[import-untyped]
import requests
from datetime import datetime, timezone
import tempfile
import constants

log = logging.getLogger()
log.setLevel(logging.INFO)

_MODEL = None

FEATURE_COLUMNS = [
    "temperature", "wind_speed", "wind_gust",
    "precip_intensity", "pressure", "humidity",
]

FEATURE_BOUNDS = {
    "temperature":      (-60.0,   60.0),
    "wind_speed":       (  0.0,  200.0),
    "wind_gust":        (  0.0,  250.0),
    "precip_intensity": (  0.0,   None),
    "pressure":         (870.0, 1084.0),
    "humidity":         (  0.0,    1.0),
}


def _load_model():
    """Downloads and caches the model from S3 on cold start."""
    global _MODEL
    if _MODEL is not None:
        return _MODEL
    s3 = boto3.client("s3")
    tmp = os.path.join(tempfile.gettempdir(), "risk_model.joblib")
    bucket = os.environ["DATA_BUCKET"]
    key = os.environ.get("RISK_MODEL_KEY") or os.environ.get("MODEL_KEY", constants.MODEL_S3_KEY)
    if not os.path.exists(tmp):
        log.info(f"Downloading model from s3://{bucket}/{key}")
        s3.download_file(bucket, key, tmp)
    try:
        _MODEL = joblib.load(tmp)
    except Exception as exc:
        log.exception(f"Failed to load model from s3://{bucket}/{key}: {exc}")
        raise
    return _MODEL


def _build_vector(features):
    row = []
    for col in FEATURE_COLUMNS:
        if col not in features:
            raise ValueError(f"Missing feature: '{col}'")
        val = float(features[col])
        lo, hi = FEATURE_BOUNDS[col]
        if lo is not None:
            val = max(val, lo)
        if hi is not None:
            val = min(val, hi)
        row.append(val)
    return row


def _risk_level(score):
    if score < 0.20:
        return "Low"
    elif score < 0.40:
        return "Elevated"
    elif score < 0.60:
        return "High"
    return "Critical"


def _primary_driver(features):
    weights = {
        "wind_gust": 0.40, "precip_intensity": 0.30,
        "pressure": 0.13, "wind_speed": 0.08,
        "temperature": 0.05, "humidity": 0.04,
    }
    norm = {
        "wind_gust":        lambda v: min(max(v - 29, 0) / 51, 1.0),
        "precip_intensity": lambda v: min(v / 30, 1.0),
        "pressure":         lambda v: max(0, (1013 - v) / 63),
        "wind_speed":       lambda v: min(max(v - 30, 0) / 45, 1.0),
        "temperature":      lambda v: max(0, abs(v - 21.5) / 28.5),
        "humidity":         lambda v: max(0, (v - 0.90) / 0.10),
    }
    scores = {f: weights[f] * norm[f](float(features.get(f, 0))) for f in weights}
    return max(scores, key=scores.get).replace("_", " ").title()


def _score_day(model, day_obj):
    snapshots = day_obj.get("snapshots", [])
    if not snapshots:
        raise ValueError(f"Day {day_obj.get('day', '?')} has no snapshots")

    X = np.array([_build_vector(s["features"]) for s in snapshots], dtype=np.float32)
    raw_scores = np.clip(model.predict(X), 0.0, 1.0)

    scored_snapshots = []
    for snapshot, score in zip(snapshots, raw_scores):
        score = float(score)
        scored_snapshots.append({
            "forecast_timestamp": snapshot["forecast_timestamp"],
            "forecast_lead_hours": int(snapshot.get("forecast_lead_hours", 0)),
            "risk_score": round(score, 4),
            "risk_level": _risk_level(score),
            "primary_driver": _primary_driver(snapshot["features"]),
        })

    all_scores = [s["risk_score"] for s in scored_snapshots]
    peak_score = max(all_scores)
    mean_score = round(sum(all_scores) / len(all_scores), 4)
    worst = scored_snapshots[all_scores.index(peak_score)]

    return {
        "date": day_obj["date"],
        "day": day_obj["day"],
        "peak_risk_score": round(peak_score, 4),
        "mean_risk_score": mean_score,
        "risk_level": _risk_level(peak_score),
        "primary_driver": worst["primary_driver"],
        "worst_interval": worst["forecast_timestamp"],
        "snapshots": scored_snapshots,
    }


def _build_adage_response(body, scored_days):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    hub_id = body["hub_id"]
    hub_name = body.get("hub_name", "")
    version = os.environ.get("MODEL_VERSION", "rf_v1.0")

    events = []
    for d in scored_days:
        events.append({
            "time_object": {
                "timestamp": f"{d['date']}T00:00:00",
                "duration": 1,
                "duration_unit": "day",
                "timezone": "UTC",
            },
            "event_type": "daily_risk_assessment",
            "attribute": {
                "hub_id": hub_id,
                "hub_name": hub_name,
                "day": d["day"],
                "date": d["date"],
                "peak_risk_score": d["peak_risk_score"],
                "mean_risk_score": d["mean_risk_score"],
                "risk_level": d["risk_level"],
                "primary_driver": d["primary_driver"],
                "worst_interval": d["worst_interval"],
                "snapshots": d["snapshots"],
                "model_version": version,
            },
        })

    peak_day = max(scored_days, key=lambda d: d["peak_risk_score"])
    all_peaks = [d["peak_risk_score"] for d in scored_days]
    outlook = round(max(all_peaks), 4)

    events.append({
        "time_object": {
            "timestamp": body.get("forecast_origin", now),
            "duration": 7,
            "duration_unit": "day",
            "timezone": "UTC",
        },
        "event_type": "seven_day_outlook",
        "attribute": {
            "hub_id": hub_id,
            "hub_name": hub_name,
            "lat": body.get("lat"),
            "lon": body.get("lon"),
            "outlook_risk_score": outlook,
            "outlook_risk_level": _risk_level(outlook),
            "peak_day": peak_day["date"],
            "peak_day_number": peak_day["day"],
            "forecast_origin": body.get("forecast_origin"),
            "days_assessed": len(scored_days),
            "model_version": version,
        },
    })

    return {
        "data_source": "Pirate Weather API (GFS)",
        "dataset_type": "Supply Chain Disruption Risk Assessment",
        "dataset_id": f"s3://{os.environ.get('DATA_BUCKET', 'supply-chain-data')}/risk/weather/{hub_id}/latest.json",
        "time_object": {"timestamp": now, "timezone": "UTC"},
        "events": events,
    }


def _fetch_processed_data(hub_id, date):
    base_url = os.environ["API_BASE_URL"]
    url = f"{base_url}{constants.RETRIEVE_PROCESSED_WEATHER_PATH}/{hub_id}"
    resp = requests.get(url, params={"date": date}, timeout=10)
    if resp.status_code == constants.STATUS_NOT_FOUND:
        raise LookupError(f"Processed data not found for hub {hub_id} on {date}")
    if resp.status_code != constants.STATUS_OK:
        raise RuntimeError(f"Retrieval service returned {resp.status_code}: {resp.text}")
    return resp.json()


def _is_s3_event(event):
    """Return True if the event came from an S3 bucket notification."""
    try:
        return event["Records"][0]["eventSource"] == "aws:s3"
    except (KeyError, IndexError, TypeError):
        return False


def _compute_and_store_risk(s3, bucket, hub_id, processed):
    """Run the ML model on processed data and write latest.json to S3."""
    days = processed.get("days", [])
    if not days:
        raise ValueError(f"No forecast days available for hub {hub_id}")
    if len(days) != 7:
        log.warning(f"Expected 7 day objects, received {len(days)}")

    model = _load_model()
    scored_days = [_score_day(model, d) for d in days]
    adage_response = _build_adage_response(processed, scored_days)

    s3.put_object(
        Bucket=bucket,
        Key=f"risk/weather/{hub_id}/latest.json",
        Body=json.dumps(adage_response),
        ContentType="application/json",
    )
    log.info(f"Stored risk scores to risk/weather/{hub_id}/latest.json")
    return adage_response


def _handle_s3_event(event):
    """Handle an S3 trigger, compute risk scores and store latest.json."""
    s3 = boto3.client("s3")
    results = []

    for record in event.get("Records", []):
        try:
            bucket = record["s3"]["bucket"]["name"]
            key = record["s3"]["object"]["key"]
            log.info(f"S3 event received for s3://{bucket}/{key}")

            # Extract hub_id from key pattern, processed/weather/{hub_id}/{date}.json
            parts = key.split("/")
            if len(parts) < 4 or parts[0] != "processed" or parts[1] != "weather":
                log.info(f"Ignoring S3 key outside processed/weather prefix: {key}")
                results.append({"status": "ignored", "key": key})
                continue

            hub_id = parts[2]

            # Validate hub_id against hubs.json
            hubs_obj = s3.get_object(Bucket=bucket, Key=constants.HUBS_FILE_KEY)
            hubs = json.loads(hubs_obj["Body"].read())
            if hub_id not in hubs:
                log.warning(f"S3 event for invalid hub_id: {hub_id}")
                results.append({"status": "ignored", "reason": "invalid hub_id", "key": key})
                continue

            date_str = parts[3].replace(".json", "")
            processed = _fetch_processed_data(hub_id, date_str)

            _compute_and_store_risk(s3, bucket, hub_id, processed)
            results.append({"status": "scored", "hub_id": hub_id, "key": key})

        except Exception as e:
            log.exception(f"Error processing record for {record.get('s3', {}).get('object', {}).get('key', 'unknown')}: {e}")
            results.append({"status": "error", "error": str(e), "key": record.get("s3", {}).get("object", {}).get("key", "unknown")})

    return results


def _handle_api_event(event):
    """Handle an API Gateway request, return cached risk or compute on demand."""
    s3 = boto3.client("s3")
    path_params = event.get("pathParameters") or {}
    query_params = event.get("queryStringParameters") or {}
    hub_id = path_params.get("hub_id")

    if not hub_id:
        return response(constants.STATUS_BAD_REQUEST, {"error": "Missing hub_id"})

    bucket = os.environ["DATA_BUCKET"]
    obj = s3.get_object(Bucket=bucket, Key=constants.HUBS_FILE_KEY)
    hubs = json.loads(obj["Body"].read())
    if hub_id not in hubs:
        return response(constants.STATUS_BAD_REQUEST, {"error": "Invalid hub_id"})

    date = query_params.get("date")
    if date:
        try:
            datetime.strptime(date, constants.DATE_FORMAT)
        except ValueError:
            return response(constants.STATUS_BAD_REQUEST, {"error": "Invalid date format. Use DD-MM-YYYY"})
    else:
        date = datetime.now(timezone.utc).strftime(constants.DATE_FORMAT)

    # Try to return the precomputed cached result
    try:
        cached = s3.get_object(
            Bucket=bucket, Key=f"risk/weather/{hub_id}/latest.json"
        )
        adage_response = json.loads(cached["Body"].read())
        log.info(f"Returning cached risk scores for hub {hub_id}")
        return response(constants.STATUS_OK, adage_response)
    except s3.exceptions.NoSuchKey:
        log.info(f"No cached risk for hub {hub_id}, computing on demand")

    # Fallback ie compute on demand
    processed = _fetch_processed_data(hub_id, date)

    adage_response = _compute_and_store_risk(s3, bucket, hub_id, processed)
    return response(constants.STATUS_OK, adage_response)


def lambda_handler(event, context):
    try:
        if not os.environ.get("DATA_BUCKET"):
            return response(constants.STATUS_INTERNAL_SERVER_ERROR, {"error": "Missing DATA_BUCKET configuration"})
        if not os.environ.get("API_BASE_URL"):
            return response(constants.STATUS_INTERNAL_SERVER_ERROR, {"error": "Missing API_BASE_URL configuration"})
        if _is_s3_event(event):
            return _handle_s3_event(event)
        return _handle_api_event(event)

    except ValueError as e:
        return response(constants.STATUS_BAD_REQUEST, {"error": str(e)})
    except LookupError as e:
        return response(constants.STATUS_NOT_FOUND, {"error": str(e)})
    except RuntimeError as e:
        return response(502, {"error": str(e)})
    except Exception:
        log.exception("Unhandled error")
        return response(constants.STATUS_INTERNAL_SERVER_ERROR, {"error": "Internal server error"})


def response(status, body):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps(body),
    }
