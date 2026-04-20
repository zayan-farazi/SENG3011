import json
import logging
import math
import os
import tempfile
import uuid
from datetime import datetime, timezone
from typing import Optional
from decimal import Decimal
import boto3
from boto3.dynamodb.conditions import Key
import joblib  # type: ignore[import-untyped]
import numpy as np
import requests

import constants
from lambdas.metrics import log_metric

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Module-level caches (survive across warm Lambda invocations)
# ---------------------------------------------------------------------------
_MODEL = None
_NEWS_API_KEY: Optional[str] = None

# ---------------------------------------------------------------------------
# News API configuration
# ---------------------------------------------------------------------------
NEWS_API_BASE_URL = os.environ.get(
    "NEWS_API_BASE_URL",
    "https://i9pdxmupj7.execute-api.ap-southeast-2.amazonaws.com",
)
NEWS_API_KEY_SSM_PARAM = os.environ.get("NEWS_API_KEY_SSM_PARAM", "/seng3011/news-api-key")
SENTIMENT_TIMEOUT_SECONDS = 15

# ---------------------------------------------------------------------------
# Sentiment configuration
#
# The News Sentiment API enforces a strict concurrency limit of 1 request
# per API key. To reliably serve 60+ concurrent hubs, we use only the 7d timeframe
#
# article_threshold: expected typical article count. Used to compute
#                    confidence via 1 - exp(-count/threshold).
# ---------------------------------------------------------------------------
TIMEFRAME_CONFIG: dict[str, dict] = { # did this so that we can integrate 24hr and 30d news sentiment score option in the future when the news setiment analysis api can support it
    "7d":  {"base_weight": 1.0, "article_threshold": 100},
}

# Combination weights for weather vs geo.
WEATHER_WEIGHT = float(os.environ.get("WEATHER_RISK_WEIGHT", "0.65"))
GEO_WEIGHT     = float(os.environ.get("GEO_RISK_WEIGHT",     "0.35"))

# ---------------------------------------------------------------------------
# Per-hub geopolitical country map.
# ---------------------------------------------------------------------------
HUB_GEO_META: dict[str, dict] = {
    "H001": {
        "country": "Singapore",
    },
    "H002": {
        "country": "China",
    },
    "H003": {
        "country": "Netherlands",
    },
    "H004": {
        "country": "Australia",
    },
    "H005": {
        "country": "United States",
    },
    "H006": {
        "country": "South Africa",
    },
    "H007": {
        "country": "United Arab Emirates",
    },
    "H008": {
        "country": "Brazil",
    },
}

# ---------------------------------------------------------------------------
# ML model feature configuration
# ---------------------------------------------------------------------------
FEATURE_COLUMNS = [
    "temperature", "wind_speed", "wind_gust",
    "precip_intensity", "pressure", "humidity",
]

FEATURE_BOUNDS = {
    "temperature":     (-60.0, 60.0),
    "wind_speed":      (0.0, 200.0),
    "wind_gust":       (0.0, 250.0),
    "precip_intensity":(0.0, None),
    "pressure":        (870.0, 1084.0),
    "humidity":        (0.0, 1.0),
}


# ===========================================================================
# MODEL LOADING
# ===========================================================================

def _load_model():
    global _MODEL
    if _MODEL is not None:
        return _MODEL
    s3 = boto3.client("s3")
    tmp = os.path.join(tempfile.gettempdir(), "risk_model.joblib")
    bucket = os.environ["DATA_BUCKET"]
    key = os.environ.get("RISK_MODEL_KEY") or os.environ.get("MODEL_KEY", constants.MODEL_S3_KEY)
    if not os.path.exists(tmp):
        logger.info(f"Downloading model from s3://{bucket}/{key}")
        s3.download_file(bucket, key, tmp)
    try:
        _MODEL = joblib.load(tmp)
    except Exception:
        logger.exception(f"Failed to load model from s3://{bucket}/{key}")
        raise
    return _MODEL


# ===========================================================================
# NEWS API KEY MANAGEMENT
# ===========================================================================

def _get_news_api_key() -> Optional[str]:
    """
    Three-tier resolution — module cache → SSM → register new key.
    Returns None (with a warning) if the news API is unreachable so the
    analytics invocation can degrade gracefully rather than fail hard.
    """
    global _NEWS_API_KEY
    if _NEWS_API_KEY:
        return _NEWS_API_KEY

    ssm_key = _load_key_from_ssm()
    if ssm_key:
        _NEWS_API_KEY = ssm_key
        logger.info("Loaded news API key from SSM")
        return _NEWS_API_KEY

    new_key = _register_new_news_api_key()
    if new_key:
        _NEWS_API_KEY = new_key
        _persist_key_to_ssm(new_key)
        logger.info("Registered and stored new news API key")
        return _NEWS_API_KEY

    logger.warning("Could not obtain a news API key; geopolitical risk will be skipped")
    return None


def _load_key_from_ssm() -> Optional[str]:
    try:
        region = os.environ.get("AWS_REGION", constants.DEFAULT_REGION)
        ssm = boto3.client("ssm", region_name=region)
        resp = ssm.get_parameter(Name=NEWS_API_KEY_SSM_PARAM, WithDecryption=True)
        return resp["Parameter"]["Value"]
    except Exception:
        return None


def _persist_key_to_ssm(key: str) -> None:
    try:
        region = os.environ.get("AWS_REGION", constants.DEFAULT_REGION)
        ssm = boto3.client("ssm", region_name=region)
        ssm.put_parameter(
            Name=NEWS_API_KEY_SSM_PARAM,
            Value=key,
            Type="SecureString",
            Overwrite=True,
        )
    except Exception:
        logger.warning("Could not persist news API key to SSM")


def _register_new_news_api_key() -> Optional[str]:
    try:
        resp = requests.post(f"{NEWS_API_BASE_URL}/api/auth/key", timeout=10)
        if resp.status_code in (200, 201):
            data = resp.json()
            for field in ("key", "api_key", "apiKey", "token", "access_token"):
                if field in data:
                    return data[field]
            if isinstance(data, str) and len(data) > 8:
                return data
        logger.warning(f"Key registration returned {resp.status_code}: {resp.text[:200]}")
    except Exception:
        logger.warning("Key registration request failed")
    return None


def _invalidate_news_api_key() -> None:
    global _NEWS_API_KEY
    _NEWS_API_KEY = None
    logger.info("Invalidated cached news API key")


# ===========================================================================
# SENTIMENT FETCHING  —  one (country, timeframe) pair per call
# ===========================================================================

def _fetch_sentiment(country: str, timeframe: str, api_key: str) -> Optional[dict]:
    """
    Calls GET /api/sentiment for one country and one timeframe.
    Returns a normalised dict on success, None on any failure.
    Known response schema:
        {
            "country": str,
            "timeframe": str,
            "articleCount": int,
            "averageSentiment": float,   # approximately [-1, 1]
            "distribution": { "positive": int, "neutral": int, "negative": int }
        }
    """
    try:
        resp = requests.get(
            f"{NEWS_API_BASE_URL}/api/sentiment",
            params={"keyword": country, "timeframe": timeframe},
            headers={"x-api-key": api_key},
            timeout=SENTIMENT_TIMEOUT_SECONDS,
        )

        if resp.status_code in (401, 403):
            logger.warning(f"News API auth failure for '{country}' / {timeframe}")
            return None

        if resp.status_code != 200:
            logger.warning(
                f"News API {resp.status_code} for '{country}' / {timeframe}: {resp.text[:200]}"
            )
            return None

        data = resp.json()
        avg_sentiment = float(data.get("averageSentiment", 0.0))
        article_count = int(data.get("articleCount", 0))
        distribution  = data.get("distribution", {})

        risk_score = round((1.0 - avg_sentiment) / 2.0, 4)

        logger.info(
            f"Sentiment '{country}' {timeframe}: "
            f"avg={avg_sentiment:.4f} risk={risk_score:.4f} articles={article_count}"
        )

        return {
            "country":          country,
            "timeframe":        timeframe,
            "article_count":    article_count,
            "avg_sentiment":    round(avg_sentiment, 4),
            "risk_score":       risk_score,
            "distribution":     distribution,
        }

    except requests.Timeout:
        logger.warning(f"Sentiment request timed out for '{country}' / {timeframe}")
        return None
    except Exception:
        logger.warning(f"Sentiment request failed for '{country}' / {timeframe}")
        return None


# ===========================================================================
# COUNTRY RISK SCORING
# ===========================================================================

def _timeframe_confidence(article_count: int, threshold: int) -> float:
    """
    Confidence in [0, 1] based on article count relative to the expected
    typical volume for that timeframe.

    Uses 1 - exp(-count/threshold) so:
      count = 0         → confidence ≈ 0.00
      count = threshold → confidence ≈ 0.63
      count = 2×thresh  → confidence ≈ 0.86
      count → ∞         → confidence → 1.00
    """
    return round(1.0 - math.exp(-article_count / max(threshold, 1)), 4)


def _compute_country_composite(timeframe_results: dict[str, Optional[dict]]) -> dict:
    """
    Converts the 7d sentiment result for a single country into a
    composite risk score with confidence weighting.
    """
    effective_weights = {}
    for tf, cfg in TIMEFRAME_CONFIG.items():
        result = timeframe_results.get(tf)
        if result is None:
            effective_weights[tf] = 0.0
            continue
        confidence = _timeframe_confidence(result["article_count"], cfg["article_threshold"])
        effective_weights[tf] = cfg["base_weight"] * confidence

    total_weight = sum(effective_weights.values())

    if total_weight == 0:
        # No data at all for this country.
        return {
            "composite_risk_score": None,
            "timeframes": {},
            "data_available": False,
        }

    # Normalise and compute weighted average.
    composite = 0.0
    for tf, eff_w in effective_weights.items():
        result = timeframe_results.get(tf)
        if result and eff_w > 0:
            composite += result["risk_score"] * (eff_w / total_weight)

    composite = round(max(0.0, min(1.0, composite)), 4)

    # Build per-timeframe detail for the stored response.
    timeframe_detail = {}
    for tf in TIMEFRAME_CONFIG:
        result = timeframe_results.get(tf)
        if result:
            timeframe_detail[tf] = {
                "risk_score":    result["risk_score"],
                "avg_sentiment": result["avg_sentiment"],
                "article_count": result["article_count"],
                "distribution":  result["distribution"],
                "confidence":    _timeframe_confidence(
                    result["article_count"],
                    TIMEFRAME_CONFIG[tf]["article_threshold"],
                ),
                "effective_weight": round(
                    effective_weights[tf] / total_weight if total_weight > 0 else 0, 4
                ),
            }

    return {
        "composite_risk_score": composite,
        "timeframes": timeframe_detail,
        "data_available": True,
    }


# ===========================================================================
# GEO RISK ORCHESTRATION
# ===========================================================================

def _get_geopolitical_risk(geo_meta: dict, api_key: str) -> dict:
    country: str = geo_meta.get("country", "Unknown")
    timeframes = list(TIMEFRAME_CONFIG.keys())  # ["7d"]

    tf = timeframes[0]
    try:
        raw_result = _fetch_sentiment(country, tf, api_key)
    except Exception:
        raw_result = None
        logger.warning(f"Fetch failed for '{country}' / {tf}")

    timeframe_results = {tf: raw_result}
    composite = _compute_country_composite(timeframe_results)
    composite["country"] = country

    if not composite["data_available"]:
        logger.warning(f"No sentiment data obtained for {country}")
        return _neutral_geo_risk(country)

    geo_composite = composite["composite_risk_score"]
    logger.info(f"Geo risk for {country}: score={geo_composite:.4f}")

    return {
        "country":                 country,
        "geopolitical_risk_score": geo_composite,
        "geopolitical_risk_level": _risk_level(geo_composite),
        "country_scores":          [composite],
        "data_available":          True,
    }


def _get_geopolitical_risk_with_retry(geo_meta: dict) -> dict:
    """Fetches geo risk with one automatic key refresh on auth failure."""
    api_key = _get_news_api_key()
    if not api_key:
        return _neutral_geo_risk(geo_meta.get("country", "Unknown"))

    geo_risk = _get_geopolitical_risk(geo_meta, api_key)

    # If key was invalidated mid-flight by a 401, retry once with a fresh key.
    if not geo_risk["data_available"] and _NEWS_API_KEY is None:
        logger.info("Retrying geopolitical risk fetch with refreshed API key")
        api_key = _get_news_api_key()
        if api_key:
            geo_risk = _get_geopolitical_risk(geo_meta, api_key)

    return geo_risk


def _neutral_geo_risk(country: str) -> dict:
    return {
        "country":                 country,
        "geopolitical_risk_score": 0.5,
        "geopolitical_risk_level": "Elevated",
        "country_scores":          [],
        "data_available":          False,
    }


# ===========================================================================
# COUNTRY / GEO RESOLUTION  —  scheduled hubs hardcoded, dynamic via Nominatim
# ===========================================================================

def _resolve_geo_meta(hub_id: str, lat: float, lon: float) -> dict:
    if hub_id in HUB_GEO_META:
        return HUB_GEO_META[hub_id]

    country = _reverse_geocode_country(lat, lon)
    if country:
        logger.info(f"Resolved dynamic hub {hub_id} → '{country}'")
    else:
        country  = "Unknown"
        logger.warning(f"Could not resolve country for {hub_id} at ({lat},{lon})")

    return {"country": country}


def _reverse_geocode_country(lat: float, lon: float) -> Optional[str]:
    try:
        resp = requests.get(
            "https://nominatim.openstreetmap.org/reverse",
            params={  # type: ignore[arg-type]
                "lat": lat,
                "lon": lon,
                "format": "json",
                "accept-language": "en"
            },
            headers={"User-Agent": "SupplyChainRiskApp/1.0"},
            timeout=6,
        )
        if resp.status_code == 200:
            return resp.json().get("address", {}).get("country")
    except Exception:
        logger.warning(f"Nominatim reverse geocode failed for ({lat},{lon})")
    return None


# ===========================================================================
# COMBINED RISK FORMULA
# ===========================================================================

def _combine_risk_scores(weather_peak: float, geo_risk: dict) -> dict:
    """
    Combines weather ML risk with geopolitical sentiment risk.

    If geo data is unavailable the formula falls back to weather-only rather
    than inflating the score with a neutral 0.5 placeholder.
    """
    geo_available = geo_risk.get("data_available", False)
    geo_score     = geo_risk.get("geopolitical_risk_score", 0.5)

    if geo_available:
        w_total  = WEATHER_WEIGHT + GEO_WEIGHT
        combined = (WEATHER_WEIGHT * weather_peak + GEO_WEIGHT * geo_score) / w_total
    else:
        combined = weather_peak

    combined = round(max(0.0, min(1.0, combined)), 4)

    return {
        "combined_risk_score":  combined,
        "combined_risk_level":  _risk_level(combined),
        "weather_component":    round(weather_peak, 4),
        "geopolitical_component": round(geo_score, 4),
        "weather_weight":       WEATHER_WEIGHT if geo_available else 1.0,
        "geo_weight":           GEO_WEIGHT     if geo_available else 0.0,
        "geo_data_available":   geo_available,
    }


# ===========================================================================
# ML WEATHER SCORING
# ===========================================================================

def _build_vector(features: dict) -> list[float]:
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


def notify_watchlist(hub_id: str) -> None:
    try:
        region   = os.environ.get("AWS_REGION", constants.DEFAULT_REGION)
        ses      = boto3.client("ses", region_name=region)
        dynamodb = boto3.resource("dynamodb", region_name=region)
        table    = dynamodb.Table(os.environ.get("WATCHLIST_TABLE_NAME", "watchlist"))
        messages = dynamodb.Table(os.environ.get("MESSAGES_TABLE_NAME", "messages"))
        result   = table.query(
            IndexName="hub-id-index",
            KeyConditionExpression=Key("hub_id").eq(hub_id),
        )
        for item in result.get("Items", []):
            subject = f"Hub {hub_id} Alert"
            body = "Critical risk level"
            ses.send_email(
                Source="alerts@yourdomain.com",
                Destination={"ToAddresses": [item["notification_email"]]},
                Message={
                    "Subject": {"Data": subject},
                    "Body":    {"Text": {"Data": body}},
                },
            )
            messages.put_item(
                Item={
                    "user_id": item["user_id"],
                    "sent_at": f"{datetime.now(timezone.utc).isoformat()}#{uuid.uuid4().hex[:8]}",
                    "hub_id": hub_id,
                    "notification_email": item["notification_email"],
                    "subject": subject,
                    "message": body,
                }
            )
    except Exception:
        return

def store_risk_score(hub_id, score):
    try:
        region   = os.environ.get("AWS_REGION", constants.DEFAULT_REGION)
        dynamodb = boto3.resource("dynamodb", region_name=region)
        table = dynamodb.Table(os.environ.get("SCORES_TABLE_NAME", "scores"))

        table.update_item(
            Key={"hub_id": hub_id},
            UpdateExpression="SET risk_score = :s",
            ExpressionAttributeValues={":s": Decimal(str(score))}
        )
    except Exception:
        return


def _risk_level(score: float, hub_id: Optional[str] = None) -> str:
    store_risk_score(hub_id, score)
    if score < 0.20:
        return "Low"
    if score < 0.40:
        return "Elevated"
    if score < 0.60:
        return "High"
    if hub_id is not None:
        notify_watchlist(hub_id)
    return "Critical"


def _primary_driver(features: dict) -> str:
    weights = {
        "wind_gust":       0.40,
        "precip_intensity":0.30,
        "pressure":        0.13,
        "wind_speed":      0.08,
        "temperature":     0.05,
        "humidity":        0.04,
    }
    norm = {
        "wind_gust":       lambda v: min(max(v - 29, 0) / 51, 1.0),
        "precip_intensity":lambda v: min(v / 30, 1.0),
        "pressure":        lambda v: max(0, (1013 - v) / 63),
        "wind_speed":      lambda v: min(max(v - 30, 0) / 45, 1.0),
        "temperature":     lambda v: max(0, abs(v - 21.5) / 28.5),
        "humidity":        lambda v: max(0, (v - 0.90) / 0.10),
    }
    scores = {f: weights[f] * norm[f](float(features.get(f, 0))) for f in weights}
    return max(scores, key=lambda k: scores[k]).replace("_", " ").title()


def _score_day(model, day_obj: dict, hub_id: str) -> dict:
    snapshots = day_obj.get("snapshots", [])
    if not snapshots:
        raise ValueError(f"Day {day_obj.get('day', '?')} has no snapshots")

    X          = np.array([_build_vector(s["features"]) for s in snapshots], dtype=np.float32)
    raw_scores = np.clip(model.predict(X), 0.0, 1.0)

    scored_snapshots = []
    for snapshot, score in zip(snapshots, raw_scores):
        score = float(score)
        scored_snapshots.append({
            "forecast_timestamp":  snapshot["forecast_timestamp"],
            "forecast_lead_hours": int(snapshot.get("forecast_lead_hours", 0)),
            "risk_score":          round(score, 4),
            "risk_level":          _risk_level(score, hub_id),
            "primary_driver":      _primary_driver(snapshot["features"]),
        })

    all_scores = [s["risk_score"] for s in scored_snapshots]
    peak_score = max(all_scores)
    worst      = scored_snapshots[all_scores.index(peak_score)]

    return {
        "date":            day_obj["date"],
        "day":             day_obj["day"],
        "peak_risk_score": round(peak_score, 4),
        "mean_risk_score": round(sum(all_scores) / len(all_scores), 4),
        "risk_level":      _risk_level(peak_score),
        "primary_driver":  worst["primary_driver"],
        "worst_interval":  worst["forecast_timestamp"],
        "snapshots":       scored_snapshots,
    }


# ===========================================================================
# ADAGE RESPONSE BUILDER
# ===========================================================================

def _build_adage_response(
    body: dict,
    scored_days: list[dict],
    geo_risk: dict,
    combined: dict,
) -> dict:
    now      = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    hub_id   = body["hub_id"]
    hub_name = body.get("hub_name", "")
    version  = os.environ.get("MODEL_VERSION", "rf_v1.0")

    events = []

    # One event per forecast day
    for d in scored_days:
        day_combined = _combine_risk_scores(d["peak_risk_score"], geo_risk)
        events.append({
            "time_object": {
                "timestamp":     f"{d['date']}T00:00:00",
                "duration":      1,
                "duration_unit": "day",
                "timezone":      "UTC",
            },
            "event_type": "daily_risk_assessment",
            "attribute": {
                "hub_id":          hub_id,
                "hub_name":        hub_name,
                "day":             d["day"],
                "date":            d["date"],
                "peak_risk_score": d["peak_risk_score"],
                "mean_risk_score": d["mean_risk_score"],
                "risk_level":      d["risk_level"],

                "combined_risk_score":      day_combined["combined_risk_score"],
                "combined_risk_level":      day_combined["combined_risk_level"],
                "weather_component":        day_combined["weather_component"],
                "geopolitical_component":   day_combined["geopolitical_component"],
                "weather_weight":           day_combined["weather_weight"],
                "geo_weight":               day_combined["geo_weight"],

                "primary_driver":  d["primary_driver"],
                "worst_interval":  d["worst_interval"],
                "snapshots":       d["snapshots"],
                "model_version":   version,
            },
        })

    # Seven-day outlook with combined risk fields added.
    peak_day      = max(scored_days, key=lambda d: d["peak_risk_score"])
    weather_peak  = round(max(d["peak_risk_score"] for d in scored_days), 4)

    events.append({
        "time_object": {
            "timestamp":     body.get("forecast_origin", now),
            "duration":      len(scored_days),
            "duration_unit": "day",
            "timezone":      "UTC",
        },
        "event_type": "seven_day_outlook",
        "attribute": {
            "hub_id":                   hub_id,
            "hub_name":                 hub_name,
            "lat":                      body.get("lat"),
            "lon":                      body.get("lon"),
            # Weather-only score preserved for backward compatibility.
            "outlook_risk_score":       weather_peak,
            "outlook_risk_level":       _risk_level(weather_peak),
            # Combined score.
            "combined_risk_score":      combined["combined_risk_score"],
            "combined_risk_level":      combined["combined_risk_level"],
            "weather_component":        combined["weather_component"],
            "geopolitical_component":   combined["geopolitical_component"],
            "weather_weight":           combined["weather_weight"],
            "geo_weight":               combined["geo_weight"],
            "peak_day":                 peak_day["date"],
            "peak_day_number":          peak_day["day"],
            "forecast_origin":          body.get("forecast_origin"),
            "days_assessed":            len(scored_days),
            "model_version":            version,
        },
    })

    # Geopolitical risk event — carries all the detail the frontend needs.
    events.append({
        "time_object": {
            "timestamp":     now,
            "duration":      7,
            "duration_unit": "day",
            "timezone":      "UTC",
        },
        "event_type": "geopolitical_risk_assessment",
        "attribute": {
            "hub_id":                          hub_id,
            "hub_name":                        hub_name,
            "country":                         geo_risk["country"],
            "geopolitical_risk_score":         geo_risk["geopolitical_risk_score"],
            "geopolitical_risk_level":         geo_risk["geopolitical_risk_level"],
            "data_available":                  geo_risk["data_available"],
            "country_scores":                  geo_risk.get("country_scores", []),
        },
    })

    return {
        "data_source":   "Pirate Weather API (GFS) + News Sentiment API",
        "dataset_type":  "Supply Chain Disruption Risk Assessment",
        "dataset_id":    (
            f"s3://{os.environ.get('DATA_BUCKET', 'supply-chain-data')}"
            f"/risk/weather/{hub_id}/latest.json"
        ),
        "time_object":   {"timestamp": now, "timezone": "UTC"},
        "events":        events,
    }


# ===========================================================================
# CORE COMPUTATION
# ===========================================================================

def _compute_and_store_risk(s3, bucket: str, hub_id: str, processed: dict) -> dict:
    days = processed.get("days", [])
    if not days:
        raise ValueError(f"No forecast days available for hub {hub_id}")
    if len(days) != 7:
        logger.warning(f"Expected 7 day objects, received {len(days)}")

    model        = _load_model()
    scored_days  = [_score_day(model, d, hub_id) for d in days]
    log_metric(constants.RISK_CALCULATIONS, 1, constants.RISK_SERVICE)
    weather_peak = max(d["peak_risk_score"] for d in scored_days)

    lat      = processed.get("lat", 0.0)
    lon      = processed.get("lon", 0.0)
    geo_meta = _resolve_geo_meta(hub_id, lat, lon)
    geo_risk = _get_geopolitical_risk_with_retry(geo_meta)
    combined = _combine_risk_scores(weather_peak, geo_risk)

    logger.info(
        f"Final risk for {hub_id}: weather={weather_peak:.4f} "
        f"geo={geo_risk['geopolitical_risk_score']:.4f} "
        f"combined={combined['combined_risk_score']:.4f}"
    )

    if combined["combined_risk_level"] == "Critical":
        notify_watchlist(hub_id)

    adage_response = _build_adage_response(processed, scored_days, geo_risk, combined)

    s3.put_object(
        Bucket=bucket,
        Key=f"risk/weather/{hub_id}/latest.json",
        Body=json.dumps(adage_response),
        ContentType="application/json",
    )
    logger.info(f"Stored combined risk to risk/weather/{hub_id}/latest.json")
    return adage_response


# ===========================================================================
# HUB ID VALIDATION  /  PROCESSED DATA FETCHING
# ===========================================================================

def validate_hub_id(base_url: str, hub_id: str) -> bool:
    resp = requests.get(f"{base_url}/{constants.LOCATION_PATH}/{hub_id}", timeout=10)
    return resp.status_code == constants.STATUS_OK


def _fetch_processed_data(hub_id: str, date: str) -> dict:
    base_url = os.environ["API_BASE_URL"]
    url      = f"{base_url}/{constants.RETRIEVE_PROCESSED_WEATHER_PATH}/{hub_id}"
    logger.info(f"Fetching processed weather for hub_id={hub_id}, date={date}")
    resp = requests.get(url, params={"date": date}, timeout=10)
    if resp.status_code == constants.STATUS_NOT_FOUND:
        raise LookupError(f"Processed weather data not found for hub {hub_id} on {date}")
    if resp.status_code != constants.STATUS_OK:
        raise RuntimeError(f"Retrieval service returned {resp.status_code}: {resp.text}")
    return resp.json()


# ===========================================================================
# EVENT HANDLERS
# ===========================================================================

def _is_s3_event(event: dict) -> bool:
    try:
        return event["Records"][0]["eventSource"] == "aws:s3"
    except (KeyError, IndexError, TypeError):
        return False


def _handle_s3_event(event: dict) -> list:
    s3      = boto3.client("s3")
    results = []

    for record in event.get("Records", []):
        try:
            bucket = record["s3"]["bucket"]["name"]
            key    = record["s3"]["object"]["key"]
            logger.info(f"Analytics S3 trigger: key={key}")

            parts = key.split("/")
            if len(parts) < 4 or parts[0] != "processed" or parts[1] != "weather":
                results.append({"status": "ignored", "key": key})
                continue

            hub_id   = parts[2]
            if not validate_hub_id(os.environ["API_BASE_URL"], hub_id):
                logger.error(f"S3 event for invalid hub_id: {hub_id}")
                results.append({"status": "ignored", "reason": "invalid hub_id", "key": key})
                continue

            date_str  = parts[3].replace(".json", "")
            processed = _fetch_processed_data(hub_id, date_str)
            _compute_and_store_risk(s3, bucket, hub_id, processed)
            results.append({"status": "scored", "hub_id": hub_id, "key": key})

        except Exception as e:
            logger.exception(f"Error processing S3 record: {e}")
            results.append({
                "status": "error",
                "error":  str(e),
                "key":    record.get("s3", {}).get("object", {}).get("key", "unknown"),
            })

    return results


def _handle_api_event(event: dict) -> dict:
    s3          = boto3.client("s3")
    bucket      = os.environ["DATA_BUCKET"]
    path_params = event.get("pathParameters") or {}
    hub_id      = path_params.get("hub_id")

    if not hub_id:
        return response(constants.STATUS_BAD_REQUEST, {"error": "Missing hub_id"})

    if not validate_hub_id(os.environ["API_BASE_URL"], hub_id):
        return response(constants.STATUS_BAD_REQUEST, {"error": "Invalid hub_id"})

    date = datetime.now(timezone.utc).strftime(constants.DATE_FORMAT)

    try:
        cached = s3.get_object(Bucket=bucket, Key=f"risk/weather/{hub_id}/latest.json")
        adage_response = json.loads(cached["Body"].read())
        logger.info(f"Returning cached risk scores for hub {hub_id}")
        return response(constants.STATUS_OK, adage_response)
    except s3.exceptions.NoSuchKey:
        logger.info(f"No cached risk for {hub_id}, computing on demand")

    processed      = _fetch_processed_data(hub_id, date)
    adage_response = _compute_and_store_risk(s3, bucket, hub_id, processed)
    return response(constants.STATUS_OK, adage_response)


# ===========================================================================
# LAMBDA ENTRY POINT
# ===========================================================================

def lambda_handler(event, context):
    try:
        if not os.environ.get("DATA_BUCKET"):
            return response(
                constants.STATUS_INTERNAL_SERVER_ERROR,
                {"error": "Missing DATA_BUCKET configuration"},
            )
        if not os.environ.get("API_BASE_URL"):
            return response(
                constants.STATUS_INTERNAL_SERVER_ERROR,
                {"error": "Missing API_BASE_URL configuration"},
            )

        if _is_s3_event(event):
            logger.info(f"Analytics triggered by S3 event: {event}")
            return _handle_s3_event(event)

        logger.info(f"Analytics triggered by API request: {event}")
        log_metric(constants.DATA_REQUESTS, 1, constants.RISK_SERVICE)
        return _handle_api_event(event)

    except ValueError as e:
        logger.exception(str(e))
        return response(constants.STATUS_BAD_REQUEST, {"error": str(e)})
    except LookupError as e:
        logger.exception(str(e))
        return response(constants.STATUS_NOT_FOUND, {"error": str(e)})
    except RuntimeError as e:
        logger.exception(str(e))
        return response(constants.STATUS_BAD_GATEWAY, {"error": str(e)})
    except Exception as e:
        logger.exception(f"Unhandled error: {str(e)}")
        return response(constants.STATUS_INTERNAL_SERVER_ERROR, {"error": str(e)})


def response(status: int, body: dict) -> dict:
    return {
        "statusCode": status,
        "headers": {
            "Content-Type":                "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps(body),
    }
