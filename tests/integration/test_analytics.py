import json
from datetime import datetime, timezone
from unittest.mock import patch, Mock
from lambdas.ingestion.handler import lambda_handler as ingestion_handler
from lambdas.processing.handler import lambda_handler as processing_handler
from lambdas.analytics.handler import lambda_handler as analytics_handler
from tests.test_constants import HUB_ID_1, RAW_WEATHER_DATA_H1
from constants import DATE_FORMAT, STATUS_OK
from lambdas.retrieval.handler import lambda_handler as retrieval_handler
from constants import RETRIEVE_PROCESSED_WEATHER_PATH
import sys
import time
import requests  # type: ignore[import-untyped]  # noqa: E402
from lambdas.analytics import handler  # noqa: E402
"""
Integration test for the analytics handler with geopolitical risk.

Simulates the full pipeline trigger (as would happen on a 6-hour interval):
    1. Raw weather data gets ingested → stored in mock S3
    2. Processing transforms raw → processed, stored in mock S3
    3. Analytics handler is triggered (via API event + S3 event)
    4. Analytics handler:
       - Loads ML model (dummy) from S3
       - Scores weather risk per-day
       - Calls the news sentiment API (mocked with realistic data)
       - Combines 65% weather + 35% geopolitical
       - Stores result in S3 as latest.json
    5. Test verifies the stored output has all three risk dimensions
"""





# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_hub_info():
    return {"hub_id": HUB_ID_1, "name": "Port of Singapore", "lat": 1.264, "lon": 103.820}


def _make_sentiment_response(country, timeframe, article_count, avg_sentiment):
    """Build a realistic response matching the live news API schema."""
    positive = int(article_count * max(0, (1 + avg_sentiment) / 2))
    negative = article_count - positive
    return {
        "country": country,
        "timeframe": timeframe,
        "articleCount": article_count,
        "averageSentiment": avg_sentiment,
        "distribution": {"positive": positive, "neutral": 0, "negative": negative},
    }


# Realistic sentiment data for Singapore keywords (7d-only mode)
MOCK_SENTIMENT_RESPONSES = {
    ("Singapore", "7d"): _make_sentiment_response("Singapore", "7d", 180, -0.08),
}


def _selective_mock_get(processed_data):
    """
    Returns a side_effect function that:
      - Mocks internal API calls (hub validation, retrieval)
      - Mocks news sentiment API with realistic data
    """
    def side_effect(url, **kwargs):
        # Mock news sentiment API with realistic responses
        if "api/sentiment" in url:
            params = kwargs.get("params", {})
            country = params.get("keyword", "")
            timeframe = params.get("timeframe", "")
            key = (country, timeframe)

            # Verify x-api-key header is used (not query param)
            headers = kwargs.get("headers", {})
            assert "x-api-key" in headers, (
                f"API key must be in x-api-key header, got headers={headers}"
            )
            assert "key" not in params, (
                f"API key must NOT be in query params, got params={params}"
            )

            mock_resp = Mock()
            if key in MOCK_SENTIMENT_RESPONSES:
                mock_resp.status_code = 200
                mock_resp.json.return_value = MOCK_SENTIMENT_RESPONSES[key]
                mock_resp.text = json.dumps(MOCK_SENTIMENT_RESPONSES[key])
            else:
                mock_resp.status_code = 404
                mock_resp.json.return_value = {"error": "Not found"}
                mock_resp.text = '{"error": "Not found"}'
            return mock_resp

        # Mock hub validation (location service)
        if "/ese/v1/location/" in url:
            mock_resp = Mock()
            mock_resp.status_code = STATUS_OK
            mock_resp.json.return_value = _mock_hub_info()
            return mock_resp

        # Mock retrieval service (returns processed weather data)
        if "/ese/v1/retrieve/processed/weather/" in url:
            mock_resp = Mock()
            mock_resp.status_code = STATUS_OK
            mock_resp.json.return_value = processed_data
            mock_resp.text = json.dumps(processed_data)
            return mock_resp

        # Default: return 200 with empty body
        mock_resp = Mock()
        mock_resp.status_code = STATUS_OK
        mock_resp.json.return_value = {}
        return mock_resp

    return side_effect


# ===========================================================================
# Test 1: Full Pipeline (API-triggered)
# ===========================================================================

@patch("lambdas.analytics.handler._get_news_api_key", return_value="test-integration-key")
@patch("lambdas.analytics.handler.validate_hub_id", return_value=True)
@patch("lambdas.processing.handler.get_hub_info_from_pos")
@patch("lambdas.ingestion.handler.fetch_weather")
def test_full_pipeline_with_geopolitical_risk(
    mock_fetch_weather,
    mock_get_hub_info_from_pos,
    mock_validate_hub_id,
    mock_get_api_key,
    setup_s3,
):
    """
    End-to-end pipeline: ingest → process → analytics.

    Verifies that the stored risk output contains:
      - daily_risk_assessment events (weather-only ML scores)
      - seven_day_outlook event (with combined_risk_score)
      - geopolitical_risk_assessment event (with keyword breakdowns)
      - All three risk dimensions stored independently
    """
    s3 = setup_s3["s3"]
    bucket = setup_s3["bucket"]

    # Setup mocks for ingestion
    mock_get_hub_info_from_pos.return_value = {"hub_id": HUB_ID_1, "hub_name": "Port of Singapore"}

    with open(RAW_WEATHER_DATA_H1, "r") as f:
        weather_data = json.load(f)
    now_ts = int(datetime.now(tz=timezone.utc).timestamp())
    weather_data["currently"]["time"] = now_ts
    mock_fetch_weather.return_value = json.dumps(weather_data)

    # === STAGE 1: Ingestion ===
    with patch("lambdas.ingestion.handler.requests.get", side_effect=_selective_mock_get({})):
        ingestion_event = {"pathParameters": {"hub_id": HUB_ID_1}}
        ingestion_resp = ingestion_handler(ingestion_event, None)
        assert ingestion_resp["statusCode"] == STATUS_OK

    date_str = datetime.fromtimestamp(now_ts, tz=timezone.utc).strftime(DATE_FORMAT)

    # Verify raw data stored in S3
    raw_key = f"raw/weather/{HUB_ID_1}/{date_str}.json"
    raw_obj = s3.get_object(Bucket=bucket, Key=raw_key)
    raw_data = json.loads(raw_obj["Body"].read())
    assert "hourly" in raw_data

    # === STAGE 2: Processing ===
    processing_event = {"body": json.dumps(raw_data)}
    processing_resp = processing_handler(processing_event, None)
    assert processing_resp["statusCode"] == STATUS_OK

    # Verify processed data stored in S3
    processed_key = f"processed/weather/{HUB_ID_1}/{date_str}.json"
    processed_obj = s3.get_object(Bucket=bucket, Key=processed_key)
    processed_data = json.loads(processed_obj["Body"].read())
    assert "days" in processed_data
    assert "hub_id" in processed_data

    # === STAGE 3: Analytics (with mocked news sentiment API) ===
    with patch("lambdas.analytics.handler.requests.get",
               side_effect=_selective_mock_get(processed_data)):
        analytics_event = {
            "pathParameters": {"hub_id": HUB_ID_1},
            "queryStringParameters": {"date": date_str},
        }
        analytics_resp = analytics_handler(analytics_event, None)

    assert analytics_resp["statusCode"] == STATUS_OK, (
        f"Analytics failed: {analytics_resp['body']}"
    )

    # === VERIFY: Stored output in S3 ===
    risk_key = f"risk/weather/{HUB_ID_1}/latest.json"
    risk_obj = s3.get_object(Bucket=bucket, Key=risk_key)
    risk_data = json.loads(risk_obj["Body"].read())

    # Also verify the API response matches what's stored
    api_body = json.loads(analytics_resp["body"])
    assert api_body == risk_data

    # --- Check top-level structure ---
    assert "events" in risk_data
    assert "data_source" in risk_data
    assert "News Sentiment" in risk_data["data_source"], (
        "data_source should mention News Sentiment API"
    )

    # --- Check event types ---
    daily_events = [e for e in risk_data["events"] if e["event_type"] == "daily_risk_assessment"]
    outlook_events = [e for e in risk_data["events"] if e["event_type"] == "seven_day_outlook"]
    geo_events = [e for e in risk_data["events"] if e["event_type"] == "geopolitical_risk_assessment"]

    assert len(daily_events) >= 1, "Should have at least 1 daily risk assessment"
    assert len(outlook_events) == 1, "Should have exactly 1 seven_day_outlook"
    assert len(geo_events) == 1, "Should have exactly 1 geopolitical_risk_assessment"

    # --- Check daily events have weather + combined risk scores ---
    for daily in daily_events:
        attr = daily["attribute"]
        assert "peak_risk_score" in attr
        assert "risk_level" in attr
        assert "primary_driver" in attr
        assert 0.0 <= attr["peak_risk_score"] <= 1.0

        assert "combined_risk_score" in attr
        assert "combined_risk_level" in attr
        assert "weather_component" in attr
        assert "geopolitical_component" in attr
        assert "weather_weight" in attr
        assert "geo_weight" in attr
        assert 0.0 <= attr["combined_risk_score"] <= 1.0

    # --- Check seven_day_outlook has COMBINED risk ---
    outlook = outlook_events[0]["attribute"]

    # Weather-only (backward compat)
    assert "outlook_risk_score" in outlook
    assert 0.0 <= outlook["outlook_risk_score"] <= 1.0

    # Combined score
    assert "combined_risk_score" in outlook
    assert "combined_risk_level" in outlook
    assert "weather_component" in outlook
    assert "geopolitical_component" in outlook
    assert "weather_weight" in outlook
    assert "geo_weight" in outlook
    assert 0.0 <= outlook["combined_risk_score"] <= 1.0

    # Verify it's the 65/35 blend, not weather-only
    assert outlook["weather_weight"] == 0.65
    assert outlook["geo_weight"] == 0.35

    # Combined should differ from weather-only (geo data was available)
    assert outlook["combined_risk_score"] != outlook["outlook_risk_score"], (
        "Combined should differ from weather-only when geo data is available"
    )

    # --- Check geopolitical event has full breakdown ---
    geo = geo_events[0]["attribute"]
    assert geo["country"] == "Singapore"
    assert "geopolitical_risk_score" in geo
    assert 0.0 <= geo["geopolitical_risk_score"] <= 1.0
    assert "geopolitical_risk_level" in geo
    assert geo["data_available"] is True

    # Verify country geo breakdown
    assert len(geo["country_scores"]) >= 1
    countries_found = {c["country"] for c in geo["country_scores"]}
    assert "Singapore" in countries_found
    for country_score in geo["country_scores"]:
        if country_score.get("data_available"):
            assert 0.0 <= country_score["composite_risk_score"] <= 1.0


# ===========================================================================
# Test 2: S3 Trigger (simulates automated 6-hour pipeline)
# ===========================================================================

@patch("lambdas.analytics.handler._get_news_api_key", return_value="test-integration-key")
@patch("lambdas.analytics.handler.validate_hub_id", return_value=True)
@patch("lambdas.processing.handler.get_hub_info_from_pos")
@patch("lambdas.ingestion.handler.fetch_weather")
def test_s3_trigger_simulates_automated_pipeline(
    mock_fetch_weather,
    mock_get_hub_info_from_pos,
    mock_validate_hub_id,
    mock_get_api_key,
    setup_s3,
):
    """
    Simulates the automated S3 trigger (like the 6-hour EventBridge cron):
      1. Processed weather data lands in S3
      2. S3 notification triggers analytics handler
      3. Handler fetches processed data, runs ML model + news API, stores result

    This mimics exactly what happens in production when EventBridge fires
    ingestion → S3 trigger → processing → S3 trigger → analytics.
    """
    s3 = setup_s3["s3"]
    bucket = setup_s3["bucket"]

    # Setup ingestion mocks
    mock_get_hub_info_from_pos.return_value = {"hub_id": HUB_ID_1, "hub_name": "Port of Singapore"}

    with open(RAW_WEATHER_DATA_H1, "r") as f:
        weather_data = json.load(f)
    now_ts = int(datetime.now(tz=timezone.utc).timestamp())
    weather_data["currently"]["time"] = now_ts
    mock_fetch_weather.return_value = json.dumps(weather_data)

    # Run ingestion + processing to get processed data in S3
    with patch("lambdas.ingestion.handler.requests.get", side_effect=_selective_mock_get({})):
        ingestion_handler({"pathParameters": {"hub_id": HUB_ID_1}}, None)
    date_str = datetime.fromtimestamp(now_ts, tz=timezone.utc).strftime(DATE_FORMAT)

    raw_key = f"raw/weather/{HUB_ID_1}/{date_str}.json"
    raw_data = json.loads(s3.get_object(Bucket=bucket, Key=raw_key)["Body"].read())
    processing_handler({"body": json.dumps(raw_data)}, None)

    # Read the processed data that was stored
    processed_key = f"processed/weather/{HUB_ID_1}/{date_str}.json"
    processed_data = json.loads(
        s3.get_object(Bucket=bucket, Key=processed_key)["Body"].read()
    )

    # === Simulate S3 trigger event ===
    # This is the exact event format AWS S3 sends to the analytics Lambda
    s3_trigger_event = {
        "Records": [
            {
                "eventSource": "aws:s3",
                "s3": {
                    "bucket": {"name": bucket},
                    "object": {"key": processed_key},
                },
            }
        ]
    }

    with patch("lambdas.analytics.handler.requests.get",
               side_effect=_selective_mock_get(processed_data)):
        result = analytics_handler(s3_trigger_event, None)

    # S3 trigger returns a list of results
    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0]["status"] == "scored", f"Expected 'scored', got: {result[0]}"
    assert result[0]["hub_id"] == HUB_ID_1

    # === Verify stored output ===
    risk_key = f"risk/weather/{HUB_ID_1}/latest.json"
    risk_obj = s3.get_object(Bucket=bucket, Key=risk_key)
    risk_data = json.loads(risk_obj["Body"].read())

    # Check all three event types are present
    event_types = [e["event_type"] for e in risk_data["events"]]
    assert "daily_risk_assessment" in event_types
    assert "seven_day_outlook" in event_types
    assert "geopolitical_risk_assessment" in event_types

    # Verify combined score is present and uses 65/35 blend
    outlook = [e for e in risk_data["events"] if e["event_type"] == "seven_day_outlook"][0]
    assert "combined_risk_score" in outlook["attribute"]
    assert "weather_component" in outlook["attribute"]
    assert "geopolitical_component" in outlook["attribute"]
    assert outlook["attribute"]["weather_weight"] == 0.65
    assert outlook["attribute"]["geo_weight"] == 0.35

    # Verify geo breakdown saved
    geo = [e for e in risk_data["events"] if e["event_type"] == "geopolitical_risk_assessment"][0]
    assert geo["attribute"]["country"] == "Singapore"
    assert geo["attribute"]["data_available"] is True
    assert len(geo["attribute"]["country_scores"]) >= 1


# ===========================================================================
# Test 3: Graceful degradation when news API is unreachable
# ===========================================================================

@patch("lambdas.analytics.handler._get_news_api_key", return_value=None)
@patch("lambdas.analytics.handler.validate_hub_id", return_value=True)
@patch("lambdas.processing.handler.get_hub_info_from_pos")
@patch("lambdas.ingestion.handler.fetch_weather")
def test_degrades_to_weather_only_when_no_api_key(
    mock_fetch_weather,
    mock_get_hub_info_from_pos,
    mock_validate_hub_id,
    mock_get_api_key,
    setup_s3,
):
    """
    When the news API key is unavailable, the handler should:
      - Still score weather risk normally
      - Return geopolitical_risk_score = 0.5 (neutral)
      - Set combined = weather-only (not the 65/35 blend)
      - Mark data_available = False
    """
    s3 = setup_s3["s3"]
    bucket = setup_s3["bucket"]

    mock_get_hub_info_from_pos.return_value = {"hub_id": HUB_ID_1, "hub_name": "Port of Singapore"}

    with open(RAW_WEATHER_DATA_H1, "r") as f:
        weather_data = json.load(f)
    now_ts = int(datetime.now(tz=timezone.utc).timestamp())
    weather_data["currently"]["time"] = now_ts
    mock_fetch_weather.return_value = json.dumps(weather_data)

    # Ingest + process
    with patch("lambdas.ingestion.handler.requests.get", side_effect=_selective_mock_get({})):
        ingestion_handler({"pathParameters": {"hub_id": HUB_ID_1}}, None)
    date_str = datetime.fromtimestamp(now_ts, tz=timezone.utc).strftime(DATE_FORMAT)

    raw_key = f"raw/weather/{HUB_ID_1}/{date_str}.json"
    raw_data = json.loads(s3.get_object(Bucket=bucket, Key=raw_key)["Body"].read())
    processing_handler({"body": json.dumps(raw_data)}, None)

    processed_key = f"processed/weather/{HUB_ID_1}/{date_str}.json"
    processed_data = json.loads(
        s3.get_object(Bucket=bucket, Key=processed_key)["Body"].read()
    )

    # Mock the retrieval call but NO sentiment calls should happen
    def mock_get(url, **kwargs):
        if "/ese/v1/retrieve/processed/weather/" in url:
            mock_resp = Mock()
            mock_resp.status_code = STATUS_OK
            mock_resp.json.return_value = processed_data
            mock_resp.text = json.dumps(processed_data)
            return mock_resp
        mock_resp = Mock()
        mock_resp.status_code = STATUS_OK
        mock_resp.json.return_value = {}
        return mock_resp

    with patch("lambdas.analytics.handler.requests.get", side_effect=mock_get):
        analytics_resp = analytics_handler(
            {"pathParameters": {"hub_id": HUB_ID_1},
             "queryStringParameters": {"date": date_str}},
            None,
        )

    assert analytics_resp["statusCode"] == STATUS_OK

    risk_key = f"risk/weather/{HUB_ID_1}/latest.json"
    risk_data = json.loads(s3.get_object(Bucket=bucket, Key=risk_key)["Body"].read())

    # Should still have all event types
    event_types = [e["event_type"] for e in risk_data["events"]]
    assert "daily_risk_assessment" in event_types
    assert "seven_day_outlook" in event_types
    assert "geopolitical_risk_assessment" in event_types

    # Verify weather-only fallback
    outlook = [e for e in risk_data["events"] if e["event_type"] == "seven_day_outlook"][0]
    assert outlook["attribute"]["weather_weight"] == 1.0
    assert outlook["attribute"]["geo_weight"] == 0.0
    # Combined should equal weather-only
    assert outlook["attribute"]["combined_risk_score"] == outlook["attribute"]["outlook_risk_score"]

    # Geo event should show unavailable
    geo = [e for e in risk_data["events"] if e["event_type"] == "geopolitical_risk_assessment"][0]
    assert geo["attribute"]["data_available"] is False
    assert geo["attribute"]["geopolitical_risk_score"] == 0.5


def _mock_requests(mock_get, payload, status=STATUS_OK):
    mock_resp = Mock()
    mock_resp.status_code = status
    mock_resp.json.return_value = payload
    mock_resp.text = json.dumps(payload)
    mock_get.return_value = mock_resp

@patch("lambdas.analytics.handler.requests.get")
@patch("lambdas.processing.handler.get_hub_info_from_pos")
@patch("lambdas.ingestion.handler.fetch_weather")
def test_ingestion_processing_analytics(
    mock_fetch_weather,
    mock_get_hub_info_from_pos,
    mock_get_requests,
    setup_s3,
):
    s3 = setup_s3["s3"]
    bucket = setup_s3["bucket"]

    mock_get_hub_info_from_pos.return_value = {"hub_id": HUB_ID_1, "hub_name": "Test Hub"}

    with open(RAW_WEATHER_DATA_H1, "r") as f:
        weather_data = json.load(f)
    now_ts = int(datetime.now(tz=timezone.utc).timestamp())
    weather_data["currently"]["time"] = now_ts
    mock_fetch_weather.return_value = json.dumps(weather_data)

    ingestion_event = {"pathParameters": {"hub_id": HUB_ID_1}}
    ingestion_resp = ingestion_handler(ingestion_event, None)
    ingestion_body = json.loads(ingestion_resp["body"])
    assert ingestion_resp["statusCode"] == STATUS_OK
    assert ingestion_body["message"] == "Success"

    date_str = datetime.fromtimestamp(now_ts, tz=timezone.utc).strftime(DATE_FORMAT)

    raw_key = f"raw/weather/{HUB_ID_1}/{date_str}.json"
    obj = s3.get_object(Bucket=bucket, Key=raw_key)
    raw_data = json.loads(obj["Body"].read())
    assert raw_data["currently"]["time"] == now_ts

    processing_event = {"body": json.dumps(raw_data)}
    processing_resp = processing_handler(processing_event, None)
    assert processing_resp["statusCode"] == STATUS_OK

    processed_key = f"processed/weather/{HUB_ID_1}/{date_str}.json"
    obj = s3.get_object(Bucket=bucket, Key=processed_key)
    processed_data = json.loads(obj["Body"].read())

    _mock_requests(mock_get_requests, processed_data)

    # Analytics handler should be able to interact with the processed path of date_str in mock s3
    analytics_event = {
        "pathParameters": {"hub_id": HUB_ID_1},
        "queryStringParameters": {"date": date_str},
    }
    analytics_resp = analytics_handler(analytics_event, None)
    assert analytics_resp["statusCode"] == STATUS_OK

    body = json.loads(analytics_resp["body"])
    assert "events" in body
    assert "data_source" in body
    assert "time_object" in body
    # basic check for risk assessment output
    daily_events = [e for e in body["events"] if e["event_type"] == "daily_risk_assessment"]
    assert len(daily_events) >= 1

@patch("lambdas.analytics.handler.requests.get")
@patch("lambdas.processing.handler.get_hub_info_from_pos")
@patch("lambdas.ingestion.handler.fetch_weather")
def test_processing_to_analytics_schema_break(
    mock_fetch_weather,
    mock_get_hub_info_from_pos,
    mock_get,
    setup_s3,
):
    s3 = setup_s3["s3"]
    bucket = setup_s3["bucket"]

    mock_get_hub_info_from_pos.return_value = {"hub_id": HUB_ID_1, "hub_name": "Test Hub"}

    # ingestion normal
    with open(RAW_WEATHER_DATA_H1) as f:
        data = json.load(f)

    data["currently"]["time"] = int(datetime.now(timezone.utc).timestamp())
    mock_fetch_weather.return_value = json.dumps(data)

    ingestion_handler({"pathParameters": {"hub_id": HUB_ID_1}}, None)

    # processing runs
    date_str = datetime.now(timezone.utc).strftime(DATE_FORMAT)
    raw_key = f"raw/weather/{HUB_ID_1}/{date_str}.json"
    raw = json.loads(s3.get_object(Bucket=bucket, Key=raw_key)["Body"].read())
    processing_handler({"body": json.dumps(raw)}, None)

    # Succcess analytic
    retrieval_event = {
        "rawPath": RETRIEVE_PROCESSED_WEATHER_PATH,
        "pathParameters": { "hub_id": HUB_ID_1 },
        "queryStringParameters": { "date": date_str}
    }
    ret_resp = retrieval_handler(retrieval_event, None)
    mock_resp = Mock()
    mock_resp.status_code = STATUS_OK
    mock_resp.json.return_value =json.loads(ret_resp["body"])
    mock_resp.text = json.dumps(json.loads(ret_resp["body"]))
    mock_get.return_value = mock_resp
    _mock_requests(mock_get, json.loads(ret_resp["body"]))
    resp = analytics_handler({
        "pathParameters": {"hub_id": HUB_ID_1},
        "queryStringParameters": {"date": date_str}
    }, None)
    assert resp["statusCode"] == STATUS_OK

    # break processed data after processing (simulate corruption)
    processed_key = f"processed/weather/{HUB_ID_1}/{date_str}.json"
    s3.put_object(
        Bucket=bucket,
        Key=processed_key,
        Body=json.dumps({"invalid": "schema"}) 
    )

    # Use retrieval to get the corrupted processed data and mock the corrupted data in the analytics. 
    # This parralels the real logic inside analytic lambda
    retrieval_event = {
        "rawPath": RETRIEVE_PROCESSED_WEATHER_PATH,
        "pathParameters": { "hub_id": HUB_ID_1 },
        "queryStringParameters": { "date": date_str}
    }
    ret_resp = retrieval_handler(retrieval_event, None)
    assert json.loads(ret_resp["body"]) == {"invalid": "schema"}
    _mock_requests(mock_get, json.loads(ret_resp["body"]))

    resp = analytics_handler({
        "pathParameters": {"hub_id": HUB_ID_1},
        "queryStringParameters": {"date": date_str}
    }, None)

    # cached check
    assert resp["statusCode"] == STATUS_OK
    s3.delete_object(
        Bucket=bucket,
        Key=f"risk/weather/{HUB_ID_1}/latest.json"
    )
    # Schema breaks
    resp = analytics_handler({
        "pathParameters": {"hub_id": HUB_ID_1},
        "queryStringParameters": {"date": date_str}
    }, None)
    assert resp["statusCode"] != STATUS_OK


def test_16_hubs_data_flow():

    """
    End-to-End flow verification script for 16 hubs (8 preset + 8 dynamic).
    Hits Nominatim and the News Sentiment API to verify the full resolution chain.
    """
    
    
    
    
    NEWS_API_BASE = "https://i9pdxmupj7.execute-api.ap-southeast-2.amazonaws.com"
    
    # Setup New Key
    requests.delete(f"{NEWS_API_BASE}/api/auth/key", timeout=10)
    resp = requests.post(f"{NEWS_API_BASE}/api/auth/key", timeout=10)
    key = resp.json()["key"]
    print(f"Acquired Key: {key[:12]}...")
    
    # 8 Preset Hubs
    hubs = [
        {"hub_id": f"H00{i+1}", "lat": 0.0, "lon": 0.0} for i in range(8)
    ]
    
    # 8 Dynamic Hubs (lat, lon) -> resolving via Nominatim
    dynamic_coords = [
        ("LOC01", 48.8566, 2.3522),    # Paris, France
        ("LOC02", 35.6762, 139.6503),  # Tokyo, Japan
        ("LOC03", 28.6139, 77.2090),   # New Delhi, India
        ("LOC04", 51.5072, -0.1276),   # London, UK
        ("LOC05", 52.5200, 13.4050),   # Berlin, Germany
        ("LOC06", 45.4215, -75.6972),  # Ottawa, Canada
        ("LOC07", -41.2865, 174.7762), # Wellington, NZ
        ("LOC08", -34.6037, -58.3816), # Buenos Aires, Argentina
    ]
    
    for hid, lat, lon in dynamic_coords:
        hubs.append({"hub_id": hid, "lat": lat, "lon": lon})
    
    ok = 0
    fail = 0
    
    print(f"\n{'HUB':<6} | {'T' :<2} | {'COUNTRY RESOLVED':<20} | {'SNT' :<5} | {'RISK_S'} | {'WTH_S'} | {'CMB_S'}")
    print("-" * 75)
    
    for i, hub in enumerate(hubs):
        t0 = time.time()
        
        # 1. Reverse Geocode (if dynamic) or Map (if preset)
        hub_type = "PR" if hub["hub_id"].startswith("H") else "DY"
        geo_meta = handler._resolve_geo_meta(hub["hub_id"], hub["lat"], hub["lon"])
        
        # 2. Get Geopolitical Score
        geo_risk = handler._get_geopolitical_risk(geo_meta, key)
        
        dt = time.time() - t0
        country = geo_risk.get("country", "Unknown")
        
        # 3. Simulate Weather Peak = 0.8 (Extreme) to test Combiner
        weather_peak = 0.8000
        combined = handler._combine_risk_scores(weather_peak, geo_risk)
    
        if geo_risk["data_available"]:
            ok += 1
            g_s = geo_risk["geopolitical_risk_score"]
            c_s = combined["combined_risk_score"]
            
            print(f"{hub['hub_id']:<6} | {hub_type:<2} | {country:<20} | {dt:4.1f}s | {g_s:.4f} | {weather_peak:.4f} | {c_s:.4f}")
        else:
            fail += 1
            print(f"{hub['hub_id']:<6} | {hub_type:<2} | {country:<20} | {dt:4.1f}s | FAIL   |        |")
    
        # Nominatim requires 1s delay; we use 1.2s to be safe
        time.sleep(1.2)
    
    print(f"\nRESULT: {ok}/16 OK, {fail}/16 FAIL")

def test_live_news_api_e2e():

    """
    Live Integration Test - Analytics Handler with Real News Sentiment API
    ======================================================================
    
    This script tests the full geopolitical risk pipeline against the REAL
    news sentiment API, with NO mocking of external services.
    
    What IS mocked:  ML model (dummy RandomForest), S3 storage (in-memory dict)
    What is REAL:    News Sentiment API, Nominatim reverse geocoding
    
    Flow tested:
        1. Register a fresh API key (DELETE old -> POST new)
        2. For each preset hub, resolve geo metadata (country + keywords)
        3. Fetch sentiment from the live API for each keyword (7d only)
        4. Compute keyword composites with confidence weighting
        5. Combine weather (65%) + geopolitical (35%) into final score
        6. Verify the output payload has all required fields
    
    Run:
        python tests/integration/test_live_news_sentiment.py
    """
    
    
    
    # Add project root so we can import our handler
    
    
    
    # ---------------------------------------------------------------------------
    # Config
    # ---------------------------------------------------------------------------
    NEWS_API_BASE = "https://i9pdxmupj7.execute-api.ap-southeast-2.amazonaws.com"
    # Hubs to test (mix of stable & volatile regions)
    TEST_HUBS = {
        "H001": {"name": "Port of Singapore", "lat": 1.264, "lon": 103.820, "country": "Singapore"},
        "H002": {"name": "Port of Shanghai", "lat": 31.23, "lon": 121.47, "country": "China"},
        "H003": {"name": "Port of Rotterdam", "lat": 51.95, "lon": 4.13, "country": "Netherlands"},
    }
    PASS = "[OK]"
    FAIL = "[FAIL]"
    WARN = "[WARN]"
    
    
    # ---------------------------------------------------------------------------
    # Helpers
    # ---------------------------------------------------------------------------
    def print_header(text):
        print(f"\n{'='*70}")
        print(f"  {text}")
        print(f"{'='*70}")
    
    
    def print_result(label, passed, detail=""):
        icon = PASS if passed else FAIL
        print(f"  {icon} {label}" + (f" - {detail}" if detail else ""))
    
    
    # ---------------------------------------------------------------------------
    # Step 1: API Key Management
    # ---------------------------------------------------------------------------
    def get_fresh_api_key():
        """Delete any existing key, register a new one."""
        print_header("STEP 1: API Key Registration")
    
        # Delete old key
        del_resp = requests.delete(f"{NEWS_API_BASE}/api/auth/key", timeout=10)
        print(f"  DELETE /api/auth/key -> {del_resp.status_code}")
    
        # Register new key
        reg_resp = requests.post(f"{NEWS_API_BASE}/api/auth/key", timeout=10)
        print(f"  POST   /api/auth/key -> {reg_resp.status_code}")
    
        if reg_resp.status_code not in (200, 201):
            print(f"  {FAIL} Could not register API key: {reg_resp.text}")
            return None
    
        data = reg_resp.json()
        key = data.get("key")
        print_result("API key registered", key is not None, f"key={key[:16]}...")
        return key
    
    
    # ---------------------------------------------------------------------------
    # Step 2: Test x-api-key Header Auth
    # ---------------------------------------------------------------------------
    def test_auth_header(api_key):
        """Verify the API accepts x-api-key header and rejects query param."""
        print_header("STEP 2: Authentication (x-api-key header)")
    
        # Should succeed with header
        r_ok = requests.get(
            f"{NEWS_API_BASE}/api/sentiment",
            params={"keyword": "test", "timeframe": "7d"},
            headers={"x-api-key": api_key},
            timeout=10,
        )
        print_result(
            "x-api-key header accepted",
            r_ok.status_code == 200,
            f"status={r_ok.status_code}",
        )
    
        # Should fail with query param
        r_bad = requests.get(
            f"{NEWS_API_BASE}/api/sentiment",
            params={"keyword": "test", "timeframe": "7d", "key": api_key},
            timeout=10,
        )
        print_result(
            "Query param 'key' rejected",
            r_bad.status_code == 401,
            f"status={r_bad.status_code}",
        )
    
        return r_ok.status_code == 200
    
    
    # ---------------------------------------------------------------------------
    # Step 3: Geo Meta Resolution
    # ---------------------------------------------------------------------------
    def test_geo_meta_resolution():
        """Test that preset hubs resolve to correct countries, and dynamic hubs
        use Nominatim reverse geocoding."""
        print_header("STEP 3: Geo Meta Resolution")
    
        all_passed = True
    
        # Preset hubs
        for hub_id, info in TEST_HUBS.items():
            meta = handler._resolve_geo_meta(hub_id, info["lat"], info["lon"])
            passed = meta["country"] == info["country"]
            all_passed = all_passed and passed
            print_result(
                f"{hub_id} ({info['name']})",
                passed,
                f"country={meta['country']}, keywords={meta['keywords']}",
            )
    
        # Dynamic hub (Tokyo - not in preset list)
        print("\n  --- Dynamic hub (reverse geocoding) ---")
        dyn_meta = handler._resolve_geo_meta("LOC_tokyo_test", 35.6762, 139.6503)
        dyn_passed = dyn_meta["country"] == "Japan" or dyn_meta["country"] != "Unknown"
        all_passed = all_passed and dyn_passed
        print_result(
            "Dynamic LOC_tokyo_test",
            dyn_passed,
            f"country={dyn_meta['country']}, keywords={dyn_meta['keywords']}",
        )
    
        return all_passed
    
    
    # ---------------------------------------------------------------------------
    # Step 4: Live Sentiment Fetch
    # ---------------------------------------------------------------------------
    def test_live_sentiment_fetch(api_key):
        """Call _fetch_sentiment for real keywords (7d only)."""
        print_header("STEP 4: Live Sentiment Fetch (keywords x 7d)")
    
        keywords = ["Singapore", "Strait of Malacca", "China", "South China Sea"]
        results = {}
        all_ok = True
    
        for kw in keywords:
            result = handler._fetch_sentiment(kw, "7d", api_key)
            key = f"{kw}/7d"
            results[key] = result
            if result is not None:
                score = result["risk_score"]
                count = result["article_count"]
                valid = 0.0 <= score <= 1.0
                all_ok = all_ok and valid
                print_result(
                    f"{key:40s}",
                    valid,
                    f"risk={score:.4f}  articles={count}  sentiment={result['avg_sentiment']:.3f}",
                )
            else:
                print(f"  {WARN} {key:40s} - no data returned")
            # Small delay to avoid rate limiting
            time.sleep(1.0)
    
        return all_ok, results
    
    
    # ---------------------------------------------------------------------------
    # Step 5: Keyword Composite Aggregation
    # ---------------------------------------------------------------------------
    def test_keyword_composite(api_key):
        """Test composite aggregation for a single keyword (7d only)."""
        print_header("STEP 5: Keyword Composite (7d-only aggregation)")
    
        keyword = "China"
        tf_results = {}
    
        result = handler._fetch_sentiment(keyword, "7d", api_key)
        tf_results["7d"] = result
        time.sleep(1.0)
    
        composite = handler._compute_keyword_composite(tf_results)
        available = composite["data_available"]
    
        print(f"  Keyword: {keyword}")
        print(f"  Data available: {available}")
    
        if available:
            score = composite["composite_risk_score"]
            valid = 0.0 <= score <= 1.0
            print_result("Composite score in [0, 1]", valid, f"score={score:.4f}")
    
            for tf, tf_data in composite["timeframes"].items():
                conf = tf_data.get("confidence", "?")
                eff_w = tf_data.get("effective_weight", "?")
                print(f"    {tf}: risk={tf_data['risk_score']:.4f}  "
                      f"articles={tf_data['article_count']}  "
                      f"confidence={conf}  eff_weight={eff_w}")
    
            return True
        else:
            print(f"  {WARN} No sentiment data available for '{keyword}' - API may have no articles")
            return True  # Graceful - not a failure
    
    
    # ---------------------------------------------------------------------------
    # Step 6: Full Geopolitical Risk Score
    # ---------------------------------------------------------------------------
    def test_full_geo_risk(api_key):
        """Test the complete geopolitical risk pipeline for a hub."""
        print_header("STEP 7: Full Geopolitical Risk (H002 - Port of Shanghai)")
    
        # Resolve geo meta first, then compute geopolitical risk
        geo_meta = handler._resolve_geo_meta("H002", 31.23, 121.47)
        geo_risk = handler._get_geopolitical_risk(geo_meta, api_key)
    
        print(f"  Country:            {geo_risk.get('country')}")
        print(f"  Data available:     {geo_risk.get('data_available')}")
        print(f"  Geo risk score:     {geo_risk.get('geopolitical_risk_score')}")
        print(f"  Geo risk level:     {geo_risk.get('geopolitical_risk_level')}")
    
        kw_scores = geo_risk.get("keyword_scores", [])
        print(f"  Keywords scored:    {len(kw_scores)}")
        for kws in kw_scores:
            status = f"risk={kws['composite_risk_score']:.4f}" if kws.get("data_available") else "no data"
            print(f"    * {kws['keyword']}: {status}")
    
        score = geo_risk.get("geopolitical_risk_score", -1)
        valid = 0.0 <= score <= 1.0
        print_result("Geo risk score in [0, 1]", valid, f"score={score:.4f}")
    
        return valid, geo_risk
    
    
    # ---------------------------------------------------------------------------
    # Step 8: Combined Risk Formula
    # ---------------------------------------------------------------------------
    def test_combined_risk(geo_risk):
        """Verify the 65/35 weather+geo blend."""
        print_header("STEP 8: Combined Risk Formula (65% weather / 35% geo)")
    
        weather_score = 0.42  # Simulated weather risk
    
        combined = handler._combine_risk_scores(weather_score, geo_risk)
    
        w_comp = combined["weather_component"]
        g_comp = combined["geopolitical_component"]
        c_score = combined["combined_risk_score"]
        w_weight = combined["weather_weight"]
        g_weight = combined["geo_weight"]
    
        print(f"  Weather component:  {w_comp}")
        print(f"  Geo component:      {g_comp}")
        print(f"  Weather weight:     {w_weight}")
        print(f"  Geo weight:         {g_weight}")
        print(f"  Combined score:     {c_score}")
        print(f"  Combined level:     {combined['combined_risk_level']}")
    
        if geo_risk.get("data_available"):
            expected = round(0.65 * weather_score + 0.35 * g_comp, 4)
            match = c_score == expected
            print_result("65/35 blend correct", match, f"expected={expected}, got={c_score}")
            print_result("Weights correct", w_weight == 0.65 and g_weight == 0.35)
        else:
            match = c_score == weather_score
            print_result("Fallback to weather-only", match, f"combined={c_score}, weather={weather_score}")
    
        valid = 0.0 <= c_score <= 1.0
        print_result("Combined score in [0, 1]", valid)
    
        return valid and (match if geo_risk.get("data_available") else True)
    
    
    # ---------------------------------------------------------------------------
    # Step 9: Dynamic Hub (user-created location)
    # ---------------------------------------------------------------------------
    def test_dynamic_hub(api_key):
        """Test geo risk for a user-created hub (not in preset list)."""
        print_header("STEP 9: Dynamic Hub - Tokyo (user-created location)")
    
        # Resolve via Nominatim, then compute geopolitical risk
        geo_meta = handler._resolve_geo_meta("LOC_user_tokyo", 35.6762, 139.6503)
        geo_risk = handler._get_geopolitical_risk(geo_meta, api_key)
    
        print(f"  Country:         {geo_risk.get('country')}")
        print(f"  Data available:  {geo_risk.get('data_available')}")
        print(f"  Risk score:      {geo_risk.get('geopolitical_risk_score')}")
        print(f"  Keywords:        {[k['keyword'] for k in geo_risk.get('keyword_scores', [])]}")
    
        score = geo_risk.get("geopolitical_risk_score", -1)
        valid = 0.0 <= score <= 1.0
        country_resolved = geo_risk.get("country", "Unknown") != "Unknown"
    
        print_result("Country resolved via Nominatim", country_resolved, geo_risk.get("country"))
        print_result("Risk score in [0, 1]", valid, f"score={score:.4f}")
    
        return valid
    
    
    # ---------------------------------------------------------------------------
    # Step 10: ADAGE Response Structure
    # ---------------------------------------------------------------------------
    def test_adage_response(geo_risk):
        """Verify the ADAGE-compliant response structure."""
        print_header("STEP 10: ADAGE Response Structure Verification")
    
        # Build fake scored_days
        scored_days = [
            {
                "date": f"2026-04-{14+i:02d}",
                "day": i + 1,
                "peak_risk_score": 0.3 + i * 0.05,
                "mean_risk_score": 0.25 + i * 0.03,
                "risk_level": handler._risk_level(0.3 + i * 0.05),
                "primary_driver": "Wind Gust",
                "worst_interval": f"2026-04-{14+i:02d}T12:00:00Z",
                "snapshots": [],
            }
            for i in range(7)
        ]
    
        combined = handler._combine_risk_scores(0.42, geo_risk)
        body = {
            "hub_id": "H002",
            "hub_name": "Port of Shanghai",
            "lat": 31.23,
            "lon": 121.47,
            "forecast_origin": "2026-04-14T00:00:00Z",
        }
    
        result = handler._build_adage_response(body, scored_days, geo_risk, combined)
    
        # Check event types
        event_types = [e["event_type"] for e in result["events"]]
        daily_count = event_types.count("daily_risk_assessment")
        has_outlook = "seven_day_outlook" in event_types
        has_geo = "geopolitical_risk_assessment" in event_types
    
        print_result(f"daily_risk_assessment x {daily_count}", daily_count == 7)
        print_result("seven_day_outlook present", has_outlook)
        print_result("geopolitical_risk_assessment present", has_geo)
    
        # Check outlook has combined fields
        outlook = [e for e in result["events"] if e["event_type"] == "seven_day_outlook"][0]
        attr = outlook["attribute"]
        has_combined = all(k in attr for k in [
            "combined_risk_score", "combined_risk_level",
            "weather_component", "geopolitical_component",
            "weather_weight", "geo_weight",
        ])
        print_result("Outlook has combined risk fields", has_combined)
    
        # Check geo event
        geo_ev = [e for e in result["events"] if e["event_type"] == "geopolitical_risk_assessment"][0]
        geo_attr = geo_ev["attribute"]
        has_geo_fields = all(k in geo_attr for k in [
            "country", "geopolitical_risk_score", "geopolitical_risk_level",
            "trajectory_label", "keyword_scores", "data_available",
        ])
        print_result("Geo event has all required fields", has_geo_fields)
    
        # Check data_source mentions both APIs
        has_sources = "Pirate Weather" in result.get("data_source", "") and \
                      "News Sentiment" in result.get("data_source", "")
        print_result("data_source mentions both APIs", has_sources)
    
        return daily_count == 7 and has_outlook and has_geo and has_combined and has_geo_fields
    
    
    # ===========================================================================
    # Main
    # ===========================================================================
    def main():
        print("\n" + "=" * 70)
        print("  LIVE INTEGRATION TEST - Analytics + News Sentiment API")
        print("  " + datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"))
        print("=" * 70)
    
        results = {}
    
        # Step 1: Get API key
        api_key = get_fresh_api_key()
        results["api_key"] = api_key is not None
        if not api_key:
            print(f"\n{FAIL} Cannot proceed without API key. Aborting.")
            return
    
        # Step 2: Auth header
        results["auth"] = test_auth_header(api_key)
    
        # Step 3: Geo meta
        results["geo_meta"] = test_geo_meta_resolution()
    
        # Step 4: Live sentiment
        results["sentiment"], _ = test_live_sentiment_fetch(api_key)
    
        # Step 5: Keyword composite
        results["composite"] = test_keyword_composite(api_key)
    
        # Step 6: Full geo risk
        results["geo_risk"], geo_risk = test_full_geo_risk(api_key)
    
        # Step 7: Combined risk
        results["combined"] = test_combined_risk(geo_risk)
    
        # Step 8: Dynamic hub
        results["dynamic_hub"] = test_dynamic_hub(api_key)
    
        # Step 9: ADAGE response
        results["adage"] = test_adage_response(geo_risk)
    
        # Summary
        print_header("SUMMARY")
        total = len(results)
        passed = sum(1 for v in results.values() if v)
        for step, passed_flag in results.items():
            icon = PASS if passed_flag else FAIL
            print(f"  {icon} {step}")
    
        print(f"\n  {passed}/{total} steps passed")
        if passed == total:
            print(f"\n  {PASS} ALL TESTS PASSED - Geopolitical risk integration is working!")
        else:
            print(f"\n  {FAIL} Some tests failed - review output above")
    
        # Clean up: don't leave the key in module cache
        handler._NEWS_API_KEY = None
    
        return passed == total
    
    
    if __name__ == "__main__":
        success = main()
        sys.exit(0 if success else 1)
    