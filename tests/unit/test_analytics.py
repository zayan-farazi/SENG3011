import json
import math
import os
from unittest.mock import MagicMock, patch
from lambdas.analytics import handler
import io
import tempfile
import pytest
import numpy as np
import joblib  # type: ignore[import-untyped]
from unittest.mock import Mock
from sklearn.ensemble import RandomForestRegressor  # type: ignore[import-untyped]
from moto import mock_aws
import boto3

from lambdas.analytics.handler import lambda_handler
from tests.test_constants import (
    TEST_BUCKET_NAME, HUB_ID_1, HUB_INVALID, PROCESSED_WEATHER_DATA_FILE,
)
from constants import (
    STATUS_OK, STATUS_BAD_REQUEST, STATUS_NOT_FOUND, STATUS_INTERNAL_SERVER_ERROR,
    HUBS_FILE_KEY, MODEL_S3_KEY, DEFAULT_REGION
)


def _create_dummy_model():
    np.random.seed(42)
    X = np.random.rand(20, 6).astype(np.float32)
    y = np.random.rand(20).astype(np.float32)
    model = RandomForestRegressor(n_estimators=2, random_state=42)
    model.fit(X, y)
    buf = io.BytesIO()
    joblib.dump(model, buf)
    buf.seek(0)
    return buf.read()


@pytest.fixture(autouse=True)
def reset_model_cache():
    handler._MODEL = None
    tmp = os.path.join(tempfile.gettempdir(), "risk_model.joblib")
    if os.path.exists(tmp):
        os.remove(tmp)
    yield
    handler._MODEL = None
    if os.path.exists(tmp):
        os.remove(tmp)


@pytest.fixture
def setup_analytics_s3():
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=TEST_BUCKET_NAME)
        os.environ["DATA_BUCKET"] = TEST_BUCKET_NAME
        os.environ["API_BASE_URL"] = "http://test-api"

        with open(HUBS_FILE_KEY, "r") as f:
            hubs = json.load(f)
        s3.put_object(Bucket=TEST_BUCKET_NAME, Key=HUBS_FILE_KEY, Body=json.dumps(hubs))

        model_bytes = _create_dummy_model()
        s3.put_object(Bucket=TEST_BUCKET_NAME, Key=MODEL_S3_KEY, Body=model_bytes)

        yield s3


def _mock_retrieval_response():
    with open(PROCESSED_WEATHER_DATA_FILE, "r") as f:
        data = json.load(f)
    mock_resp = Mock()
    mock_resp.status_code = STATUS_OK
    mock_resp.json.return_value = data
    mock_resp.text = json.dumps(data)
    return mock_resp


@patch("lambdas.analytics.handler._get_news_api_key", return_value="fake-key")
@patch("lambdas.analytics.handler.requests.get")
def test_valid_risk_request(mock_get, mock_api_key, setup_analytics_s3):
    def mock_get_side_effect(url, **kwargs):
        if "api/sentiment" in url:
            mock_resp = Mock()
            mock_resp.status_code = 200
            mock_resp.json.return_value = {
                "country": kwargs.get("params", {}).get("country", "Singapore"),
                "timeframe": "7d",
                "articleCount": 100,
                "averageSentiment": 0.0,
                "distribution": {"positive": 50, "neutral": 0, "negative": 50}
            }
            return mock_resp
        return _mock_retrieval_response()

    mock_get.side_effect = mock_get_side_effect

    event = {
        "pathParameters": {"hub_id": HUB_ID_1},
        "queryStringParameters": {"date": "10-03-2026"},
    }

    result = lambda_handler(event, None)
    assert result["statusCode"] == STATUS_OK

    body = json.loads(result["body"])
    assert "events" in body
    assert "data_source" in body
    assert body["dataset_type"] == "Supply Chain Disruption Risk Assessment"

    daily_events = [e for e in body["events"] if e["event_type"] == "daily_risk_assessment"]
    outlook_events = [e for e in body["events"] if e["event_type"] == "seven_day_outlook"]
    geo_events = [e for e in body["events"] if e["event_type"] == "geopolitical_risk_assessment"]
    assert len(daily_events) >= 1
    assert len(outlook_events) == 1
    assert len(geo_events) == 1

    day = daily_events[0]["attribute"]
    assert "peak_risk_score" in day
    assert "risk_level" in day
    assert "snapshots" in day
    assert day["risk_level"] in ("Low", "Elevated", "High", "Critical")

    for s in day["snapshots"]:
        assert "risk_score" in s
        assert "risk_level" in s
        assert "primary_driver" in s
        assert 0.0 <= s["risk_score"] <= 1.0

    outlook = outlook_events[0]["attribute"]
    assert "combined_risk_score" in outlook
    assert "geopolitical_component" in outlook
    assert outlook["geo_weight"] == 0.35
    assert outlook["weather_weight"] == 0.65


def test_missing_hub_id():
    event = {
        "pathParameters": {},
        "queryStringParameters": {"date": "10-03-2026"},
    }

    result = lambda_handler(event, None)
    assert result["statusCode"] == STATUS_BAD_REQUEST
    assert json.loads(result["body"]) == {"error": "Missing hub_id"}


@patch("lambdas.analytics.handler.requests.get")
def test_invalid_hub_id(mock_get, setup_analytics_s3):
    mock_hub_resp = Mock()
    mock_hub_resp.status_code = STATUS_NOT_FOUND
    mock_get.return_value = mock_hub_resp
    event = {
        "pathParameters": {"hub_id": HUB_INVALID},
        "queryStringParameters": {"date": "10-03-2026"},
    }

    result = lambda_handler(event, None)
    assert result["statusCode"] == STATUS_BAD_REQUEST
    assert json.loads(result["body"]) == {"error": "Invalid hub_id"}


@patch("lambdas.analytics.handler.requests.get")
def test_retrieval_fails(mock_get, setup_analytics_s3):
    valid_hub_resp = Mock()
    valid_hub_resp.status_code = STATUS_OK

    retrieval_resp = Mock()
    retrieval_resp.status_code = STATUS_INTERNAL_SERVER_ERROR
    retrieval_resp.text = "Internal server error"

    mock_get.side_effect = [valid_hub_resp, retrieval_resp]

    event = {
        "pathParameters": {"hub_id": HUB_ID_1},
        "queryStringParameters": {"date": "10-03-2026"},
    }

    result = lambda_handler(event, None)
    assert result["statusCode"] == 502


@patch("lambdas.analytics.handler.requests.get")
def test_retrieval_not_found(mock_get, setup_analytics_s3):
    valid_hub_resp = Mock()
    valid_hub_resp.status_code = STATUS_OK

    retrieval_resp = Mock()
    retrieval_resp.status_code = STATUS_NOT_FOUND
    retrieval_resp.text = "Not found"

    mock_get.side_effect = [valid_hub_resp, retrieval_resp]

    event = {
        "pathParameters": {"hub_id": HUB_ID_1},
        "queryStringParameters": {"date": "10-03-2026"},
    }

    result = lambda_handler(event, None)
    assert result["statusCode"] == STATUS_NOT_FOUND
    assert "not found" in json.loads(result["body"])["error"].lower()


@patch("lambdas.analytics.handler.requests.get")
def test_model_not_found(mock_get, setup_analytics_s3):
    s3 = setup_analytics_s3
    s3.delete_object(Bucket=TEST_BUCKET_NAME, Key=MODEL_S3_KEY)
    mock_get.return_value = _mock_retrieval_response()

    event = {
        "pathParameters": {"hub_id": HUB_ID_1},
        "queryStringParameters": {"date": "10-03-2026"},
    }

    result = lambda_handler(event, None)
    assert result["statusCode"] == STATUS_INTERNAL_SERVER_ERROR


@patch("lambdas.analytics.handler.requests.get")
def test_missing_feature(mock_get, setup_analytics_s3):
    with open(PROCESSED_WEATHER_DATA_FILE, "r") as f:
        data = json.load(f)
    del data["days"][0]["snapshots"][0]["features"]["temperature"]

    mock_resp = Mock()
    mock_resp.status_code = STATUS_OK
    mock_resp.json.return_value = data
    mock_get.return_value = mock_resp

    event = {
        "pathParameters": {"hub_id": HUB_ID_1},
        "queryStringParameters": {"date": "10-03-2026"},
    }

    result = lambda_handler(event, None)
    assert result["statusCode"] == STATUS_BAD_REQUEST
    assert "temperature" in json.loads(result["body"])["error"]


def _make_s3_event(bucket, key):
    """Build a minimal S3 notification event."""
    return {
        "Records": [{
            "eventSource": "aws:s3",
            "s3": {
                "bucket": {"name": bucket},
                "object": {"key": key},
            },
        }],
    }


@patch("lambdas.analytics.handler.requests.get")
def test_s3_event_triggers_risk_computation(mock_get, setup_analytics_s3):
    s3 = setup_analytics_s3
    mock_get.return_value = _mock_retrieval_response()

    s3_key = f"processed/weather/{HUB_ID_1}/10-03-2026.json"

    event = _make_s3_event(TEST_BUCKET_NAME, s3_key)
    result = lambda_handler(event, None)

    assert result[0]["status"] == "scored"
    assert result[0]["hub_id"] == HUB_ID_1

    # Verify latest.json was written
    obj = s3.get_object(
        Bucket=TEST_BUCKET_NAME,
        Key=f"risk/weather/{HUB_ID_1}/latest.json",
    )
    cached = json.loads(obj["Body"].read())
    assert "events" in cached
    assert cached["dataset_type"] == "Supply Chain Disruption Risk Assessment"


def test_s3_event_ignores_irrelevant_key(setup_analytics_s3):
    s3 = setup_analytics_s3
    s3.put_object(
        Bucket=TEST_BUCKET_NAME,
        Key="raw/weather/H001/10-03-2026.json",
        Body=b"{}",
    )

    event = _make_s3_event(TEST_BUCKET_NAME, "raw/weather/H001/10-03-2026.json")
    result = lambda_handler(event, None)

    assert result[0]["status"] == "ignored"


@patch("lambdas.analytics.handler.requests.get")
def test_api_returns_cached_result(mock_get, setup_analytics_s3):
    mock_hub_resp = Mock()
    mock_hub_resp.status_code = STATUS_OK
    mock_get.return_value = mock_hub_resp
    s3 = setup_analytics_s3
    cached_body = {"events": [], "cached": True}
    s3.put_object(
        Bucket=TEST_BUCKET_NAME,
        Key=f"risk/weather/{HUB_ID_1}/latest.json",
        Body=json.dumps(cached_body),
        ContentType="application/json",
    )

    event = {
        "pathParameters": {"hub_id": HUB_ID_1},
        "queryStringParameters": {"date": "10-03-2026"},
    }

    result = lambda_handler(event, None)
    assert result["statusCode"] == STATUS_OK
    body = json.loads(result["body"])
    assert body["cached"] is True


@patch("lambdas.analytics.handler.requests.get")
def test_api_returns_cached_result_and_backfills_score_table(mock_get, setup_analytics_s3):
    mock_hub_resp = Mock()
    mock_hub_resp.status_code = STATUS_OK
    mock_get.return_value = mock_hub_resp

    dynamodb = boto3.resource("dynamodb", region_name=DEFAULT_REGION)
    scores_table = dynamodb.create_table(
        TableName="scores",
        KeySchema=[{"AttributeName": "hub_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "hub_id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    scores_table.wait_until_exists()

    s3 = setup_analytics_s3
    cached_body = {
        "events": [
            {
                "event_type": "seven_day_outlook",
                "attribute": {"combined_risk_score": 0.42},
            }
        ],
        "cached": True,
    }
    s3.put_object(
        Bucket=TEST_BUCKET_NAME,
        Key=f"risk/weather/{HUB_ID_1}/latest.json",
        Body=json.dumps(cached_body),
        ContentType="application/json",
    )

    event = {
        "pathParameters": {"hub_id": HUB_ID_1},
        "queryStringParameters": {"date": "10-03-2026"},
    }

    result = lambda_handler(event, None)

    assert result["statusCode"] == STATUS_OK
    item = scores_table.get_item(Key={"hub_id": HUB_ID_1})["Item"]
    assert float(item["risk_score"]) == pytest.approx(0.42)


@patch("lambdas.analytics.handler.requests.get")
def test_api_falls_back_to_compute(mock_get, setup_analytics_s3):
    mock_get.return_value = _mock_retrieval_response()

    event = {
        "pathParameters": {"hub_id": HUB_ID_1},
        "queryStringParameters": {"date": "10-03-2026"},
    }

    result = lambda_handler(event, None)
    assert result["statusCode"] == STATUS_OK

    body = json.loads(result["body"])
    assert "events" in body
    assert body["dataset_type"] == "Supply Chain Disruption Risk Assessment"


@patch("lambdas.analytics.handler.requests.get")
def test_s3_event_ignores_invalid_hub(mock_get, setup_analytics_s3):
    mock_hub_resp = Mock()
    mock_hub_resp.status_code = STATUS_NOT_FOUND
    mock_get.return_value = mock_hub_resp
    s3 = setup_analytics_s3
    s3_key = "processed/weather/FAKE_HUB/10-03-2026.json"
    s3.put_object(
        Bucket=TEST_BUCKET_NAME, Key=s3_key,
        Body=b"{}",
    )

    event = _make_s3_event(TEST_BUCKET_NAME, s3_key)
    result = lambda_handler(event, None)

    assert result[0]["status"] == "ignored"
    assert result[0]["reason"] == "invalid hub_id"


@patch("lambdas.analytics.handler.requests.get")
def test_s3_event_multiple_records(mock_get, setup_analytics_s3):
    """Multiple S3 records in one event including one valid, one irrelevant prefix, one invalid hub."""
    def side_effect(url, *args, **kwargs):
        if f"/{HUB_INVALID}" in url:
            resp = Mock()
            resp.status_code = STATUS_NOT_FOUND
            return resp
        return _mock_retrieval_response()

    mock_get.side_effect = side_effect

    valid_key = f"processed/weather/{HUB_ID_1}/10-03-2026.json"
    irrelevant_key = "raw/weather/H001/10-03-2026.json"
    invalid_hub_key = f"processed/weather/{HUB_INVALID}/10-03-2026.json"

    event = {
        "Records": [
            {
                "eventSource": "aws:s3",
                "s3": {
                    "bucket": {"name": TEST_BUCKET_NAME},
                    "object": {"key": valid_key},
                },
            },
            {
                "eventSource": "aws:s3",
                "s3": {
                    "bucket": {"name": TEST_BUCKET_NAME},
                    "object": {"key": irrelevant_key},
                },
            },
            {
                "eventSource": "aws:s3",
                "s3": {
                    "bucket": {"name": TEST_BUCKET_NAME},
                    "object": {"key": invalid_hub_key},
                },
            },
        ],
    }

    result = lambda_handler(event, None)

    assert len(result) == 3

    # valid hub, should be scored
    assert result[0]["status"] == "scored"
    assert result[0]["hub_id"] == HUB_ID_1

    # irrelevant prefix, should be ignored
    assert result[1]["status"] == "ignored"
    assert result[1]["key"] == irrelevant_key

    # invalid hub, should be ignored
    assert result[2]["status"] == "ignored"
    assert result[2]["reason"] == "invalid hub_id"





@patch("lambdas.analytics.handler._get_news_api_key", return_value="fake-key")
@patch("lambdas.analytics.handler.requests.get")
@patch("lambdas.analytics.handler.validate_hub_id", return_value=True)
def test_dynamic_hub_reverse_geocoding_sentiment(mock_val_hub, mock_get, mock_api_key, setup_analytics_s3):
    # Create fake retrieval response but modify hub_id to an unknown dynamic hub
    with open(PROCESSED_WEATHER_DATA_FILE, "r") as f:
        data = json.load(f)
    data["hub_id"] = "LOC_DYNAMIC99"
    data["lat"] = -33.8688
    data["lon"] = 151.2093

    dynamic_country = "Australia"

    def mock_get_side_effect(url, **kwargs):
        if "api/sentiment" in url:
            params = kwargs.get("params", {})
            mock_resp = Mock()
            mock_resp.status_code = 200
            # Test that it queries sentiment on the dynamic country
            assert params.get("keyword") == dynamic_country
            mock_resp.json.return_value = {
                "country": params.get("keyword"),
                "timeframe": "7d",
                "articleCount": 200,
                "averageSentiment": 1.0,
                "distribution": {"positive": 200, "neutral": 0, "negative": 0}
            }
            return mock_resp
        elif "openstreetmap" in url:
            mock_resp = Mock()
            mock_resp.status_code = 200
            params = kwargs.get("params", {})
            assert params.get("lat") == data["lat"]
            assert params.get("lon") == data["lon"]
            mock_resp.json.return_value = {"address": {"country": dynamic_country}}
            return mock_resp
        elif "ese/v1/retrieve/processed/weather" in url:
            mock_resp = Mock()
            mock_resp.status_code = STATUS_OK
            mock_resp.json.return_value = data
            mock_resp.text = json.dumps(data)
            return mock_resp

        mock_resp = Mock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {}
        return mock_resp

    mock_get.side_effect = mock_get_side_effect

    event = {
        "pathParameters": {"hub_id": "LOC_DYNAMIC99"},
        "queryStringParameters": {"date": "10-03-2026"},
    }

    # we must patch validate_hub_id correctly inside handler since it's hardcoded and does requests.get in handler.py
    # actually, validate_hub_id in handler uses requests.get so we must mock it there as well if we don't mock the function
    # Let's add validate location URL handling to our mock_get_side_effect
    def mock_get_side_effect_with_validation(url, **kwargs):
        if "ese/v1/location/" in url:
            mock_resp = Mock()
            mock_resp.status_code = STATUS_OK
            return mock_resp
        return mock_get_side_effect(url, **kwargs)

    mock_get.side_effect = mock_get_side_effect_with_validation

    result = lambda_handler(event, None)
    assert result["statusCode"] == STATUS_OK, result["body"]
    body = json.loads(result["body"])

    geo_events = [e for e in body["events"] if e["event_type"] == "geopolitical_risk_assessment"]
    assert len(geo_events) == 1
    geo = geo_events[0]["attribute"]

    # Validates Reverse Geocoding extracted country correctly
    assert geo["country"] == dynamic_country
    assert geo["data_available"] is True
    # The geopolitical_risk_score should be derived from the api.
    # We mocked averageSentiment = 1.0, so risk_score should be 0.0
    # composite_risk_score logic in handler -> risk = 0.0
    assert geo["geopolitical_risk_score"] == 0.0

    outlook_events = [e for e in body["events"] if e["event_type"] == "seven_day_outlook"]
    outlook = outlook_events[0]["attribute"]
    assert "combined_risk_score" in outlook
    assert outlook["geo_weight"] == 0.35

"""
Unit tests for the geopolitical risk integration in the new analytics handler.

Tests cover:
    - Sentiment-to-risk conversion
    - Timeframe confidence calculation
    - Per-keyword composite aggregation (7d-only mode)
    - Combined risk formula (65% weather + 35% geopolitical)
    - Graceful degradation when geo data is unavailable
    - API authentication via x-api-key header (not query param)
    - Full integration flow with mocked external calls
    - Dynamic hub geo resolution via Nominatim
"""






# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_sentiment_response(keyword, timeframe, article_count, avg_sentiment):
    """Build a mock response dict matching the news API schema."""
    positive = int(article_count * max(0, (1 + avg_sentiment) / 2))
    negative = article_count - positive
    return {
        "keyword": keyword,
        "timeframe": timeframe,
        "articleCount": article_count,
        "averageSentiment": avg_sentiment,
        "distribution": {
            "positive": positive,
            "neutral": 0,
            "negative": negative,
        },
    }


def _mock_get_response(status_code, json_data):
    """Create a mock requests.Response."""
    mock_resp = MagicMock()
    mock_resp.status_code = status_code
    mock_resp.json.return_value = json_data
    mock_resp.text = json.dumps(json_data)
    return mock_resp


def _make_snapshot(temp=20, wind_speed=10, wind_gust=15, precip=0.5, pressure=1013, humidity=0.5, timestamp="2026-04-14T00:00:00Z", lead_hours=0):
    return {
        "forecast_timestamp": timestamp,
        "forecast_lead_hours": lead_hours,
        "features": {
            "temperature": temp,
            "wind_speed": wind_speed,
            "wind_gust": wind_gust,
            "precip_intensity": precip,
            "pressure": pressure,
            "humidity": humidity,
        },
    }


def _make_day(day_num, date, snapshots=None):
    if snapshots is None:
        snapshots = [_make_snapshot()]
    return {"day": day_num, "date": date, "snapshots": snapshots}


def _make_processed_data(hub_id="H001", hub_name="Port of Singapore", lat=1.264, lon=103.820, num_days=7):
    days = []
    for i in range(num_days):
        days.append(_make_day(
            day_num=i + 1,
            date=f"2026-04-{14 + i:02d}",
            snapshots=[
                _make_snapshot(timestamp=f"2026-04-{14 + i:02d}T{h:02d}:00:00Z", lead_hours=i * 24 + h)
                for h in [0, 6, 12, 18]
            ],
        ))
    return {
        "schema_version": "1.0",
        "hub_id": hub_id,
        "hub_name": hub_name,
        "lat": lat,
        "lon": lon,
        "forecast_origin": "2026-04-14T00:00:00Z",
        "days": days,
    }


# ===========================================================================
# 1. Sentiment → Risk Conversion
# ===========================================================================

class TestSentimentToRisk:
    """Verify that averageSentiment [-1, 1] maps to risk [0, 1] correctly."""

    @patch("lambdas.analytics.handler.requests.get")
    def test_very_negative_sentiment_gives_high_risk(self, mock_get):
        """averageSentiment = -1.0 → risk = 1.0"""
        mock_get.return_value = _mock_get_response(200, _make_sentiment_response("China", "24h", 50, -1.0))
        result = handler._fetch_sentiment("China", "24h", "test-key")
        assert result is not None
        assert result["risk_score"] == 1.0

    @patch("lambdas.analytics.handler.requests.get")
    def test_neutral_sentiment_gives_mid_risk(self, mock_get):
        """averageSentiment = 0.0 → risk = 0.5"""
        mock_get.return_value = _mock_get_response(200, _make_sentiment_response("China", "24h", 50, 0.0))
        result = handler._fetch_sentiment("China", "24h", "test-key")
        assert result is not None
        assert result["risk_score"] == 0.5

    @patch("lambdas.analytics.handler.requests.get")
    def test_very_positive_sentiment_gives_low_risk(self, mock_get):
        """averageSentiment = 1.0 → risk = 0.0"""
        mock_get.return_value = _mock_get_response(200, _make_sentiment_response("China", "24h", 50, 1.0))
        result = handler._fetch_sentiment("China", "24h", "test-key")
        assert result is not None
        assert result["risk_score"] == 0.0

    @patch("lambdas.analytics.handler.requests.get")
    def test_mixed_sentiment(self, mock_get):
        """averageSentiment = -0.3 → risk = (1 - (-0.3)) / 2 = 0.65"""
        mock_get.return_value = _mock_get_response(200, _make_sentiment_response("China", "7d", 100, -0.3))
        result = handler._fetch_sentiment("China", "7d", "test-key")
        assert result is not None
        assert result["risk_score"] == 0.65


# ===========================================================================
# 2. API Authentication
# ===========================================================================

class TestAPIAuthentication:
    """Verify the API key is sent as x-api-key header, not query param."""

    @patch("lambdas.analytics.handler.requests.get")
    def test_api_key_sent_as_header_not_query_param(self, mock_get):
        mock_get.return_value = _mock_get_response(200, _make_sentiment_response("China", "24h", 10, 0.0))
        handler._fetch_sentiment("China", "24h", "my-secret-key")

        mock_get.assert_called_once()
        call_kwargs = mock_get.call_args
        # Key must NOT be in params
        assert "key" not in call_kwargs.kwargs.get("params", call_kwargs[1].get("params", {}))
        # Key must be in headers as x-api-key
        headers = call_kwargs.kwargs.get("headers", call_kwargs[1].get("headers", {}))
        assert headers.get("x-api-key") == "my-secret-key"

    @patch("lambdas.analytics.handler.requests.get")
    def test_401_does_not_invalidate_cached_key(self, mock_get):
        """A 401 response should NOT clear the cached API key.
        The News Sentiment API uses 401 as a rate-limit signal, not a
        true auth failure. Invalidating on 401 causes cascade failures."""
        handler._NEWS_API_KEY = "old-key"
        mock_get.return_value = _mock_get_response(401, {"code": 401, "message": "Missing or invalid API key"})
        result = handler._fetch_sentiment("China", "7d", "old-key")
        assert result is None
        # Key should NOT be cleared — 401 is a rate-limit signal
        assert handler._NEWS_API_KEY == "old-key"

    @patch("lambdas.analytics.handler.requests.get")
    def test_non_200_returns_none(self, mock_get):
        mock_get.return_value = _mock_get_response(500, {"error": "internal"})
        result = handler._fetch_sentiment("China", "24h", "test-key")
        assert result is None


# ===========================================================================
# 3. Timeframe Confidence
# ===========================================================================

class TestTimeframeConfidence:
    def test_zero_articles_gives_zero_confidence(self):
        assert handler._timeframe_confidence(0, 20) == 0.0

    def test_at_threshold_gives_about_063(self):
        conf = handler._timeframe_confidence(20, 20)
        expected = round(1.0 - math.exp(-1), 4)
        assert conf == expected

    def test_double_threshold_gives_about_086(self):
        conf = handler._timeframe_confidence(40, 20)
        expected = round(1.0 - math.exp(-2), 4)
        assert conf == expected

    def test_large_count_approaches_one(self):
        conf = handler._timeframe_confidence(10000, 20)
        assert conf > 0.99

    def test_threshold_zero_handled(self):
        # Should not divide by zero
        conf = handler._timeframe_confidence(5, 0)
        assert 0.0 <= conf <= 1.0


# ===========================================================================
# 4. Per-Country Composite (7d-only mode)
# ===========================================================================

class TestCountryComposite:
    def test_7d_only_present(self):
        """With 7d-only mode, composite should equal the 7d risk score."""
        results = {
            "7d":  {"risk_score": 0.6, "article_count": 100, "avg_sentiment": -0.2, "distribution": {}},
        }
        composite = handler._compute_country_composite(results)
        assert composite["data_available"] is True
        assert composite["composite_risk_score"] is not None
        assert 0.0 <= composite["composite_risk_score"] <= 1.0
        assert "7d" in composite["timeframes"]

    def test_7d_missing(self):
        """No 7d data -> data_available=False, composite=None."""
        results = {"7d": None}
        composite = handler._compute_country_composite(results)
        assert composite["data_available"] is False
        assert composite["composite_risk_score"] is None

    def test_low_article_count_reduces_confidence(self):
        """A 7d result with very few articles should have lower confidence."""
        results_low = {
            "7d": {"risk_score": 0.8, "article_count": 2, "avg_sentiment": -0.6, "distribution": {}},
        }
        composite_low = handler._compute_country_composite(results_low)

        results_high = {
            "7d": {"risk_score": 0.8, "article_count": 200, "avg_sentiment": -0.6, "distribution": {}},
        }
        composite_high = handler._compute_country_composite(results_high)

        # Both should have data available
        assert composite_low["data_available"] is True
        assert composite_high["data_available"] is True
        # High article count should have higher confidence
        assert composite_high["timeframes"]["7d"]["confidence"] > composite_low["timeframes"]["7d"]["confidence"]


# ===========================================================================
# 5. Combined Risk Formula
# ===========================================================================

class TestCombinedRisk:
    def test_65_35_split_when_geo_available(self):
        """Combined = 0.65 * weather + 0.35 * geo."""
        geo_risk = {"geopolitical_risk_score": 0.8, "data_available": True}
        result = handler._combine_risk_scores(0.4, geo_risk)
        expected = round(0.65 * 0.4 + 0.35 * 0.8, 4)
        assert result["combined_risk_score"] == expected
        assert result["geo_data_available"] is True
        assert result["weather_weight"] == 0.65
        assert result["geo_weight"] == 0.35

    def test_weather_only_when_geo_unavailable(self):
        """When geo data isn't available, combined = weather (no 0.5 inflation)."""
        geo_risk = {"geopolitical_risk_score": 0.5, "data_available": False}
        result = handler._combine_risk_scores(0.4, geo_risk)
        assert result["combined_risk_score"] == 0.4
        assert result["geo_data_available"] is False
        assert result["weather_weight"] == 1.0
        assert result["geo_weight"] == 0.0

    def test_combined_clamped_to_0_1(self):
        """Result should never go below 0 or above 1."""
        geo_risk = {"geopolitical_risk_score": 1.0, "data_available": True}
        result = handler._combine_risk_scores(1.0, geo_risk)
        assert result["combined_risk_score"] <= 1.0

        geo_risk2 = {"geopolitical_risk_score": 0.0, "data_available": True}
        result2 = handler._combine_risk_scores(0.0, geo_risk2)
        assert result2["combined_risk_score"] >= 0.0

    def test_preserves_individual_components(self):
        geo_risk = {"geopolitical_risk_score": 0.7, "data_available": True}
        result = handler._combine_risk_scores(0.3, geo_risk)
        assert result["weather_component"] == 0.3
        assert result["geopolitical_component"] == 0.7


# ===========================================================================
# 7. Geo Meta Resolution
# ===========================================================================

class TestGeoMetaResolution:
    def test_preset_hub_uses_hardcoded_meta(self):
        """Preset hubs (H001-H008) should return hardcoded country."""
        meta = handler._resolve_geo_meta("H001", 1.264, 103.820)
        assert meta["country"] == "Singapore"

    @patch("lambdas.analytics.handler.requests.get")
    def test_dynamic_hub_uses_nominatim(self, mock_get):
        """Dynamic hubs should reverse-geocode via Nominatim."""
        mock_get.return_value = _mock_get_response(200, {
            "address": {"country": "Japan"}
        })
        meta = handler._resolve_geo_meta("LOC_abc123", 35.6762, 139.6503)
        assert meta["country"] == "Japan"

    @patch("lambdas.analytics.handler.requests.get")
    def test_dynamic_hub_nominatim_failure_falls_back_to_region(self, mock_get):
        """If Nominatim fails, fall back to coordinate-based regional keywords."""
        mock_get.side_effect = Exception("timeout")
        meta = handler._resolve_geo_meta("LOC_xyz", 1.3, 103.8)
        assert meta["country"] == "Unknown"

    def test_all_preset_hubs_have_meta(self):
        """All 8 preset hubs should have geo metadata."""
        for hub_id in ["H001", "H002", "H003", "H004", "H005", "H006", "H007", "H008"]:
            assert hub_id in handler.HUB_GEO_META
            meta = handler.HUB_GEO_META[hub_id]
            assert "country" in meta


# ===========================================================================
# 8. Neutral Geo Risk Fallback
# ===========================================================================

class TestNeutralGeoRisk:
    def test_neutral_returns_050(self):
        result = handler._neutral_geo_risk("Unknown")
        assert result["geopolitical_risk_score"] == 0.5
        assert result["data_available"] is False


# ===========================================================================
# 9. ADAGE Response Structure
# ===========================================================================

class TestAdageResponse:
    def test_response_has_three_event_types(self):
        """The response should have daily_risk_assessment, seven_day_outlook, and geopolitical_risk_assessment."""
        scored_days = [
            {
                "date": f"2026-04-{14+i:02d}",
                "day": i + 1,
                "peak_risk_score": 0.3 + i * 0.05,
                "mean_risk_score": 0.25 + i * 0.03,
                "risk_level": "Elevated",
                "primary_driver": "Wind Gust",
                "worst_interval": f"2026-04-{14+i:02d}T12:00:00Z",
                "snapshots": [],
            }
            for i in range(7)
        ]
        geo_risk = handler._neutral_geo_risk("Singapore")
        combined = handler._combine_risk_scores(0.6, geo_risk)
        body = {"hub_id": "H001", "hub_name": "Port of Singapore", "lat": 1.264, "lon": 103.820, "forecast_origin": "2026-04-14T00:00:00Z"}

        result = handler._build_adage_response(body, scored_days, geo_risk, combined)

        event_types = [e["event_type"] for e in result["events"]]
        assert event_types.count("daily_risk_assessment") == 7
        assert "seven_day_outlook" in event_types
        assert "geopolitical_risk_assessment" in event_types

    def test_seven_day_outlook_has_combined_fields(self):
        """The seven_day_outlook event should include both weather-only and combined scores."""
        scored_days = [
            {
                "date": "2026-04-14", "day": 1, "peak_risk_score": 0.45,
                "mean_risk_score": 0.35, "risk_level": "High",
                "primary_driver": "Wind Gust",
                "worst_interval": "2026-04-14T12:00:00Z", "snapshots": [],
            }
        ]
        geo_risk = {
            "country": "Singapore",
            "geopolitical_risk_score": 0.7,
            "geopolitical_risk_level": "High",
            "keyword_scores": [],
            "data_available": True,
        }
        combined = handler._combine_risk_scores(0.45, geo_risk)
        body = {"hub_id": "H001", "hub_name": "Port of Singapore", "lat": 1.264, "lon": 103.820}

        result = handler._build_adage_response(body, scored_days, geo_risk, combined)
        outlook = [e for e in result["events"] if e["event_type"] == "seven_day_outlook"][0]
        attr = outlook["attribute"]

        # Weather-only score (backward compat)
        assert "outlook_risk_score" in attr
        assert attr["outlook_risk_score"] == 0.45
        # Combined score
        assert "combined_risk_score" in attr
        assert "combined_risk_level" in attr
        assert "weather_component" in attr
        assert "geopolitical_component" in attr

    def test_geopolitical_event_has_country_breakdown(self):
        """The geopolitical event should include per-country detail."""
        scored_days = [
            {
                "date": "2026-04-14", "day": 1, "peak_risk_score": 0.3,
                "mean_risk_score": 0.25, "risk_level": "Elevated",
                "primary_driver": "Wind Gust",
                "worst_interval": "2026-04-14T00:00:00Z", "snapshots": [],
            }
        ]
        country_scores = [
            {"country": "Singapore", "composite_risk_score": 0.6, "data_available": True, "timeframes": {}},
        ]
        geo_risk = {
            "country": "Singapore",
            "geopolitical_risk_score": 0.6,
            "geopolitical_risk_level": "High",
            "country_scores": country_scores,
            "data_available": True,
        }
        combined = handler._combine_risk_scores(0.3, geo_risk)
        body = {"hub_id": "H001", "hub_name": "Port of Singapore", "lat": 1.264, "lon": 103.820}

        result = handler._build_adage_response(body, scored_days, geo_risk, combined)
        geo_event = [e for e in result["events"] if e["event_type"] == "geopolitical_risk_assessment"][0]

        assert geo_event["attribute"]["country"] == "Singapore"
        assert geo_event["attribute"]["geopolitical_risk_score"] == 0.6
        assert len(geo_event["attribute"]["country_scores"]) == 1


# ===========================================================================
# 10. Risk Level Classification
# ===========================================================================

class TestRiskLevel:
    def test_low(self):
        assert handler._risk_level(0.10) == "Low"

    def test_elevated(self):
        assert handler._risk_level(0.30) == "Elevated"

    def test_high(self):
        assert handler._risk_level(0.50) == "High"

    def test_critical(self):
        assert handler._risk_level(0.70) == "Critical"

    def test_boundaries(self):
        assert handler._risk_level(0.0) == "Low"
        assert handler._risk_level(0.19) == "Low"
        assert handler._risk_level(0.20) == "Elevated"
        assert handler._risk_level(0.39) == "Elevated"
        assert handler._risk_level(0.40) == "High"
        assert handler._risk_level(0.59) == "High"
        assert handler._risk_level(0.60) == "Critical"
        assert handler._risk_level(1.0) == "Critical"


# ===========================================================================
# 11. Full Integration (mocked externals)
# ===========================================================================

class TestFullIntegration:
    """Test _compute_and_store_risk with all external deps mocked."""

    @patch("lambdas.analytics.handler.notify_watchlist")
    @patch("lambdas.analytics.handler._get_geopolitical_risk_with_retry")
    @patch("lambdas.analytics.handler._load_model")
    def test_compute_and_store_produces_combined_output(self, mock_model, mock_geo, mock_notify):
        # Mock ML model that returns constant 0.3 for all inputs
        model = MagicMock()
        model.predict.return_value = [0.3, 0.3, 0.3, 0.3]
        mock_model.return_value = model

        # Mock geo risk
        mock_geo.return_value = {
            "country": "Singapore",
            "geopolitical_risk_score": 0.7,
            "geopolitical_risk_level": "High",
            "country_scores": [],
            "data_available": True,
        }

        processed = _make_processed_data()
        mock_s3 = MagicMock()

        with patch.dict(os.environ, {"DATA_BUCKET": "test-bucket"}):
            result = handler._compute_and_store_risk(mock_s3, "test-bucket", "H001", processed)

        # Verify S3 was called to store the result
        mock_s3.put_object.assert_called_once()
        put_kwargs = mock_s3.put_object.call_args
        assert "risk/weather/H001/latest.json" in str(put_kwargs)

        # Verify result structure
        assert "events" in result
        event_types = [e["event_type"] for e in result["events"]]
        assert "daily_risk_assessment" in event_types
        assert "seven_day_outlook" in event_types
        assert "geopolitical_risk_assessment" in event_types

        # Verify combined score in seven_day_outlook
        outlook = [e for e in result["events"] if e["event_type"] == "seven_day_outlook"][0]
        expected_combined = round(0.65 * 0.3 + 0.35 * 0.7, 4)
        assert outlook["attribute"]["combined_risk_score"] == expected_combined
        assert outlook["attribute"]["weather_component"] == 0.3
        assert outlook["attribute"]["geopolitical_component"] == 0.7

        # data_source should mention both
        assert "News Sentiment" in result["data_source"]

    @patch("lambdas.analytics.handler.notify_watchlist")
    @patch("lambdas.analytics.handler._get_geopolitical_risk_with_retry")
    @patch("lambdas.analytics.handler._load_model")
    def test_degrades_gracefully_when_geo_unavailable(self, mock_model, mock_geo, mock_notify):
        model = MagicMock()
        model.predict.return_value = [0.5, 0.5, 0.5, 0.5]
        mock_model.return_value = model

        mock_geo.return_value = handler._neutral_geo_risk("Singapore")

        processed = _make_processed_data()
        mock_s3 = MagicMock()

        with patch.dict(os.environ, {"DATA_BUCKET": "test-bucket"}):
            result = handler._compute_and_store_risk(mock_s3, "test-bucket", "H001", processed)

        outlook = [e for e in result["events"] if e["event_type"] == "seven_day_outlook"][0]
        # Combined should equal weather-only since geo is unavailable
        assert outlook["attribute"]["combined_risk_score"] == 0.5
        assert outlook["attribute"]["weather_component"] == 0.5
